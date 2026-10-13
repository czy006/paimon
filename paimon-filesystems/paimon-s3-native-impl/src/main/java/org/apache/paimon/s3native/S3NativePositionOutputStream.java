/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.s3native;

import org.apache.paimon.fs.PositionOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Positional output stream writing to S3.
 *
 * <p>Data is buffered into local per-part temp files (64KB write buffer). A filled part is rolled
 * into a pending list and submitted as an async {@code UploadPart} with bounded in-flight
 * concurrency once the multipart threshold is reached ([PORTED-ICE I5]: {@code threshold = partSize
 * * factor}, default 1.5 — smaller objects stay on the single-PutObject path); {@link #close()}
 * uploads the tail and completes the multipart upload. With {@code s3.checksum-enabled}, part-level
 * and whole-object MD5 digests are sent as Content-MD5 ([PORTED-ICE I4]); write tags, storage class
 * and canned ACL are attached to object-creating requests ([PORTED-ICE I6]).
 *
 * <p>[PORTED] Buffering pattern from Apache Flink flink-filesystems/flink-s3-fs-native
 * (FLINK-38592, Apache License 2.0) writer/NativeS3RecoverableFsDataOutputStream (per-part temp
 * files, 64KB buffer, CompletedPart collection, abort-on-failure) and NativeS3OutputStream
 * (single-PutObject small-file path, here extended to Iceberg's multi-file sequence form). Local
 * reference: /Users/SL/javaProject/flink/flink-filesystems/flink-s3-fs-native/src/main/java/org/
 * apache/flink/fs/s3native/. Iceberg reference: S3OutputStream.java (threshold switch, part
 * splitting, MD5 digests, tags/storage-class) in /Users/SL/javaProject/iceberg/aws/src/main/
 * java/org/apache/iceberg/aws/s3/.
 *
 * <p>[DEVIATION D3] Parts are uploaded via bounded-concurrency async UploadPart calls instead of
 * Flink's synchronous sequential uploadPart — the TransferManager offers no parallel multipart on
 * the non-CRT Netty client, and CRT is excluded (D8).
 *
 * <p>Single-writer contract like the Flink original: write/flush/close are lock-guarded so close
 * from another thread (task cancellation) stays safe. An abandoned stream that is never closed
 * leaks its local part temp files until finalization — always close in a finally block.
 */
final class S3NativePositionOutputStream extends PositionOutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(S3NativePositionOutputStream.class);

    private static final int BUFFER_SIZE = 64 * 1024;
    private static final long CLOSE_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(15);
    private static final String OCTET_STREAM = "application/octet-stream";

    private final S3Client syncClient;
    private final S3AsyncClient asyncClient;
    private final String bucket;
    private final String key;
    private final File tmpDir;
    private final long partSize;
    private final long thresholdBytes;
    private final Semaphore uploadPermits;
    private final boolean checksumEnabled;
    private final Set<Tag> writeTags;
    @Nullable private final StorageClass writeStorageClass;
    @Nullable private final ObjectCannedACL acl;
    private final S3NativeSse sse;

    private final ReentrantLock lock = new ReentrantLock();

    /** All temp files ever created, for best-effort cleanup on any exit path. */
    private final List<File> tempFiles = new ArrayList<>();

    /** Filled parts not yet submitted (only accumulated below the multipart threshold). */
    private final List<FileAndDigest> pendingParts = new ArrayList<>();

    private final List<CompletableFuture<CompletedPart>> partFutures = new ArrayList<>();

    private OutputStream currentBuffer;
    private MessageDigest currentPartDigest;
    private MessageDigest wholeObjectDigest;
    private File currentPartFile;
    private long currentPartSize;

    private String uploadId;
    private final String uploadToken = UUID.randomUUID().toString();
    private int nextPartNumber = 1;
    private long pos;
    private boolean closed;
    @Nullable private IOException failure;

    /** Capture site for the unclosed-stream warning emitted by {@link #finalize()}. */
    private final StackTraceElement[] createStack;

    S3NativePositionOutputStream(
            S3Client syncClient,
            S3AsyncClient asyncClient,
            String bucket,
            String key,
            S3NativeOptions options)
            throws IOException {
        this.syncClient = syncClient;
        this.asyncClient = asyncClient;
        this.bucket = bucket;
        this.key = key;
        this.partSize = options.partSizeBytes;
        this.thresholdBytes = (long) (options.partSizeBytes * options.multipartThresholdFactor);
        this.uploadPermits = new Semaphore(options.maxConcurrentUploads);
        this.checksumEnabled = options.checksumEnabled;
        this.writeTags = toTags(options.writeTags);
        this.writeStorageClass = options.writeStorageClass;
        this.acl = options.acl;
        this.sse = options.sse;
        this.createStack = Thread.currentThread().getStackTrace();
        this.tmpDir = new File(options.tmpDir);
        if (checksumEnabled) {
            this.currentPartDigest = newDigest();
            this.wholeObjectDigest = newDigest();
        }
        Files.createDirectories(tmpDir.toPath());
        try {
            rollPartFile();
        } catch (IOException | RuntimeException e) {
            throw failStream(e);
        }
    }

    private static Set<Tag> toTags(Map<String, String> tagMap) {
        Set<Tag> tags = new HashSet<>();
        for (Map.Entry<String, String> entry : tagMap.entrySet()) {
            tags.add(Tag.builder().key(entry.getKey()).value(entry.getValue()).build());
        }
        return tags;
    }

    private static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 digest unavailable", e);
        }
    }

    @Override
    public long getPos() {
        lock.lock();
        try {
            return pos;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void write(int b) throws IOException {
        lock.lock();
        try {
            ensureOpen();
            currentBuffer.write(b);
            pos++;
            currentPartSize++;
            if (wholeObjectDigest != null) {
                wholeObjectDigest.update((byte) b);
            }
            onPartMaybeFull();
            maybeStartUploading();
        } catch (IOException | RuntimeException e) {
            if (!closed) {
                throw failStream(e);
            }
            throw e;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException("buffer");
        }
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new IndexOutOfBoundsException();
        }
        lock.lock();
        try {
            ensureOpen();
            // [PORTED-ICE Spec §14 I2] Split writes larger than a part at part boundaries, so a
            // single large write cannot produce oversized parts. Mirrors Iceberg
            // S3OutputStream#write(byte[], int, int).
            int remaining = len;
            int relativeOffset = off;
            while (currentPartSize + remaining > partSize) {
                int writeSize = (int) (partSize - currentPartSize);
                currentBuffer.write(b, relativeOffset, writeSize);
                pos += writeSize;
                currentPartSize += writeSize;
                updateWholeDigest(b, relativeOffset, writeSize);
                remaining -= writeSize;
                relativeOffset += writeSize;
                rollPendingPart();
                maybeStartUploading();
            }
            currentBuffer.write(b, relativeOffset, remaining);
            pos += remaining;
            currentPartSize += remaining;
            updateWholeDigest(b, relativeOffset, remaining);
            onPartMaybeFull();
            // [PORTED-ICE I5] Iceberg re-checks the threshold at the end of every write; without
            // this tail check a sub-part-increment stream just past the threshold would reach
            // close() and take the single-PutObject path instead of starting the MPU.
            maybeStartUploading();
        } catch (IOException | RuntimeException e) {
            if (!closed) {
                throw failStream(e);
            }
            throw e;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        // Flushes the 64KB buffer into the local part file only; an S3 object has no
        // intermediate visibility, matching S3A semantics.
        lock.lock();
        try {
            ensureOpen();
            currentBuffer.flush();
        } catch (IOException | RuntimeException e) {
            if (!closed) {
                throw failStream(e);
            }
            throw e;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            try {
                if (uploadId == null) {
                    // [PORTED-ICE I5] Below the threshold: one PutObject over the sequence of all
                    // staging files (Iceberg S3OutputStream#completeUploads).
                    currentBuffer.flush();
                    currentBuffer.close();
                    currentBuffer = null;
                    List<FileAndDigest> parts = allParts();
                    long contentLength = 0;
                    for (FileAndDigest part : parts) {
                        contentLength += part.file.length();
                    }
                    PutObjectRequest.Builder requestBuilder =
                            PutObjectRequest.builder().bucket(bucket).key(key);
                    sse.apply(requestBuilder);
                    applyWriteAttributes(
                            requestBuilder::tagging,
                            requestBuilder::storageClass,
                            requestBuilder::acl);
                    if (checksumEnabled && wholeObjectDigest != null) {
                        requestBuilder.contentMD5(
                                Base64.getEncoder().encodeToString(wholeObjectDigest.digest()));
                    }
                    syncClient.putObject(
                            requestBuilder.build(),
                            RequestBody.fromContentProvider(
                                    () -> sequence(parts), contentLength, OCTET_STREAM));
                } else {
                    if (currentPartSize > 0) {
                        rollPendingPart();
                    }
                    currentBuffer.close();
                    currentBuffer = null;
                    discardCurrentEmptyPart();
                    submitPendingParts();
                    completeUpload();
                }
            } catch (IOException | RuntimeException e) {
                throw failStream(new IOException("Failed to upload s3://" + bucket + "/" + key, e));
            }
            cleanupTempFiles();
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------------

    private void ensureOpen() throws IOException {
        if (failure != null) {
            throw new IOException("Stream has failed", failure);
        }
        if (closed) {
            throw new IOException("Stream is closed");
        }
    }

    /** The whole-object digest only covers the pre-multipart prefix, as in Iceberg. */
    private void updateWholeDigest(byte[] b, int off, int len) {
        if (checksumEnabled && uploadId == null && wholeObjectDigest != null) {
            wholeObjectDigest.update(b, off, len);
        }
    }

    private void onPartMaybeFull() throws IOException {
        if (currentPartSize >= partSize) {
            rollPendingPart();
            maybeStartUploading();
        }
    }

    /**
     * [PORTED-ICE I5] Parts start uploading only once the stream has passed the multipart
     * threshold; afterwards every filled part is submitted immediately.
     */
    private void maybeStartUploading() throws IOException {
        if (uploadId != null || pos >= thresholdBytes) {
            submitPendingParts();
        }
    }

    private List<FileAndDigest> allParts() {
        List<FileAndDigest> all = new ArrayList<>(pendingParts);
        if (currentPartFile != null) {
            all.add(new FileAndDigest(currentPartFile, null));
        }
        return all;
    }

    private void discardCurrentEmptyPart() {
        if (currentPartFile != null && currentPartFile.delete()) {
            tempFiles.remove(currentPartFile);
        }
        currentPartFile = null;
    }

    private InputStream sequence(List<FileAndDigest> parts) {
        if (parts.isEmpty()) {
            return new ByteArrayInputStream(new byte[0]);
        }
        List<InputStream> streams = new ArrayList<>();
        for (FileAndDigest part : parts) {
            streams.add(uncheckedInputStream(part.file));
        }
        InputStream sequence = streams.get(0);
        for (int i = 1; i < streams.size(); i++) {
            sequence = new SequenceInputStream(sequence, streams.get(i));
        }
        return sequence;
    }

    private static InputStream uncheckedInputStream(File file) {
        try {
            return new BufferedInputStream(Files.newInputStream(file.toPath()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Closes the current part file and moves it to the pending list; rolls a fresh file. */
    private void rollPendingPart() throws IOException {
        try {
            currentBuffer.flush();
            currentBuffer.close();
        } catch (IOException e) {
            throw new IOException("Failed to flush local part file for " + key, e);
        }
        currentBuffer = null;
        byte[] digest = null;
        if (checksumEnabled && currentPartDigest != null) {
            digest = currentPartDigest.digest();
            currentPartDigest = newDigest();
        }
        pendingParts.add(new FileAndDigest(currentPartFile, digest));
        rollPartFile();
    }

    /**
     * Submits all pending parts as async UploadPart calls with bounded in-flight concurrency. Any
     * failure aborts the upload and poisons the stream so later writes fail fast with the original
     * cause chain intact.
     */
    private void submitPendingParts() throws IOException {
        if (pendingParts.isEmpty()) {
            return;
        }
        if (uploadId == null) {
            uploadId = startMultipartUpload();
        }
        for (FileAndDigest part : pendingParts) {
            final File partFile = part.file;
            final int partNumber = nextPartNumber++;
            try {
                // Bounded in-flight parts; single-writer contract makes blocking under the lock
                // safe.
                uploadPermits.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for an upload permit", e);
            }
            try {
                UploadPartRequest.Builder requestBuilder =
                        UploadPartRequest.builder()
                                .bucket(bucket)
                                .key(key)
                                .uploadId(uploadId)
                                .partNumber(partNumber);
                sse.applyCustomer(requestBuilder);
                if (part.md5 != null) {
                    requestBuilder.contentMD5(Base64.getEncoder().encodeToString(part.md5));
                }
                CompletableFuture<CompletedPart> future =
                        asyncClient
                                .uploadPart(
                                        requestBuilder.build(),
                                        AsyncRequestBody.fromFile(partFile.toPath()))
                                .thenApply(
                                        response -> toCompletedPart(partNumber, response, partFile))
                                .whenComplete((completed, error) -> uploadPermits.release());
                partFutures.add(future);
            } catch (RuntimeException e) {
                uploadPermits.release(); // whenComplete never registered on synchronous failure
                throw new IOException("Failed to submit part " + partNumber + " of " + key, e);
            }
        }
        pendingParts.clear();
    }

    private String startMultipartUpload() throws IOException {
        CreateMultipartUploadRequest.Builder requestBuilder =
                CreateMultipartUploadRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .metadata(
                                S3NativeUploadMetadata.withToken(
                                        Collections.emptyMap(), uploadToken));
        sse.apply(requestBuilder);
        applyWriteAttributes(
                requestBuilder::tagging, requestBuilder::storageClass, requestBuilder::acl);
        try {
            return syncClient.createMultipartUpload(requestBuilder.build()).uploadId();
        } catch (RuntimeException e) {
            throw new IOException("Failed to start multipart upload for " + key, e);
        }
    }

    /** [PORTED-ICE I6] Tags, storage class and ACL on every object-creating request. */
    private void applyWriteAttributes(
            java.util.function.Consumer<Tagging> taggingSetter,
            java.util.function.Consumer<StorageClass> storageClassSetter,
            java.util.function.Consumer<ObjectCannedACL> aclSetter) {
        if (!writeTags.isEmpty()) {
            taggingSetter.accept(Tagging.builder().tagSet(writeTags).build());
        }
        if (writeStorageClass != null) {
            storageClassSetter.accept(writeStorageClass);
        }
        if (acl != null) {
            aclSetter.accept(acl);
        }
    }

    private CompletedPart toCompletedPart(
            int partNumber, UploadPartResponse response, File partFile) {
        if (!partFile.delete()) {
            LOG.debug("Failed to delete uploaded part temp file {}", partFile);
        }
        return CompletedPart.builder().partNumber(partNumber).eTag(response.eTag()).build();
    }

    private void completeUpload() throws IOException {
        List<CompletedPart> parts = new ArrayList<>(partFutures.size());
        try {
            CompletableFuture.allOf(partFutures.toArray(new CompletableFuture[0]))
                    .get(CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            for (CompletableFuture<CompletedPart> future : partFutures) {
                parts.add(future.get());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while uploading parts of " + key, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IOException("Failed to upload parts of " + key, e);
        }
        parts.sort(Comparator.comparing(CompletedPart::partNumber));
        try {
            CompleteMultipartUploadRequest.Builder requestBuilder =
                    CompleteMultipartUploadRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .uploadId(uploadId)
                            .multipartUpload(
                                    CompletedMultipartUpload.builder().parts(parts).build());
            sse.applyCustomer(requestBuilder);
            syncClient.completeMultipartUpload(requestBuilder.build());
        } catch (NoSuchUploadException e) {
            if (!uploadedObjectMatches()) {
                throw new IOException("Failed to complete multipart upload for " + key, e);
            }
        } catch (RuntimeException e) {
            throw new IOException("Failed to complete multipart upload for " + key, e);
        }
    }

    private boolean uploadedObjectMatches() {
        try {
            // [PORTED-ICE I8] The NoSuchUpload recovery probe carries SSE-C headers like
            // Iceberg's getObjectMetadata recovery path.
            software.amazon.awssdk.services.s3.model.HeadObjectRequest.Builder builder =
                    HeadObjectRequest.builder().bucket(bucket).key(key);
            sse.applyCustomer(builder);
            return S3NativeUploadMetadata.matches(
                    syncClient.headObject(builder.build()), uploadToken, pos);
        } catch (RuntimeException e) {
            return false;
        }
    }

    /** Discards a failed write without letting a later close publish its staged prefix. */
    private IOException failStream(Exception cause) {
        failure =
                cause instanceof IOException
                        ? (IOException) cause
                        : new IOException("Failed to write s3://" + bucket + "/" + key, cause);
        closed = true;
        OutputStream buffer = currentBuffer;
        currentBuffer = null;
        if (buffer != null) {
            try {
                buffer.close();
            } catch (IOException | RuntimeException e) {
                if (e != failure) {
                    failure.addSuppressed(e);
                }
            }
        }
        try {
            abortUploadQuietly();
        } catch (RuntimeException e) {
            failure.addSuppressed(e);
        }
        try {
            cleanupTempFiles();
        } catch (RuntimeException e) {
            failure.addSuppressed(e);
        }
        return failure;
    }

    private void abortUploadQuietly() {
        if (uploadId != null) {
            try {
                syncClient.abortMultipartUpload(
                        AbortMultipartUploadRequest.builder()
                                .bucket(bucket)
                                .key(key)
                                .uploadId(uploadId)
                                .build());
            } catch (RuntimeException e) {
                LOG.warn("Failed to abort multipart upload for {} (uploadId {})", key, uploadId, e);
            }
        }
    }

    private void rollPartFile() throws IOException {
        File partFile = new File(tmpDir, "paimon-s3-native-" + UUID.randomUUID());
        OutputStream outputStream;
        try {
            outputStream = createPartStream(partFile.toPath());
        } catch (IOException e) {
            throw new IOException("Failed to create local part file " + partFile, e);
        }
        // Register only files we created, so a name collision cannot delete someone else's file.
        currentPartFile = partFile;
        tempFiles.add(partFile);
        currentPartSize = 0;
        outputStream = new BufferedOutputStream(outputStream, BUFFER_SIZE);
        // [PORTED-ICE I4] Part-level MD5 accumulates as bytes pass through the buffer.
        currentBuffer =
                checksumEnabled && currentPartDigest != null
                        ? new DigestOutputStream(outputStream, currentPartDigest)
                        : outputStream;
    }

    private static OutputStream createPartStream(Path path) throws IOException {
        Set<StandardOpenOption> options =
                EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        if (Files.getFileStore(path.getParent())
                .supportsFileAttributeView(PosixFileAttributeView.class)) {
            // Create and open atomically with owner-only permissions; never reopen the path.
            return Channels.newOutputStream(
                    Files.newByteChannel(
                            path,
                            options,
                            PosixFilePermissions.asFileAttribute(
                                    PosixFilePermissions.fromString("rw-------"))));
        }
        // Non-POSIX providers retain their native file-creation permissions.
        return Channels.newOutputStream(Files.newByteChannel(path, options));
    }

    private void cleanupTempFiles() {
        for (File file : tempFiles) {
            if (file.exists() && !file.delete()) {
                file.deleteOnExit();
            }
        }
    }

    /**
     * [PORTED-ICE Spec §14 I3] Last-resort cleanup for streams a writer failed to close: abort the
     * multipart upload and delete temp files. Completing the upload from a finalizer would be
     * unsafe, so the data is intentionally discarded. Mirrors Iceberg S3OutputStream#finalize.
     */
    @Override
    @SuppressWarnings({"Finalize", "deprecation"})
    protected void finalize() throws Throwable {
        super.finalize();
        if (!closed) {
            lock.lock();
            try {
                closed = true;
                // [DEVIATION] Iceberg's finalize only removes staging files and leaves the
                // multipart upload to bucket lifecycle rules; we also abort it to avoid paying
                // for orphaned parts, accepting a bounded SDK call on the finalizer thread.
                if (currentBuffer != null) {
                    try {
                        currentBuffer.close();
                    } catch (IOException e) {
                        LOG.debug("Failed to close unclosed stream's buffer", e);
                    }
                }
                abortUploadQuietly();
                cleanupTempFiles();
            } finally {
                lock.unlock();
            }
            LOG.warn(
                    "Unclosed output stream created by:\n\t{}",
                    S3NativeSeekableInputStream.formatCreateStackTrace(createStack));
        }
    }

    /** A filled staging file and, when checksums are on, its MD5 digest. */
    private static final class FileAndDigest {
        private final File file;
        private final byte[] md5;

        FileAndDigest(File file, byte[] md5) {
            this.file = file;
            this.md5 = md5;
        }
    }
}
