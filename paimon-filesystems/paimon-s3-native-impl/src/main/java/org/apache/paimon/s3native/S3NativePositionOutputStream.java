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
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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
 * <p>Data is buffered into local per-part temp files (64KB write buffer). Whenever a part reaches
 * {@code partSize} it is submitted as an async {@code UploadPart} with bounded in-flight
 * concurrency; {@link #close()} uploads the tail and completes the multipart upload. Objects
 * smaller than one part take the single-{@code PutObject} shortcut.
 *
 * <p>[PORTED] Buffering pattern from Apache Flink flink-s3-fs-native (FLINK-38592, Apache License
 * 2.0) writer/NativeS3RecoverableFsDataOutputStream (per-part temp files, 64KB buffer,
 * CompletedPart collection, abort-on-failure), small-file path from NativeS3OutputStream. Local
 * reference: /Users/SL/javaProject/flink/flink-filesystems/flink-s3-fs-native/src/main/java/org/
 * apache/flink/fs/s3native/
 *
 * <p>[DEVIATION D3] Parts are uploaded via bounded-concurrency async UploadPart calls instead of
 * Flink's synchronous sequential uploadPart — the TransferManager offers no parallel multipart on
 * the non-CRT Netty client, and CRT is excluded (D8).
 *
 * <p>Single-writer contract like the Flink original: write/flush/close are lock-guarded so close
 * from another thread (task cancellation) stays safe.
 */
final class S3NativePositionOutputStream extends PositionOutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(S3NativePositionOutputStream.class);

    private static final int BUFFER_SIZE = 64 * 1024;
    private static final long CLOSE_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(15);

    private final S3Client syncClient;
    private final S3AsyncClient asyncClient;
    private final String bucket;
    private final String key;
    private final File tmpDir;
    private final long partSize;
    private final Semaphore uploadPermits;

    private final ReentrantLock lock = new ReentrantLock();

    /** All temp files ever created, for best-effort cleanup on any exit path. */
    private final List<File> tempFiles = new ArrayList<>();

    private final List<CompletableFuture<CompletedPart>> partFutures = new ArrayList<>();

    private OutputStream currentBuffer;
    private File currentPartFile;
    private long currentPartSize;

    private String uploadId;
    private int nextPartNumber = 1;
    private long pos;
    private boolean closed;

    S3NativePositionOutputStream(
            S3Client syncClient,
            S3AsyncClient asyncClient,
            String bucket,
            String key,
            String tmpDir,
            long partSize,
            int maxConcurrentUploads)
            throws IOException {
        this.syncClient = syncClient;
        this.asyncClient = asyncClient;
        this.bucket = bucket;
        this.key = key;
        this.partSize = partSize;
        this.uploadPermits = new Semaphore(maxConcurrentUploads);
        this.tmpDir = new File(tmpDir);
        Files.createDirectories(this.tmpDir.toPath());
        rollPartFile();
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
            maybeSubmitPart();
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
        lock.lock();
        try {
            ensureOpen();
            currentBuffer.write(b, off, len);
            pos += len;
            currentPartSize += len;
            maybeSubmitPart();
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
                currentBuffer.flush();
                currentBuffer.close();
                currentBuffer = null;

                if (uploadId == null) {
                    // Small-file shortcut: the whole object fits a single PutObject.
                    // [PORTED] NativeS3OutputStream#uploadToS3
                    syncClient.putObject(
                            software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                                    .bucket(bucket)
                                    .key(key)
                                    .build(),
                            software.amazon.awssdk.core.sync.RequestBody.fromFile(
                                    currentPartFile.toPath()));
                } else {
                    if (currentPartSize > 0) {
                        submitCurrentPart();
                    }
                    completeUpload();
                }
            } catch (IOException | RuntimeException e) {
                abortUploadQuietly();
                throw new IOException("Failed to upload s3://" + bucket + "/" + key, e);
            } finally {
                cleanupTempFiles();
            }
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------------

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
    }

    private void maybeSubmitPart() throws IOException {
        if (currentPartSize >= partSize) {
            submitCurrentPart();
        }
    }

    /**
     * Flushes and closes the current part file and submits it as an async UploadPart; a fresh part
     * file is rolled for subsequent writes.
     */
    private void submitCurrentPart() throws IOException {
        try {
            currentBuffer.flush();
            currentBuffer.close();
        } catch (IOException e) {
            throw new IOException("Failed to flush local part file for " + key, e);
        }
        currentBuffer = null;

        if (uploadId == null) {
            uploadId = startMultipartUpload();
        }

        final File partFile = currentPartFile;
        final int partNumber = nextPartNumber++;
        try {
            // Bounded in-flight parts; single-writer contract makes blocking under the lock safe.
            uploadPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for an upload permit", e);
        }

        CompletableFuture<CompletedPart> future =
                asyncClient
                        .uploadPart(
                                UploadPartRequest.builder()
                                        .bucket(bucket)
                                        .key(key)
                                        .uploadId(uploadId)
                                        .partNumber(partNumber)
                                        .build(),
                                AsyncRequestBody.fromFile(partFile.toPath()))
                        .thenApply(response -> toCompletedPart(partNumber, response, partFile))
                        .whenComplete((part, error) -> uploadPermits.release());
        partFutures.add(future);
        rollPartFile();
    }

    private String startMultipartUpload() throws IOException {
        try {
            return syncClient
                    .createMultipartUpload(
                            software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest
                                    .builder()
                                    .bucket(bucket)
                                    .key(key)
                                    .build())
                    .uploadId();
        } catch (RuntimeException e) {
            throw new IOException("Failed to start multipart upload for " + key, e);
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
            syncClient.completeMultipartUpload(
                    software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest
                            .builder()
                            .bucket(bucket)
                            .key(key)
                            .uploadId(uploadId)
                            .multipartUpload(
                                    software.amazon.awssdk.services.s3.model
                                            .CompletedMultipartUpload.builder()
                                            .parts(parts)
                                            .build())
                            .build());
        } catch (RuntimeException e) {
            throw new IOException("Failed to complete multipart upload for " + key, e);
        }
    }

    private void abortUploadQuietly() {
        if (uploadId != null) {
            try {
                syncClient.abortMultipartUpload(
                        software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest
                                .builder()
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
        currentPartFile = new File(tmpDir, "paimon-s3-native-" + UUID.randomUUID());
        tempFiles.add(currentPartFile);
        currentPartSize = 0;
        try {
            currentBuffer =
                    new BufferedOutputStream(new FileOutputStream(currentPartFile), BUFFER_SIZE);
        } catch (IOException e) {
            throw new IOException("Failed to create local part file " + currentPartFile, e);
        }
    }

    private void cleanupTempFiles() {
        for (File file : tempFiles) {
            if (file.exists() && !file.delete()) {
                file.deleteOnExit();
            }
        }
    }
}
