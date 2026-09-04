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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CopyPartResult;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.paimon.s3native.S3PathUtils.isMarkerKey;
import static org.apache.paimon.s3native.S3PathUtils.key;
import static org.apache.paimon.s3native.S3PathUtils.markerKey;

/**
 * S3 {@link FileIO} implemented directly on AWS SDK v2, without any Hadoop dependency.
 *
 * <p>[PORTED] Object-store semantics (HeadObject-based existence, delimiter listing, CopyObject +
 * Delete rename, always-true mkdirs) derived from Apache Flink flink-filesystems/flink-s3-fs-native
 * (FLINK-38592, Apache License 2.0), class org.apache.flink.fs.s3native.NativeS3FileSystem. Local
 * reference: /Users/SL/javaProject/flink/flink-filesystems/flink-s3-fs-native/src/main/java/org/
 * apache/flink/fs/s3native/NativeS3FileSystem.java
 *
 * <p>Registered deviations from the Flink reference (see Spec §5.5): D1 directory markers (0-byte
 * {@code <key>/} objects, S3A-compatible — required by FileIOBehaviorTestBase empty-dir
 * visibility), D2 recursive directory rename, D4 zero-length files are files (key suffix decides
 * marker vs file), D5 empty-directory non-recursive delete succeeds, D6 batch recursive delete, D7
 * objects larger than 5GB are renamed via UploadPartCopy.
 */
public class S3NativeFileIO implements FileIO {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(S3NativeFileIO.class);

    /** S3 CopyObject cannot copy objects larger than 5GB in one request. */
    private static final long MAX_COPY_OBJECT_BYTES = 5L << 30;

    /**
     * Cache of client providers keyed by the translated options. S3 clients are bucket-agnostic
     * (the bucket is passed per request), so one provider per option set serves all buckets — same
     * accept-no-eviction trade-off as {@code org.apache.paimon.s3.S3FileIO#CACHE}.
     */
    private static final Map<Options, S3NativeClientProvider> CLIENTS = new ConcurrentHashMap<>();

    /** Translated s3.* options; serialized so a deserialized FileIO can rebuild everything. */
    private volatile Options normalizedOptions;

    private transient volatile S3NativeOptions options;

    @Override
    public boolean isObjectStore() {
        return true;
    }

    @Override
    public void configure(CatalogContext context) {
        Options normalized = S3ConfigTranslator.translate(context.options());
        this.normalizedOptions = normalized;
        // Fail fast on invalid values before any client is built.
        this.options = S3NativeOptions.from(normalized);
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        String key = key(path);
        HeadInfo head = headFile(ops(path), key);
        if (head == null) {
            throw new FileNotFoundException("File does not exist: " + path);
        }
        return new S3NativeSeekableInputStream(
                provider().syncClient(),
                S3PathUtils.bucket(path),
                key,
                head.contentLength,
                resolvedOptions().readBufferSize);
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        String key = key(path);
        S3NativeOptions resolved = resolvedOptions();
        if (!overwrite && classify(path).kind != Kind.MISSING) {
            // [PORTED] NativeS3FileSystem#create (NO_OVERWRITE branch). Blocks any existing
            // entry — file, marker directory or prefix directory — unlike the Flink original
            // which only checked objects.
            throw new IOException("File already exists: " + path);
        }
        return new S3NativePositionOutputStream(
                provider().syncClient(),
                provider().asyncClient(),
                S3PathUtils.bucket(path),
                key,
                resolved.tmpDir,
                resolved.partSizeBytes,
                resolved.maxConcurrentUploads);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return classify(path).toStatus(path);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        String key = key(path);
        Classification classification = classify(path);
        if (classification.kind == Kind.FILE) {
            return new FileStatus[] {classification.toStatus(path)};
        }
        if (classification.kind == Kind.MISSING) {
            throw new FileNotFoundException("File does not exist: " + path);
        }

        // Directory listing: immediate object children + common prefixes as sub-directories.
        String prefix = key.isEmpty() ? "" : key + "/";
        S3NativeObjectOperations.Children children = ops(path).listChildren(prefix);
        List<FileStatus> statuses =
                new ArrayList<>(children.objects.size() + children.commonPrefixes.size());
        for (software.amazon.awssdk.services.s3.model.S3Object object : children.objects) {
            String childKey = object.key();
            statuses.add(
                    S3NativeFileStatus.file(
                            object.size() == null ? 0L : object.size(),
                            object.lastModified() == null
                                    ? System.currentTimeMillis()
                                    : object.lastModified().toEpochMilli(),
                            new Path(path, childKey.substring(prefix.length()))));
        }
        for (String commonPrefix : children.commonPrefixes) {
            statuses.add(
                    S3NativeFileStatus.directory(
                            new Path(path, commonPrefix.substring(prefix.length()))));
        }
        return statuses.toArray(new FileStatus[0]);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return classify(path).kind != Kind.MISSING;
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        String key = key(path);
        S3NativeObjectOperations operations = ops(path);
        Classification classification = classify(path);

        switch (classification.kind) {
            case MISSING:
                return false;
            case FILE:
                operations.deleteObject(key);
                return true;
            case MARKER_DIR:
            case PREFIX_DIR:
            default:
                String prefix = key.isEmpty() ? "" : key + "/";
                if (!recursive) {
                    // [DEVIATION D5] An empty directory (only a marker) may be deleted
                    // non-recursively; a non-empty one must fail.
                    S3NativeObjectOperations.Children children = operations.listChildren(prefix);
                    if (!children.objects.isEmpty() || !children.commonPrefixes.isEmpty()) {
                        throw new IOException(
                                "Directory is not empty and recursive = false: " + path);
                    }
                    if (classification.kind == Kind.MARKER_DIR) {
                        operations.deleteObject(markerKey(key));
                    }
                    return true;
                }
                // [DEVIATION D6] Batch delete of every key under the prefix (markers included).
                operations.deleteBatch(operations.listAllKeys(prefix));
                return true;
        }
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        String key = key(path);
        if (key.isEmpty()) {
            return true; // bucket root
        }
        S3NativeObjectOperations operations = ops(path);

        if (operations.headObjectOrNull(key) != null) {
            // A plain object occupies the path (a marker would return null for the bare key).
            throw new IOException("Cannot mkdirs, a file already exists: " + path);
        }
        // An ancestor being a plain object blocks directory creation.
        int separator = -1;
        while ((separator = key.indexOf('/', separator + 1)) >= 0) {
            String ancestor = key.substring(0, separator);
            if (!ancestor.isEmpty() && operations.headObjectOrNull(ancestor) != null) {
                throw new IOException("Cannot mkdirs, a file blocks ancestor: " + ancestor);
            }
        }

        // [DEVIATION D1] Directory existence is visible via a marker. Skip creating one when
        // children already exist (S3A-style legacy prefixes without markers still list fine).
        if (!operations.hasObjectsUnder(markerKey(key))) {
            operations.putMarker(key);
        }
        return true;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        String srcKey = key(src);
        String dstKey = key(dst);
        if (srcKey.equals(dstKey)) {
            return true;
        }
        if (!S3PathUtils.bucket(src).equals(S3PathUtils.bucket(dst))) {
            // The copy below would otherwise silently land in the source bucket.
            return false;
        }
        S3NativeObjectOperations operations = ops(src);

        Classification srcClassification = classify(src);
        if (srcClassification.kind == Kind.MISSING) {
            return false;
        }
        if (classify(dst).kind != Kind.MISSING) {
            return false; // never overwrite
        }
        if (isAncestorOf(srcKey, dstKey)) {
            return false; // cannot move a directory into itself
        }

        if (srcClassification.kind == Kind.FILE) {
            long length = srcClassification.head.contentLength;
            copyObject(operations, srcKey, dstKey, length);
            operations.deleteObject(srcKey);
            return true;
        }

        // [DEVIATION D2] Directory rename: copy every key under the prefix (markers included),
        // then batch delete the source side. Not atomic, like every S3A rename.
        String srcPrefix = srcKey.isEmpty() ? "" : srcKey + "/";
        String dstPrefix = dstKey.isEmpty() ? "" : dstKey + "/";
        List<software.amazon.awssdk.services.s3.model.S3Object> srcObjects =
                operations.listAllObjects(srcPrefix);
        for (software.amazon.awssdk.services.s3.model.S3Object srcObject : srcObjects) {
            String srcObjectKey = srcObject.key();
            String renamed = dstPrefix + srcObjectKey.substring(srcPrefix.length());
            copyObject(
                    operations,
                    srcObjectKey,
                    renamed,
                    srcObject.size() == null ? 0L : srcObject.size());
        }
        List<String> srcKeys = new ArrayList<>(srcObjects.size());
        for (software.amazon.awssdk.services.s3.model.S3Object srcObject : srcObjects) {
            srcKeys.add(srcObject.key());
        }
        operations.deleteBatch(srcKeys);
        return true;
    }

    @Override
    public void close() {
        // Client providers are cached statically and intentionally outlive this instance,
        // mirroring org.apache.paimon.s3.S3FileIO.
    }

    // ------------------------------------------------------------------------

    private static boolean isAncestorOf(String ancestorKey, String descendantKey) {
        return !ancestorKey.isEmpty()
                && (descendantKey + "/").startsWith(ancestorKey + "/")
                && !descendantKey.equals(ancestorKey);
    }

    /** Copies an object; [DEVIATION D7] objects over 5GB go through UploadPartCopy. */
    private static void copyObject(
            S3NativeObjectOperations operations, String srcKey, String dstKey, long length)
            throws IOException {
        try {
            if (length <= MAX_COPY_OBJECT_BYTES) {
                CopyObjectResponse response =
                        operations
                                .client()
                                .copyObject(
                                        CopyObjectRequest.builder()
                                                .sourceBucket(operations.bucket())
                                                .sourceKey(srcKey)
                                                .destinationBucket(operations.bucket())
                                                .destinationKey(dstKey)
                                                .build());
                return;
            }
            copyObjectMultipart(operations, srcKey, dstKey, length);
        } catch (S3Exception e) {
            throw new IOException("Failed to copy " + srcKey + " to " + dstKey, e);
        }
    }

    private static void copyObjectMultipart(
            S3NativeObjectOperations operations, String srcKey, String dstKey, long length)
            throws IOException {
        String uploadId = null;
        try {
            CreateMultipartUploadResponse create =
                    operations
                            .client()
                            .createMultipartUpload(
                                    CreateMultipartUploadRequest.builder()
                                            .bucket(operations.bucket())
                                            .key(dstKey)
                                            .build());
            uploadId = create.uploadId();

            List<software.amazon.awssdk.services.s3.model.CompletedPart> parts = new ArrayList<>();
            int partNumber = 1;
            for (long offset = 0; offset < length; offset += MAX_COPY_OBJECT_BYTES, partNumber++) {
                long lastByte = Math.min(offset + MAX_COPY_OBJECT_BYTES, length) - 1;
                CopyPartResult result =
                        operations
                                .client()
                                .uploadPartCopy(
                                        UploadPartCopyRequest.builder()
                                                .sourceBucket(operations.bucket())
                                                .sourceKey(srcKey)
                                                .destinationBucket(operations.bucket())
                                                .destinationKey(dstKey)
                                                .uploadId(uploadId)
                                                .partNumber(partNumber)
                                                .copySourceRange(
                                                        String.format(
                                                                "bytes=%d-%d", offset, lastByte))
                                                .build())
                                .copyPartResult();
                parts.add(
                        software.amazon.awssdk.services.s3.model.CompletedPart.builder()
                                .partNumber(partNumber)
                                .eTag(result.eTag())
                                .build());
            }
            try {
                operations
                        .client()
                        .completeMultipartUpload(
                                software.amazon.awssdk.services.s3.model
                                        .CompleteMultipartUploadRequest.builder()
                                        .bucket(operations.bucket())
                                        .key(dstKey)
                                        .uploadId(uploadId)
                                        .multipartUpload(
                                                software.amazon.awssdk.services.s3.model
                                                        .CompletedMultipartUpload.builder()
                                                        .parts(parts)
                                                        .build())
                                        .build());
            } catch (software.amazon.awssdk.services.s3.model.NoSuchUploadException e) {
                // [PORTED] NativeS3ObjectOperations#commitMultiPartUpload — the complete request
                // may have succeeded while its response was lost; verify the object exists.
                if (operations.headObjectOrNull(dstKey) == null) {
                    abortCopyQuietly(operations, dstKey, uploadId);
                    throw new IOException(
                            "Failed to multipart-copy " + srcKey + " to " + dstKey, e);
                }
            } catch (S3Exception e) {
                abortCopyQuietly(operations, dstKey, uploadId);
                throw new IOException("Failed to multipart-copy " + srcKey + " to " + dstKey, e);
            }
        } catch (S3Exception e) {
            abortCopyQuietly(operations, dstKey, uploadId);
            throw new IOException("Failed to multipart-copy " + srcKey + " to " + dstKey, e);
        }
    }

    private static void abortCopyQuietly(
            S3NativeObjectOperations operations, String dstKey, String uploadId) {
        if (uploadId == null) {
            return;
        }
        try {
            operations
                    .client()
                    .abortMultipartUpload(
                            software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest
                                    .builder()
                                    .bucket(operations.bucket())
                                    .key(dstKey)
                                    .uploadId(uploadId)
                                    .build());
        } catch (RuntimeException abortFailure) {
            LOG.warn(
                    "Failed to abort multipart copy upload for {} (uploadId {})",
                    dstKey,
                    uploadId,
                    abortFailure);
        }
    }

    // ------------------------------------------------------------------------
    //  Classification
    // ------------------------------------------------------------------------

    /** Path classification used by every metadata operation. */
    private enum Kind {
        FILE,
        /** Directory whose existence is backed by a marker object. */
        MARKER_DIR,
        /** Directory that exists because objects live under its prefix. */
        PREFIX_DIR,
        MISSING
    }

    private static final class HeadInfo {
        final long contentLength;
        final long lastModified;

        HeadInfo(long contentLength, long lastModified) {
            this.contentLength = contentLength;
            this.lastModified = lastModified;
        }
    }

    private static final class Classification {
        final Kind kind;
        final HeadInfo head;

        Classification(Kind kind, HeadInfo head) {
            this.kind = kind;
            this.head = head;
        }

        FileStatus toStatus(Path path) throws FileNotFoundException {
            switch (kind) {
                case FILE:
                    return S3NativeFileStatus.file(head.contentLength, head.lastModified, path);
                case MARKER_DIR:
                case PREFIX_DIR:
                    return S3NativeFileStatus.directory(path);
                case MISSING:
                default:
                    throw new FileNotFoundException("File does not exist: " + path);
            }
        }
    }

    /**
     * [PORTED] NativeS3FileSystem#getFileStatus — HeadObject first, then directory detection.
     * [DEVIATION D4] Zero-length files are files: marker-ness is decided by the key suffix, not by
     * content length.
     */
    private Classification classify(Path path) throws IOException {
        String key = key(path);
        if (key.isEmpty()) {
            return new Classification(Kind.PREFIX_DIR, null); // bucket root
        }
        S3NativeObjectOperations operations = ops(path);

        HeadInfo head = headFile(operations, key);
        if (head != null) {
            return new Classification(Kind.FILE, head);
        }
        if (operations.headObjectOrNull(markerKey(key)) != null) {
            return new Classification(Kind.MARKER_DIR, null);
        }
        if (operations.hasObjectsUnder(markerKey(key))) {
            return new Classification(Kind.PREFIX_DIR, null);
        }
        return new Classification(Kind.MISSING, null);
    }

    /** HeadObject for a plain-object key, or {@code null} when missing or a marker. */
    private static HeadInfo headFile(S3NativeObjectOperations operations, String key)
            throws IOException {
        if (isMarkerKey(key)) {
            return null;
        }
        software.amazon.awssdk.services.s3.model.HeadObjectResponse head =
                operations.headObjectOrNull(key);
        if (head == null) {
            return null;
        }
        return new HeadInfo(
                head.contentLength() == null ? 0L : head.contentLength(),
                head.lastModified() == null
                        ? System.currentTimeMillis()
                        : head.lastModified().toEpochMilli());
    }

    // ------------------------------------------------------------------------
    //  Client access
    // ------------------------------------------------------------------------

    private S3NativeObjectOperations ops(Path path) {
        return new S3NativeObjectOperations(provider().syncClient(), S3PathUtils.bucket(path));
    }

    /** Lazily rebuilds the typed options after Java deserialization. */
    private S3NativeOptions resolvedOptions() {
        S3NativeOptions resolved = options;
        if (resolved == null) {
            resolved = S3NativeOptions.from(normalizedOptions);
            options = resolved;
        }
        return resolved;
    }

    private S3NativeClientProvider provider() {
        Options normalized = this.normalizedOptions;
        if (normalized == null) {
            throw new IllegalStateException("S3NativeFileIO is not configured yet");
        }
        return CLIENTS.computeIfAbsent(
                normalized, k -> S3NativeClientProvider.create(resolvedOptions()));
    }
}
