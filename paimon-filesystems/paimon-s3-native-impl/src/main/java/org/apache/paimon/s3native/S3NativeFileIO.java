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
 * Delete rename, mkdirs succeeding without directory creation) derived from Apache Flink
 * flink-filesystems/flink-s3-fs-native (FLINK-38592, Apache License 2.0), class
 * org.apache.flink.fs.s3native.NativeS3FileSystem. Local reference:
 * /Users/SL/javaProject/flink/flink-filesystems/flink-s3-fs-native/src/main/java/org/
 * apache/flink/fs/s3native/NativeS3FileSystem.java
 *
 * <p>Registered deviations from the Flink reference (see Spec §5.5): D1 directory markers (0-byte
 * {@code <key>/} objects, S3A-compatible — required by FileIOBehaviorTestBase empty-dir
 * visibility), D2 recursive directory rename, D4 zero-length files are files (key suffix decides
 * marker vs file), D5 empty-directory non-recursive delete succeeds, D6 batch recursive delete, D7
 * objects larger than 5GB are renamed via UploadPartCopy, D9 mkdirs fails fast on file conflicts
 * (Paimon contract) instead of returning true unconditionally.
 */
public class S3NativeFileIO implements FileIO {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(S3NativeFileIO.class);

    /** S3 CopyObject cannot copy objects larger than 5GB in one request. */
    private static final long MAX_COPY_OBJECT_BYTES = 5L << 30;

    /**
     * Cache of client providers keyed by {@link ClientKey} — the exact projection of options that
     * {@link S3NativeClientProvider#create} reads. Stream-level options (part size, SSE, write
     * tags, delete tuning, ...) differ per catalog/table without fragmenting the cache: they are
     * resolved per call via {@link #resolvedOptions()} and never reach client construction.
     * Bucket-agnostic (bucket passed per request). Same accept-no-eviction trade-off as {@code
     * org.apache.paimon.s3.S3FileIO#CACHE}; a size warning flags option drift (e.g. per-table
     * credentials), since each entry pins a connection pool plus a Netty event-loop group.
     */
    private static final Map<ClientKey, S3NativeClientProvider> CLIENTS = new ConcurrentHashMap<>();

    private static final int CLIENT_CACHE_WARN_THRESHOLD = 16;

    /**
     * Client-construction key; must stay in lockstep with the fields {@link
     * S3NativeClientProvider#create} reads — adding a create() input without extending this record
     * silently shares a client across differing configurations.
     */
    static final class ClientKey {
        final String accessKey;
        final String secretKey;
        final String region;
        final String endpoint;
        final boolean pathStyleAccess;
        final boolean chunkedEncodingEnabled;
        final boolean checksumValidationEnabled;
        final int maxConnections;
        final long connectionTimeoutMs;
        final long socketTimeoutMs;
        final long connectionMaxIdleTimeMs;
        final int maxRetries;
        final long retryBaseDelayMs;
        final long retryThrottleBaseDelayMs;
        final long retryMaxBackoffMs;

        private ClientKey(S3NativeOptions options) {
            this.accessKey = options.accessKey;
            this.secretKey = options.secretKey;
            this.region = options.region;
            this.endpoint = options.endpoint;
            this.pathStyleAccess = options.pathStyleAccess;
            this.chunkedEncodingEnabled = options.chunkedEncodingEnabled;
            this.checksumValidationEnabled = options.checksumValidationEnabled;
            this.maxConnections = options.maxConnections;
            this.connectionTimeoutMs = options.connectionTimeout.toMillis();
            this.socketTimeoutMs = options.socketTimeout.toMillis();
            this.connectionMaxIdleTimeMs = options.connectionMaxIdleTime.toMillis();
            this.maxRetries = options.maxRetries;
            this.retryBaseDelayMs = options.retryBaseDelay.toMillis();
            this.retryThrottleBaseDelayMs = options.retryThrottleBaseDelay.toMillis();
            this.retryMaxBackoffMs = options.retryMaxBackoff.toMillis();
        }

        static ClientKey from(S3NativeOptions options) {
            return new ClientKey(options);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClientKey that = (ClientKey) o;
            return pathStyleAccess == that.pathStyleAccess
                    && chunkedEncodingEnabled == that.chunkedEncodingEnabled
                    && checksumValidationEnabled == that.checksumValidationEnabled
                    && maxConnections == that.maxConnections
                    && connectionTimeoutMs == that.connectionTimeoutMs
                    && socketTimeoutMs == that.socketTimeoutMs
                    && connectionMaxIdleTimeMs == that.connectionMaxIdleTimeMs
                    && maxRetries == that.maxRetries
                    && retryBaseDelayMs == that.retryBaseDelayMs
                    && retryThrottleBaseDelayMs == that.retryThrottleBaseDelayMs
                    && retryMaxBackoffMs == that.retryMaxBackoffMs
                    && java.util.Objects.equals(accessKey, that.accessKey)
                    && java.util.Objects.equals(secretKey, that.secretKey)
                    && java.util.Objects.equals(region, that.region)
                    && java.util.Objects.equals(endpoint, that.endpoint);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(
                    accessKey,
                    secretKey,
                    region,
                    endpoint,
                    pathStyleAccess,
                    chunkedEncodingEnabled,
                    checksumValidationEnabled,
                    maxConnections,
                    connectionTimeoutMs,
                    socketTimeoutMs,
                    connectionMaxIdleTimeMs,
                    maxRetries,
                    retryBaseDelayMs,
                    retryThrottleBaseDelayMs,
                    retryMaxBackoffMs);
        }
    }

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
        S3NativeOptions resolved = resolvedOptions();
        return new S3NativeSeekableInputStream(
                provider().syncClient(),
                S3PathUtils.bucket(path),
                key,
                head.contentLength,
                resolved.readBufferSize,
                resolved.sse);
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        String key = key(path);
        S3NativeOptions resolved = resolvedOptions();
        if (!overwrite && classify(path).kind != Kind.MISSING) {
            // [PORTED] NativeS3FileSystem#create (NO_OVERWRITE branch). Blocks any existing
            // entry — file, marker directory or prefix directory — matching the reference's
            // exists() semantics (which also detects prefix directories).
            throw new IOException("File already exists: " + path);
        }
        return new S3NativePositionOutputStream(
                provider().syncClient(),
                provider().asyncClient(),
                S3PathUtils.bucket(path),
                key,
                resolved);
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
            // [ADAPTED] Flink returns an empty array for object keys; returning the file's own
            // status is the Hadoop/Paimon convention.
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
        if (key.isEmpty() && recursive) {
            // Deleting the bucket root would batch-delete every key in the bucket; refuse
            // like S3A's root-delete guard instead of wiping on a stray tool call.
            throw new IOException("Refusing recursive delete of the bucket root: " + path);
        }
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
                // [DEVIATION D6] Stream the prefix into parallel batch deletes — bounded memory
                // and list/delete overlap instead of materializing every key first.
                S3NativeOptions options = resolvedOptions();
                operations.deletePrefixStreaming(
                        prefix, options.deleteBatchSize, options.deleteThreads);
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

        // [DEVIATION D9] Flink's mkdirs is unconditionally true; the fail-fast checks below are
        // mandated by the Paimon FileIO contract (FileIOBehaviorTestBase mkdirs cases).
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
        // Bucket check first: a cross-bucket rename with identical keys must fail loudly,
        // not no-op with a success return (rename sits on Paimon's commit path).
        if (!S3PathUtils.bucket(src).equals(S3PathUtils.bucket(dst))) {
            // [ADAPTED] The Flink filesystem is bucket-scoped; this FileIO is bucket-agnostic, so
            // the copy below would otherwise silently land in the source bucket.
            return false;
        }
        if (srcKey.equals(dstKey)) {
            // [ADAPTED] No-op success for same-bucket same-key renames (Hadoop convention).
            return true;
        }
        if (srcKey.isEmpty()) {
            // Renaming the bucket root would recursively copy the entire bucket; refuse like
            // S3A instead of running a whole-bucket copy.
            return false;
        }
        S3NativeObjectOperations operations = ops(src);

        Classification srcClassification = classify(src);
        if (srcClassification.kind == Kind.MISSING) {
            return false;
        }
        if (classify(dst).kind != Kind.MISSING) {
            // [ADAPTED] Flink's rename silently overwrites the destination; refusing is the
            // Hadoop/Paimon convention.
            return false;
        }
        if (isAncestorOf(srcKey, dstKey)) {
            return false; // cannot move a directory into itself
        }

        if (srcClassification.kind == Kind.FILE) {
            long length = srcClassification.head.contentLength;
            copyObject(operations, resolvedOptions().sse, srcKey, dstKey, length);
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
                    resolvedOptions().sse,
                    srcObjectKey,
                    renamed,
                    srcObject.size() == null ? 0L : srcObject.size());
        }
        List<String> srcKeys = new ArrayList<>(srcObjects.size());
        for (software.amazon.awssdk.services.s3.model.S3Object srcObject : srcObjects) {
            srcKeys.add(srcObject.key());
        }
        deleteBatchAll(operations, srcKeys);
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

    private void deleteBatchAll(S3NativeObjectOperations operations, List<String> keys)
            throws IOException {
        S3NativeOptions options = resolvedOptions();
        operations.deleteBatch(keys, options.deleteBatchSize, options.deleteThreads);
    }

    /**
     * Copies an object; [DEVIATION D7] objects over 5GB go through UploadPartCopy. Both paths carry
     * the SSE settings — the copy source needs SSE-C headers to be readable, and the destination
     * must keep the encryption policy (a blind spot inherited from Iceberg, whose FileIO has no
     * rename).
     */
    private static void copyObject(
            S3NativeObjectOperations operations,
            S3NativeSse sse,
            String srcKey,
            String dstKey,
            long length)
            throws IOException {
        try {
            if (length <= MAX_COPY_OBJECT_BYTES) {
                CopyObjectRequest.Builder builder =
                        CopyObjectRequest.builder()
                                .sourceBucket(operations.bucket())
                                .sourceKey(srcKey)
                                .destinationBucket(operations.bucket())
                                .destinationKey(dstKey);
                sse.apply(builder);
                operations.client().copyObject(builder.build());
                return;
            }
            copyObjectMultipart(operations, sse, srcKey, dstKey, length);
        } catch (S3Exception e) {
            throw new IOException("Failed to copy " + srcKey + " to " + dstKey, e);
        } catch (RuntimeException e) {
            // SdkClientException (network/credential) does not extend S3Exception.
            throw new IOException("Failed to copy " + srcKey + " to " + dstKey, e);
        }
    }

    private static void copyObjectMultipart(
            S3NativeObjectOperations operations,
            S3NativeSse sse,
            String srcKey,
            String dstKey,
            long length)
            throws IOException {
        String uploadId = null;
        try {
            CreateMultipartUploadRequest.Builder createBuilder =
                    CreateMultipartUploadRequest.builder().bucket(operations.bucket()).key(dstKey);
            sse.apply(createBuilder);
            CreateMultipartUploadResponse create =
                    operations.client().createMultipartUpload(createBuilder.build());
            uploadId = create.uploadId();

            List<software.amazon.awssdk.services.s3.model.CompletedPart> parts = new ArrayList<>();
            int partNumber = 1;
            for (long offset = 0; offset < length; offset += MAX_COPY_OBJECT_BYTES, partNumber++) {
                long lastByte = Math.min(offset + MAX_COPY_OBJECT_BYTES, length) - 1;
                UploadPartCopyRequest.Builder partBuilder =
                        UploadPartCopyRequest.builder()
                                .sourceBucket(operations.bucket())
                                .sourceKey(srcKey)
                                .destinationBucket(operations.bucket())
                                .destinationKey(dstKey)
                                .uploadId(uploadId)
                                .partNumber(partNumber)
                                .copySourceRange(String.format("bytes=%d-%d", offset, lastByte));
                sse.apply(partBuilder);
                CopyPartResult result =
                        operations.client().uploadPartCopy(partBuilder.build()).copyPartResult();
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
            } catch (RuntimeException e) {
                // SdkClientException during a part: abort so the uploadId does not leak
                // orphaned billed parts until the lifecycle rule fires.
                abortCopyQuietly(operations, dstKey, uploadId);
                throw new IOException("Failed to multipart-copy " + srcKey + " to " + dstKey, e);
            }
        } catch (S3Exception e) {
            abortCopyQuietly(operations, dstKey, uploadId);
            throw new IOException("Failed to multipart-copy " + srcKey + " to " + dstKey, e);
        } catch (RuntimeException e) {
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
        // Single-listing probe replaces the marker HEAD + prefix LIST pair: one request decides
        // marker-dir / prefix-dir / missing via self-marker membership (see
        // S3NativeObjectOperations#probeDirectory). Directory and missing-path probes drop from
        // 3 requests to 2, matching the Flink reference's shape.
        switch (operations.probeDirectory(key)) {
            case MARKER_DIR:
                return new Classification(Kind.MARKER_DIR, null);
            case PREFIX_DIR:
                return new Classification(Kind.PREFIX_DIR, null);
            case MISSING:
            default:
                return new Classification(Kind.MISSING, null);
        }
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
        return new S3NativeObjectOperations(
                provider().syncClient(), S3PathUtils.bucket(path), resolvedOptions().sse);
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
        S3NativeClientProvider provider =
                CLIENTS.computeIfAbsent(
                        ClientKey.from(resolvedOptions()),
                        k -> S3NativeClientProvider.create(resolvedOptions()));
        if (CLIENTS.size() > CLIENT_CACHE_WARN_THRESHOLD) {
            LOG.warn(
                    "{} distinct S3 client configurations cached (threshold {}); each entry pins "
                            + "a connection pool and a Netty event-loop group — check for drifting "
                            + "options such as per-table credentials",
                    CLIENTS.size(),
                    CLIENT_CACHE_WARN_THRESHOLD);
        }
        return provider;
    }
}
