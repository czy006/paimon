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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Sync S3 metadata/object operations used by {@link S3NativeFileIO}.
 *
 * <p>[PORTED] Derived from Apache Flink flink-filesystems/flink-s3-fs-native (FLINK-38592, Apache
 * License 2.0), class org.apache.flink.fs.s3native.writer.NativeS3ObjectOperations (HeadObject /
 * PutObject / DeleteObject / listing semantics). Local reference: /Users/SL/javaProject/flink/
 * flink-filesystems/flink-s3-fs-native/src/main/java/org/apache/flink/fs/s3native/writer/
 * NativeS3ObjectOperations.java
 *
 * <p>[DEVIATION D1] Adds directory-marker helpers (S3A-compatible 0-byte {@code <key>/} objects).
 * [DEVIATION D6] Adds batch delete (DeleteObjects, up to 1000 keys per request) used by recursive
 * deletion.
 */
final class S3NativeObjectOperations {

    private static final Logger LOG = LoggerFactory.getLogger(S3NativeObjectOperations.class);

    private final S3Client client;
    private final String bucket;
    private final S3NativeSse sse;

    S3NativeObjectOperations(S3Client client, String bucket) {
        this(client, bucket, S3NativeSse.NONE);
    }

    S3NativeObjectOperations(S3Client client, String bucket, S3NativeSse sse) {
        this.client = client;
        this.bucket = bucket;
        this.sse = sse;
    }

    String bucket() {
        return bucket;
    }

    S3Client client() {
        return client;
    }

    /**
     * Returns object metadata, or {@code null} when the key does not exist. 404/NoSuchKey map to
     * {@code null}; other S3 errors propagate as {@link IOException}.
     *
     * <p>[DEVIATION] Flink's getFileStatus special-cases S3's 403 ambiguity (a missing object
     * answers 403 instead of 404 when the caller lacks s3:ListBucket). We propagate 403 as an error
     * — fail-loud on permission misconfiguration rather than reporting paths missing.
     */
    @Nullable
    HeadObjectResponse headObjectOrNull(String key) throws IOException {
        try {
            HeadObjectRequest.Builder builder = HeadObjectRequest.builder().bucket(bucket).key(key);
            sse.applyCustomer(builder);
            return client.headObject(builder.build());
        } catch (NoSuchKeyException e) {
            return null;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return null;
            }
            throw toIOException("headObject " + key, e);
        }
    }

    /** [DEVIATION D1] Creates a 0-byte directory marker object at {@code <key>/}. */
    void putMarker(String key) throws IOException {
        try {
            PutObjectRequest.Builder builder =
                    PutObjectRequest.builder().bucket(bucket).key(S3PathUtils.markerKey(key));
            // Markers must be encrypted like any other object: an SSE-C reader HEADs with key
            // headers, and a plaintext marker would turn every directory probe into a 400.
            sse.apply(builder);
            client.putObject(builder.build(), RequestBody.empty());
        } catch (S3Exception e) {
            throw toIOException("putMarker " + key, e);
        }
    }

    /**
     * Deletes one object. Returns {@code false} when it does not exist (404); 403 and other errors
     * propagate — S3 answers 403 instead of 404 for missing objects when the caller lacks
     * s3:ListBucket permission.
     *
     * <p>[PORTED] NativeS3ObjectOperations#deleteObject.
     */
    boolean deleteObject(String key) throws IOException {
        try {
            client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                LOG.debug("Object not found during delete for key: {}", key);
                return false;
            }
            throw toIOException("deleteObject " + key, e);
        }
    }

    /**
     * [DEVIATION D6] Batch-deletes keys with {@code DeleteObjects} (<=1000 per request). Keys
     * reported as NotFound are treated as success; any other per-key error fails the call.
     *
     * <p>[PORTED-ICE Spec §14 I7] Batches run in parallel on a shared daemon pool sized by {@code
     * s3.delete.num-threads}, mirroring Iceberg S3FileIO#deleteFiles; failures across batches are
     * aggregated into one IOException carrying the count (Iceberg throws a counted
     * BulkDeletionFailureException) instead of failing on the first batch.
     */
    void deleteBatch(List<String> keys, int batchSize, int threads) throws IOException {
        if (keys.isEmpty()) {
            return;
        }
        // Deduplicate like Iceberg's SetMultimap accumulation; duplicates would inflate the
        // failure count and repeat keys inside one request.
        List<String> distinctKeys = new ArrayList<>(new LinkedHashSet<>(keys));
        List<Future<List<String>>> tasks = new ArrayList<>();
        // First whole-request failure is kept as the cause of the aggregated IOException.
        AtomicReference<Exception> firstFailure = new AtomicReference<>();
        ExecutorService pool = deletePool(threads);
        for (int from = 0; from < distinctKeys.size(); from += batchSize) {
            List<String> batch =
                    distinctKeys.subList(from, Math.min(distinctKeys.size(), from + batchSize));
            List<ObjectIdentifier> identifiers = new ArrayList<>(batch.size());
            for (String key : batch) {
                identifiers.add(ObjectIdentifier.builder().key(key).build());
            }
            DeleteObjectsRequest request =
                    DeleteObjectsRequest.builder()
                            .bucket(bucket)
                            .delete(Delete.builder().objects(identifiers).build())
                            .build();
            tasks.add(pool.submit(() -> deleteBatchQuietly(request, batch, firstFailure)));
        }

        List<String> failedKeys = new ArrayList<>();
        for (Future<List<String>> task : tasks) {
            try {
                failedKeys.addAll(task.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Cancel outstanding batches like Iceberg; they would keep issuing requests
                // after the caller aborted.
                for (Future<List<String>> outstanding : tasks) {
                    outstanding.cancel(true);
                }
                throw new IOException("Interrupted while waiting for batch deletions", e);
            } catch (ExecutionException e) {
                // Unexpected: deleteBatchQuietly maps every failure to failed keys.
                throw new IOException("Batch deletion task failed", e.getCause());
            }
        }
        if (!failedKeys.isEmpty()) {
            throw new IOException(
                    "Batch delete failed for "
                            + failedKeys.size()
                            + " keys, first: "
                            + failedKeys.get(0),
                    firstFailure.get());
        }
    }

    /** Runs one DeleteObjects request; returns the keys that failed non-NotFound errors. */
    private List<String> deleteBatchQuietly(
            DeleteObjectsRequest request,
            List<String> batch,
            AtomicReference<Exception> firstFailure) {
        try {
            DeleteObjectsResponse response = client.deleteObjects(request);
            if (!response.hasErrors()) {
                return Collections.emptyList();
            }
            List<String> failed = new ArrayList<>();
            for (S3Error error : response.errors()) {
                String code = error.code() == null ? "" : error.code();
                // Per-key NotFound means the object is already gone — success per Spec D6.
                if (!"NoSuchKey".equals(code) && !"NotFound".equals(code)) {
                    failed.add(error.key() == null ? "?" : error.key());
                }
            }
            return failed;
        } catch (Exception e) {
            // Whole-request failure: S3Exception, but also SdkClientException for network and
            // credential failures (which do not extend S3Exception). Mark the batch's keys
            // failed and remember the first cause.
            firstFailure.compareAndSet(null, e);
            LOG.warn("DeleteObjects call failed for {} keys", batch.size(), e);
            return new ArrayList<>(batch);
        }
    }

    /**
     * Shared daemon pools keyed by thread count — one pool per configured parallelism instead of
     * Iceberg's single static pool, so option changes do not resize a live pool. Core threads time
     * out so pools for abandoned settings reclaim their threads.
     */
    private static final Map<Integer, ExecutorService> DELETE_POOLS = new ConcurrentHashMap<>();

    private static ExecutorService deletePool(int threads) {
        return DELETE_POOLS.computeIfAbsent(
                threads,
                n -> {
                    ThreadFactory factory =
                            new ThreadFactory() {
                                private final AtomicInteger seq = new AtomicInteger();

                                @Override
                                public Thread newThread(Runnable r) {
                                    Thread t =
                                            new Thread(
                                                    r, "paimon-s3-delete-" + seq.incrementAndGet());
                                    t.setDaemon(true);
                                    return t;
                                }
                            };
                    ThreadPoolExecutor executor =
                            new ThreadPoolExecutor(
                                    n,
                                    n,
                                    60L,
                                    TimeUnit.SECONDS,
                                    new LinkedBlockingQueue<>(),
                                    factory);
                    executor.allowCoreThreadTimeOut(true);
                    return executor;
                });
    }

    /**
     * Streams a recursive delete: paginates the prefix with no delimiter and submits a batch delete
     * every {@code batchSize} keys, waiting for the oldest in-flight batch whenever more than
     * {@code 2 * threads} batches are outstanding (backpressure). Memory stays bounded at
     * O(batchSize * 2 * threads) keys instead of materializing the whole listing, and deletion
     * overlaps with pagination. Failure semantics match {@link #deleteBatch}: per-key NotFound is
     * success (Spec D6), whole-request failures mark their keys failed with the first cause
     * preserved, and any failed key throws one aggregated IOException.
     */
    void deletePrefixStreaming(String prefix, int batchSize, int threads) throws IOException {
        Deque<Future<List<String>>> inFlight = new ArrayDeque<>();
        List<String> failedKeys = new ArrayList<>();
        AtomicReference<Exception> firstFailure = new AtomicReference<>();
        ExecutorService pool = deletePool(threads);
        List<String> batch = new ArrayList<>(batchSize);
        String token = null;
        try {
            do {
                ListObjectsV2Request.Builder request =
                        ListObjectsV2Request.builder().bucket(bucket).prefix(prefix);
                if (token != null) {
                    request.continuationToken(token);
                }
                ListObjectsV2Response response = client.listObjectsV2(request.build());
                token = response.nextContinuationToken();
                for (S3Object object : response.contents()) {
                    batch.add(object.key());
                    if (batch.size() == batchSize) {
                        submitBatch(inFlight, batch, firstFailure, pool);
                        batch = new ArrayList<>(batchSize);
                        drainIfCrowded(inFlight, failedKeys, threads);
                    }
                }
            } while (token != null);
            if (!batch.isEmpty()) {
                submitBatch(inFlight, batch, firstFailure, pool);
            }
            collect(inFlight, failedKeys);
        } catch (S3Exception e) {
            throw toIOException("deletePrefixStreaming " + prefix, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            for (Future<List<String>> outstanding : inFlight) {
                outstanding.cancel(true);
            }
            throw new IOException("Interrupted while streaming batch deletions", e);
        }
        if (!failedKeys.isEmpty()) {
            throw new IOException(
                    "Batch delete failed for "
                            + failedKeys.size()
                            + " keys, first: "
                            + failedKeys.get(0),
                    firstFailure.get());
        }
    }

    /** Snapshots the batch into a delete task; keeps the lambda capture effectively final. */
    private void submitBatch(
            Deque<Future<List<String>>> inFlight,
            List<String> keys,
            AtomicReference<Exception> firstFailure,
            ExecutorService pool) {
        inFlight.add(pool.submit(() -> deleteBatchQuietly(buildRequest(keys), keys, firstFailure)));
    }

    private DeleteObjectsRequest buildRequest(List<String> keys) {
        List<ObjectIdentifier> identifiers = new ArrayList<>(keys.size());
        for (String key : keys) {
            identifiers.add(ObjectIdentifier.builder().key(key).build());
        }
        return DeleteObjectsRequest.builder()
                .bucket(bucket)
                .delete(Delete.builder().objects(identifiers).build())
                .build();
    }

    /** Backpressure: waits for the oldest batches until at most {@code 2 * threads} remain. */
    private static void drainIfCrowded(
            Deque<Future<List<String>>> inFlight, List<String> failedKeys, int threads)
            throws InterruptedException, IOException {
        while (inFlight.size() > 2 * threads) {
            failedKeys.addAll(join(inFlight.pollFirst()));
        }
    }

    /** Joins one batch future, mapping task-level failures to IOException. */
    private static List<String> join(Future<List<String>> future)
            throws InterruptedException, IOException {
        try {
            return future.get();
        } catch (ExecutionException e) {
            // Unexpected: deleteBatchQuietly maps every failure to failed keys.
            throw new IOException("Batch deletion task failed", e.getCause());
        }
    }

    private static void collect(Deque<Future<List<String>>> inFlight, List<String> failedKeys)
            throws InterruptedException, IOException {
        while (!inFlight.isEmpty()) {
            failedKeys.addAll(join(inFlight.pollFirst()));
        }
    }

    /** Result of a delimiter listing: immediate object keys and common prefixes. */
    static final class Children {
        final List<S3Object> objects = new ArrayList<>();
        final List<String> commonPrefixes = new ArrayList<>();
    }

    /**
     * Lists the direct children of a prefix using delimiter "/" with pagination. Marker objects
     * (keys ending in "/") are excluded from {@link Children#objects} — they appear as common
     * prefixes instead.
     */
    Children listChildren(String prefix) throws IOException {
        Children children = new Children();
        String token = null;
        try {
            do {
                ListObjectsV2Request.Builder request =
                        ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).delimiter("/");
                if (token != null) {
                    request.continuationToken(token);
                }
                ListObjectsV2Response response = client.listObjectsV2(request.build());
                for (S3Object object : response.contents()) {
                    if (!S3PathUtils.isMarkerKey(object.key())) {
                        children.objects.add(object);
                    }
                }
                for (CommonPrefix commonPrefix : response.commonPrefixes()) {
                    children.commonPrefixes.add(commonPrefix.prefix());
                }
                token = response.nextContinuationToken();
            } while (token != null);
        } catch (S3Exception e) {
            throw toIOException("listChildren " + prefix, e);
        }
        return children;
    }

    /** Lists all objects under a prefix (no delimiter, pagination); markers included. */
    List<S3Object> listAllObjects(String prefix) throws IOException {
        List<S3Object> objects = new ArrayList<>();
        String token = null;
        try {
            do {
                ListObjectsV2Request.Builder request =
                        ListObjectsV2Request.builder().bucket(bucket).prefix(prefix);
                if (token != null) {
                    request.continuationToken(token);
                }
                ListObjectsV2Response response = client.listObjectsV2(request.build());
                objects.addAll(response.contents());
                token = response.nextContinuationToken();
            } while (token != null);
        } catch (S3Exception e) {
            throw toIOException("listAllObjects " + prefix, e);
        }
        return objects;
    }

    /** Lists all keys under a prefix; convenience over {@link #listAllObjects(String)}. */
    List<String> listAllKeys(String prefix) throws IOException {
        List<S3Object> objects = listAllObjects(prefix);
        List<String> keys = new ArrayList<>(objects.size());
        for (S3Object object : objects) {
            keys.add(object.key());
        }
        return keys;
    }

    /**
     * Single-request directory probe: {@code ListObjectsV2(prefix = key + "/", maxKeys = 2)} with
     * NO delimiter — the delimiter form does not return the prefix-equal marker itself (AWS
     * ListObjectsV2 docs, Example 8). Marker-ness is decided by membership of the self-marker
     * {@code key/} in the returned contents, not by sort order: general-purpose buckets and MinIO
     * return keys sorted, but directory buckets are documented unsorted and remain unsupported
     * (Spec §14.2). With sorted responses the marker is necessarily contents[0] when present, so
     * absence from the maxKeys=2 window is conclusive. maxKeys=2 is a defensive margin: on sorted
     * backends maxKeys=1 already suffices.
     *
     * @param key non-empty object key (the bucket root short-circuits before the probe)
     * @return MARKER_DIR if the self-marker is listed, PREFIX_DIR if any other key is listed,
     *     MISSING otherwise.
     */
    DirectoryProbe probeDirectory(String key) throws IOException {
        String prefix = S3PathUtils.markerKey(key);
        try {
            ListObjectsV2Response response =
                    client.listObjectsV2(
                            ListObjectsV2Request.builder()
                                    .bucket(bucket)
                                    .prefix(prefix)
                                    .maxKeys(2)
                                    .build());
            if (response.contents().isEmpty()) {
                return DirectoryProbe.MISSING;
            }
            for (S3Object object : response.contents()) {
                if (object.key().equals(prefix)) {
                    return DirectoryProbe.MARKER_DIR;
                }
            }
            return DirectoryProbe.PREFIX_DIR;
        } catch (S3Exception e) {
            throw toIOException("probeDirectory " + prefix, e);
        }
    }

    /** Result of the single-listing directory probe. */
    enum DirectoryProbe {
        MARKER_DIR,
        PREFIX_DIR,
        MISSING
    }

    /** Whether at least one object exists under the prefix. */
    boolean hasObjectsUnder(String prefix) throws IOException {
        try {
            ListObjectsV2Response response =
                    client.listObjectsV2(
                            ListObjectsV2Request.builder()
                                    .bucket(bucket)
                                    .prefix(prefix)
                                    .maxKeys(1)
                                    .build());
            return !response.contents().isEmpty();
        } catch (S3Exception e) {
            throw toIOException("hasObjectsUnder " + prefix, e);
        }
    }

    private static IOException toIOException(String operation, S3Exception e) {
        return new IOException(
                String.format(
                        "S3 operation failed (%s): status=%d, code=%s",
                        operation,
                        e.statusCode(),
                        e.awsErrorDetails() == null ? "n/a" : e.awsErrorDetails().errorCode()),
                e);
    }
}
