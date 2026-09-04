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
import java.util.ArrayList;
import java.util.Collections;
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
