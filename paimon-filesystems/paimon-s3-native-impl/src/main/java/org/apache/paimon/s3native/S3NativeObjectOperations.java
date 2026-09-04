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
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    /** S3 DeleteObjects accepts at most 1000 keys per request. */
    private static final int MAX_BATCH_DELETE_KEYS = 1000;

    private final S3Client client;
    private final String bucket;

    S3NativeObjectOperations(S3Client client, String bucket) {
        this.client = client;
        this.bucket = bucket;
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
            return client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
        } catch (NoSuchKeyException e) {
            return null;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return null;
            }
            throw toIOException("headObject " + key, e);
        }
    }

    /** Uploads a whole local file with a single PutObject; returns the eTag. */
    String putObject(String key, File file) throws IOException {
        try {
            PutObjectResponse response =
                    client.putObject(
                            PutObjectRequest.builder().bucket(bucket).key(key).build(),
                            RequestBody.fromFile(file.toPath()));
            return response.eTag();
        } catch (S3Exception e) {
            throw toIOException("putObject " + key, e);
        }
    }

    /** [DEVIATION D1] Creates a 0-byte directory marker object at {@code <key>/}. */
    void putMarker(String key) throws IOException {
        try {
            client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(S3PathUtils.markerKey(key))
                            .build(),
                    RequestBody.empty());
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
     */
    void deleteBatch(List<String> keys) throws IOException {
        for (int from = 0; from < keys.size(); from += MAX_BATCH_DELETE_KEYS) {
            List<String> batch =
                    keys.subList(from, Math.min(keys.size(), from + MAX_BATCH_DELETE_KEYS));
            List<ObjectIdentifier> identifiers = new ArrayList<>(batch.size());
            for (String key : batch) {
                identifiers.add(ObjectIdentifier.builder().key(key).build());
            }
            DeleteObjectsResponse response;
            try {
                response =
                        client.deleteObjects(
                                DeleteObjectsRequest.builder()
                                        .bucket(bucket)
                                        .delete(Delete.builder().objects(identifiers).build())
                                        .build());
            } catch (S3Exception e) {
                throw toIOException("deleteObjects (" + batch.size() + " keys)", e);
            }
            // Per-key NotFound errors mean the object is already gone — treated as success
            // per Spec D6; any other per-key error fails the batch.
            List<software.amazon.awssdk.services.s3.model.S3Error> fatalErrors = new ArrayList<>();
            if (response.hasErrors()) {
                for (software.amazon.awssdk.services.s3.model.S3Error error : response.errors()) {
                    String code = error.code() == null ? "" : error.code();
                    if (!"NoSuchKey".equals(code) && !"NotFound".equals(code)) {
                        fatalErrors.add(error);
                    }
                }
            }
            if (!fatalErrors.isEmpty()) {
                throw new IOException(
                        "Batch delete failed for "
                                + fatalErrors.size()
                                + " keys, first: "
                                + fatalErrors.get(0).key()
                                + " reason: "
                                + fatalErrors.get(0).message());
            }
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

    /** Lists all keys under a prefix (no delimiter, pagination); markers included. */
    List<String> listAllKeys(String prefix) throws IOException {
        List<String> keys = new ArrayList<>();
        String token = null;
        try {
            do {
                ListObjectsV2Request.Builder request =
                        ListObjectsV2Request.builder().bucket(bucket).prefix(prefix);
                if (token != null) {
                    request.continuationToken(token);
                }
                ListObjectsV2Response response = client.listObjectsV2(request.build());
                for (S3Object object : response.contents()) {
                    keys.add(object.key());
                }
                token = response.nextContinuationToken();
            } while (token != null);
        } catch (S3Exception e) {
            throw toIOException("listAllKeys " + prefix, e);
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
