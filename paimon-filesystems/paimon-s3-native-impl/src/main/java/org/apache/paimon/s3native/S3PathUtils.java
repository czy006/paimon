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

import org.apache.paimon.fs.Path;

/**
 * Bucket/key extraction and directory-marker helpers for {@code s3://bucket-name/path/to/object}.
 *
 * <p>Reference: Apache Flink (Apache License 2.0), S3UriUtils#extractKey(Path) and
 * #extractBucketName(Path), from the reviewed master snapshot:
 * https://github.com/apache/flink/blob/6ad337697826a995789a6dafc7d379f64548e34e/flink-filesystems/flink-s3-fs-native/src/main/java/org/apache/flink/fs/s3native/S3UriUtils.java#L37-L50
 *
 * <p>Paimon additionally validates a missing bucket and handles normalized directory-marker keys.
 */
final class S3PathUtils {

    private S3PathUtils() {}

    /** Returns the bucket name (URI host). Fails if the URI carries no authority. */
    static String bucket(Path path) {
        // [PORTED] S3UriUtils#extractBucketName
        String bucket = path.toUri().getHost();
        if (bucket == null || bucket.isEmpty()) {
            throw new IllegalArgumentException(
                    "Invalid S3 path (missing bucket): " + path + ". Expected s3://bucket/key");
        }
        return bucket;
    }

    /**
     * Returns the object key (URI path without leading slash). The root of a bucket maps to the
     * empty key.
     */
    static String key(Path path) {
        // [PORTED] S3UriUtils#extractKey
        String key = path.toUri().getPath();
        if (key == null) {
            return "";
        }
        // [PORTED] S3UriUtils#extractKey — single leading-slash strip; Paimon
        // Path normalization already removes trailing slashes before we get here.
        if (key.startsWith("/")) {
            key = key.substring(1);
        }
        return key;
    }

    /** Key of the directory marker object for the given directory key (trailing slash). */
    static String markerKey(String key) {
        return key.isEmpty() ? "/" : key + "/";
    }

    /** Whether the given object key is a directory marker (ends with slash). */
    static boolean isMarkerKey(String key) {
        return !key.isEmpty() && key.endsWith("/");
    }
}
