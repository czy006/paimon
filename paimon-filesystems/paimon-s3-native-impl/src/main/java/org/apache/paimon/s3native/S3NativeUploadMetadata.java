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

import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.util.HashMap;
import java.util.Map;

/** Identifies the object committed by one multipart upload, including multipart copies. */
final class S3NativeUploadMetadata {

    private static final String TOKEN_KEY = "paimon-upload-id";

    private S3NativeUploadMetadata() {}

    static Map<String, String> withToken(Map<String, String> metadata, String token) {
        Map<String, String> result = new HashMap<>(metadata);
        result.put(TOKEN_KEY, token);
        return result;
    }

    static boolean matches(HeadObjectResponse head, String token, long length) {
        // NoSuchUpload also means aborted; an existing key alone cannot prove completion.
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
        return head != null
                && head.contentLength() != null
                && head.contentLength() == length
                && token.equals(head.metadata().get(TOKEN_KEY));
    }
}
