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

import org.apache.paimon.options.Options;

import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import javax.annotation.Nullable;

/**
 * Server-side encryption settings applied to S3 requests.
 *
 * <p>[PORTED-ICE Spec §14 I8] Adopted from Iceberg S3RequestUtil (configureEncryption family):
 * SSE-S3 / SSE-KMS / DSSE-KMS shape object-creating requests (PutObject, CreateMultipartUpload);
 * SSE-C (customer-managed keys) additionally shapes Get/Head/UploadPart, which is exactly the part
 * easy to miss. Local reference: /Users/SL/javaProject/iceberg/aws/src/main/java/org/
 * apache/iceberg/aws/s3/S3RequestUtil.java
 *
 * <p>Options: {@code s3.sse.type} (none|s3|kms|dsse-kms|custom), {@code s3.sse.key} (KMS key id, or
 * base64 AES-256 key for SSE-C), {@code s3.sse.md5} (base64 MD5 of the SSE-C key).
 */
final class S3NativeSse {

    enum Type {
        NONE,
        SSE_S3,
        KMS,
        DSSE_KMS,
        CUSTOM
    }

    static final S3NativeSse NONE = new S3NativeSse(Type.NONE, null, null, null);

    private final Type type;
    @Nullable private final String kmsKeyId;
    @Nullable private final String customerKey;
    @Nullable private final String customerKeyMd5;

    private S3NativeSse(
            Type type,
            @Nullable String kmsKeyId,
            @Nullable String customerKey,
            @Nullable String customerKeyMd5) {
        this.type = type;
        this.kmsKeyId = kmsKeyId;
        this.customerKey = customerKey;
        this.customerKeyMd5 = customerKeyMd5;
    }

    static S3NativeSse from(Options options) {
        String raw = options.getString("s3.sse.type", "none");
        String key = options.get("s3.sse.key");
        String md5 = options.get("s3.sse.md5");
        switch (raw.toLowerCase(java.util.Locale.ROOT)) {
            case "none":
                return NONE;
            case "s3":
            case "aes256":
                return new S3NativeSse(Type.SSE_S3, null, null, null);
            case "kms":
            case "aws:kms":
                return new S3NativeSse(Type.KMS, key, null, null);
            case "dsse-kms":
            case "aws:kms:dsse":
                return new S3NativeSse(Type.DSSE_KMS, key, null, null);
            case "custom":
                if (key == null || md5 == null) {
                    throw new IllegalArgumentException(
                            "s3.sse.type=custom requires both s3.sse.key (base64 AES-256 key) "
                                    + "and s3.sse.md5 (base64 MD5 of the key)");
                }
                return new S3NativeSse(Type.CUSTOM, null, key, md5);
            default:
                throw new IllegalArgumentException(
                        "Invalid s3.sse.type value: "
                                + raw
                                + " (expected none, s3, kms, dsse-kms or custom)");
        }
    }

    /** Applies SSE-S3/KMS/DSSE and SSE-C to an object-creating request. */
    void apply(PutObjectRequest.Builder builder) {
        switch (type) {
            case SSE_S3:
                builder.serverSideEncryption(ServerSideEncryption.AES256);
                break;
            case KMS:
                builder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
                applyKmsKey(builder::ssekmsKeyId);
                break;
            case DSSE_KMS:
                builder.serverSideEncryption(ServerSideEncryption.AWS_KMS_DSSE);
                applyKmsKey(builder::ssekmsKeyId);
                break;
            case CUSTOM:
                applyCustomer(
                        builder::sseCustomerAlgorithm,
                        builder::sseCustomerKey,
                        builder::sseCustomerKeyMD5);
                break;
            case NONE:
            default:
                break;
        }
    }

    /** Applies SSE-S3/KMS/DSSE and SSE-C to a multipart-creating request. */
    void apply(CreateMultipartUploadRequest.Builder builder) {
        switch (type) {
            case SSE_S3:
                builder.serverSideEncryption(ServerSideEncryption.AES256);
                break;
            case KMS:
                builder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
                applyKmsKey(builder::ssekmsKeyId);
                break;
            case DSSE_KMS:
                builder.serverSideEncryption(ServerSideEncryption.AWS_KMS_DSSE);
                applyKmsKey(builder::ssekmsKeyId);
                break;
            case CUSTOM:
                applyCustomer(
                        builder::sseCustomerAlgorithm,
                        builder::sseCustomerKey,
                        builder::sseCustomerKeyMD5);
                break;
            case NONE:
            default:
                break;
        }
    }

    /** Applies SSE-C to a part-upload request (the only SSE form valid there). */
    void applyCustomer(UploadPartRequest.Builder builder) {
        if (type == Type.CUSTOM) {
            applyCustomer(
                    builder::sseCustomerAlgorithm,
                    builder::sseCustomerKey,
                    builder::sseCustomerKeyMD5);
        }
    }

    /** Applies SSE-C to a read request (the only SSE form valid there). */
    void applyCustomer(GetObjectRequest.Builder builder) {
        if (type == Type.CUSTOM) {
            applyCustomer(
                    builder::sseCustomerAlgorithm,
                    builder::sseCustomerKey,
                    builder::sseCustomerKeyMD5);
        }
    }

    /** Applies SSE-C to a metadata request (the only SSE form valid there). */
    void applyCustomer(HeadObjectRequest.Builder builder) {
        if (type == Type.CUSTOM) {
            applyCustomer(
                    builder::sseCustomerAlgorithm,
                    builder::sseCustomerKey,
                    builder::sseCustomerKeyMD5);
        }
    }

    private void applyKmsKey(java.util.function.Consumer<String> setter) {
        if (kmsKeyId != null) {
            setter.accept(kmsKeyId);
        }
    }

    private void applyCustomer(
            java.util.function.Consumer<String> algorithmSetter,
            java.util.function.Consumer<String> keySetter,
            java.util.function.Consumer<String> md5Setter) {
        algorithmSetter.accept(ServerSideEncryption.AES256.toString());
        keySetter.accept(customerKey);
        md5Setter.accept(customerKeyMd5);
    }
}
