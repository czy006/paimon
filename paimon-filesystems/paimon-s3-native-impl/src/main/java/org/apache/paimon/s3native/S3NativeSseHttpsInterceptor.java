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

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;

/** Prevents customer-provided encryption keys from being sent to plaintext S3 endpoints. */
final class S3NativeSseHttpsInterceptor implements ExecutionInterceptor {

    @Override
    public SdkHttpRequest modifyHttpRequest(
            Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {
        // SDK 2.44.4 resolves S3 endpoints before user modifyHttpRequest interceptors and signs
        // afterwards. Reject here before signing, including SDK-native endpoint overrides.
        validate(context.httpRequest());
        return context.httpRequest();
    }

    @Override
    public void beforeTransmission(
            Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
        // Recheck the finalized request on every transmission attempt.
        validate(context.httpRequest());
    }

    private static void validate(SdkHttpRequest request) {
        if (!"https".equalsIgnoreCase(request.protocol())
                && request.anyMatchingHeader(
                        name ->
                                "x-amz-server-side-encryption-customer-key".equalsIgnoreCase(name)
                                        || "x-amz-copy-source-server-side-encryption-customer-key"
                                                .equalsIgnoreCase(name))) {
            // Never include request headers or key material in an exception message.
            // https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html
            throw SdkClientException.create(
                    "SSE-C customer keys require HTTPS; refusing a non-HTTPS request");
        }
    }
}
