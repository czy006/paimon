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
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CopyPartResult;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Completion requests must satisfy SSE-C policies on both upload and multipart-copy paths. */
class S3NativeCompleteEncryptionTest {

    private static final int PART_SIZE = 5 << 20;
    private static final long COPY_SIZE = (5L << 30) + 1;
    private static final String CUSTOMER_KEY = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    private static final String CUSTOMER_MD5 = "cLyPS3KoaSFGi/joRB3OUQ==";

    @TempDir java.nio.file.Path tmpDir;

    @ParameterizedTest
    @ValueSource(strings = {"custom", "none", "s3", "kms", "dsse-kms"})
    void testOutputCompletionEncryption(String encryption) throws Exception {
        S3Client sync = mock(S3Client.class);
        S3AsyncClient async = mock(S3AsyncClient.class);
        when(sync.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(
                        CreateMultipartUploadResponse.builder().uploadId("output-upload").build());
        when(async.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                UploadPartResponse.builder().eTag("part").build()));
        AtomicReference<CompleteMultipartUploadRequest> complete = new AtomicReference<>();
        requireCompletionPolicy(sync, encryption, complete);

        try (S3NativePositionOutputStream output =
                new S3NativePositionOutputStream(
                        sync,
                        async,
                        "bucket",
                        "output",
                        S3NativeOptions.from(options(encryption)))) {
            output.write(new byte[PART_SIZE + 1]);
        }

        assertCompletion(complete.get(), encryption, "output-upload");
        verify(async, times(2))
                .uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class));
        assertThat(tmpDir.toFile().listFiles()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"custom", "none", "s3", "kms", "dsse-kms"})
    void testMultipartCopyCompletionEncryption(String encryption) throws Exception {
        S3Client sync = mock(S3Client.class);
        when(sync.headObject(any(HeadObjectRequest.class)))
                .thenAnswer(
                        invocation -> {
                            HeadObjectRequest request = invocation.getArgument(0);
                            if ("source".equals(request.key())) {
                                return HeadObjectResponse.builder()
                                        .contentLength(COPY_SIZE)
                                        .eTag("source-etag")
                                        .build();
                            }
                            throw NoSuchKeyException.builder().statusCode(404).build();
                        });
        when(sync.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(ListObjectsV2Response.builder().build());
        when(sync.getObjectTagging(any(GetObjectTaggingRequest.class)))
                .thenReturn(GetObjectTaggingResponse.builder().build());
        when(sync.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(
                        CreateMultipartUploadResponse.builder().uploadId("copy-upload").build());
        when(sync.uploadPartCopy(any(UploadPartCopyRequest.class)))
                .thenReturn(
                        UploadPartCopyResponse.builder()
                                .copyPartResult(CopyPartResult.builder().eTag("part").build())
                                .build());
        AtomicReference<CompleteMultipartUploadRequest> complete = new AtomicReference<>();
        requireCompletionPolicy(sync, encryption, complete);

        assertThat(renameWithMock(sync, options(encryption))).isTrue();

        assertCompletion(complete.get(), encryption, "copy-upload");
        verify(sync, times(2)).uploadPartCopy(any(UploadPartCopyRequest.class));
        verify(sync).deleteObject(any(DeleteObjectRequest.class));
    }

    private Options options(String encryption) {
        Options raw = new Options();
        raw.set("s3.region", "us-east-1");
        raw.set("s3.endpoint", "https://" + UUID.randomUUID() + ".invalid");
        raw.set("s3.upload.tmp.dir", tmpDir.toString());
        raw.set("s3.upload.min.part.size", "5 mb");
        raw.set("s3.multipart.threshold", "1");
        raw.set("s3.sse.type", encryption);
        raw.set("s3.sse.key", CUSTOMER_KEY);
        raw.set("s3.sse.md5", CUSTOMER_MD5);
        return raw;
    }

    private static void requireCompletionPolicy(
            S3Client client,
            String encryption,
            AtomicReference<CompleteMultipartUploadRequest> complete) {
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenAnswer(
                        invocation -> {
                            CompleteMultipartUploadRequest request = invocation.getArgument(0);
                            complete.set(request);
                            // Model the documented SSE-C bucket policy/checksum conditions.
                            if ("custom".equals(encryption)
                                    && (request.sseCustomerAlgorithm() == null
                                            || request.sseCustomerKey() == null
                                            || request.sseCustomerKeyMD5() == null)) {
                                throw S3Exception.builder()
                                        .statusCode(403)
                                        .message("SSE-C completion headers required")
                                        .build();
                            }
                            return CompleteMultipartUploadResponse.builder()
                                    .eTag("complete")
                                    .build();
                        });
    }

    private static void assertCompletion(
            CompleteMultipartUploadRequest request, String encryption, String uploadId) {
        assertThat(request).isNotNull();
        assertThat(request.uploadId()).isEqualTo(uploadId);
        assertThat(request.multipartUpload().parts()).hasSize(2);
        if ("custom".equals(encryption)) {
            assertThat(request.sseCustomerAlgorithm()).isEqualTo("AES256");
            assertThat(request.sseCustomerKey()).isEqualTo(CUSTOMER_KEY);
            assertThat(request.sseCustomerKeyMD5()).isEqualTo(CUSTOMER_MD5);
        } else {
            assertThat(request.sseCustomerAlgorithm()).isNull();
            assertThat(request.sseCustomerKey()).isNull();
            assertThat(request.sseCustomerKeyMD5()).isNull();
        }
    }

    @SuppressWarnings("unchecked")
    private static boolean renameWithMock(S3Client client, Options options) throws Exception {
        S3NativeFileIO io = new S3NativeFileIO();
        io.configure(CatalogContext.create(options));
        Constructor<S3NativeClientProvider> constructor =
                S3NativeClientProvider.class.getDeclaredConstructor(
                        S3Client.class, S3AsyncClient.class, AwsCredentialsProvider.class);
        constructor.setAccessible(true);
        Field clients = S3NativeFileIO.class.getDeclaredField("CLIENTS");
        clients.setAccessible(true);
        Map<S3NativeFileIO.ClientKey, S3NativeClientProvider> cache =
                (Map<S3NativeFileIO.ClientKey, S3NativeClientProvider>) clients.get(null);
        S3NativeFileIO.ClientKey key = S3NativeFileIO.ClientKey.from(S3NativeOptions.from(options));
        try (S3NativeClientProvider provider =
                constructor.newInstance(client, mock(S3AsyncClient.class), null)) {
            cache.put(key, provider);
            try {
                return io.rename(
                        new Path("s3://bucket/source"), new Path("s3://bucket/destination"));
            } finally {
                cache.remove(key, provider);
            }
        }
    }
}
