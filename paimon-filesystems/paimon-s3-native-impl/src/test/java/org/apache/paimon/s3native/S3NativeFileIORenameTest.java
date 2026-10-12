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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
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
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3NativeFileIORenameTest {

    static final long LARGE_OBJECT_SIZE = (5L << 30) + 1;
    static final Path SOURCE = new Path("s3://bucket/source");
    static final Path DESTINATION = new Path("s3://bucket/destination");

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testAbortedCopyDoesNotDeleteSourceWhenRecoveryCannotBeProven(boolean headDenied)
            throws Exception {
        S3Client client = mock(S3Client.class);
        when(client.getObjectTagging(any(GetObjectTaggingRequest.class)))
                .thenReturn(GetObjectTaggingResponse.builder().build());
        AtomicInteger destinationHeads = new AtomicInteger();
        when(client.headObject(any(HeadObjectRequest.class)))
                .thenAnswer(
                        invocation -> {
                            HeadObjectRequest request = invocation.getArgument(0);
                            if (request.key().equals("source")) {
                                return HeadObjectResponse.builder()
                                        .contentLength(LARGE_OBJECT_SIZE)
                                        .build();
                            }
                            if (destinationHeads.getAndIncrement() == 0) {
                                throw NoSuchKeyException.builder().statusCode(404).build();
                            }
                            if (headDenied) {
                                throw S3Exception.builder().statusCode(403).build();
                            }
                            return HeadObjectResponse.builder()
                                    .contentLength(LARGE_OBJECT_SIZE)
                                    .eTag("unrelated-object")
                                    .build();
                        });
        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(ListObjectsV2Response.builder().build());
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder().uploadId("copy").build());
        when(client.uploadPartCopy(any(UploadPartCopyRequest.class)))
                .thenReturn(
                        UploadPartCopyResponse.builder()
                                .copyPartResult(CopyPartResult.builder().eTag("part").build())
                                .build());
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenThrow(NoSuchUploadException.builder().statusCode(404).build());

        S3NativeFileIO io = fileIO(client, new Options());
        assertThatThrownBy(() -> io.rename(SOURCE, DESTINATION)).isInstanceOf(IOException.class);
        verify(client, never()).deleteObject(any(DeleteObjectRequest.class));
        verify(client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    @Test
    void testLostCopyResponseRecoversThisUploadAndDeletesSource() throws Exception {
        S3Client client = mock(S3Client.class);
        when(client.getObjectTagging(any(GetObjectTaggingRequest.class)))
                .thenReturn(GetObjectTaggingResponse.builder().build());
        AtomicReference<CreateMultipartUploadRequest> create = new AtomicReference<>();
        when(client.headObject(any(HeadObjectRequest.class)))
                .thenAnswer(
                        invocation -> {
                            HeadObjectRequest request = invocation.getArgument(0);
                            if (request.key().equals("source")) {
                                return HeadObjectResponse.builder()
                                        .contentLength(LARGE_OBJECT_SIZE)
                                        .build();
                            }
                            if (create.get() == null) {
                                throw NoSuchKeyException.builder().statusCode(404).build();
                            }
                            return HeadObjectResponse.builder()
                                    .contentLength(LARGE_OBJECT_SIZE)
                                    .metadata(create.get().metadata())
                                    .build();
                        });
        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(ListObjectsV2Response.builder().build());
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenAnswer(
                        invocation -> {
                            create.set(invocation.getArgument(0));
                            return CreateMultipartUploadResponse.builder().uploadId("copy").build();
                        });
        when(client.uploadPartCopy(any(UploadPartCopyRequest.class)))
                .thenReturn(
                        UploadPartCopyResponse.builder()
                                .copyPartResult(CopyPartResult.builder().eTag("part").build())
                                .build());
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenThrow(NoSuchUploadException.builder().statusCode(404).build());

        assertThat(fileIO(client, new Options()).rename(SOURCE, DESTINATION)).isTrue();
        verify(client).deleteObject(any(DeleteObjectRequest.class));
    }

    @SuppressWarnings("unchecked")
    static S3NativeFileIO fileIO(S3Client client, Options options) throws Exception {
        options.set("s3.region", "us-east-1");
        options.set("s3.endpoint", "http://" + UUID.randomUUID() + ".invalid");
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
        cache.put(
                S3NativeFileIO.ClientKey.from(S3NativeOptions.from(options)),
                constructor.newInstance(client, mock(S3AsyncClient.class), null));
        return io;
    }
}
