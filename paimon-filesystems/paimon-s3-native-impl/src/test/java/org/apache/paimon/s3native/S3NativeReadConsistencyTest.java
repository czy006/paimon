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
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Guards against mixing bytes from different objects after an overwrite of the same key. */
class S3NativeReadConsistencyTest {

    private static final Path PATH = new Path("s3://bucket/key");
    private static final String ORIGINAL_ETAG = "\"original\"";

    private S3Client client;
    private S3NativeFileIO fileIO;
    private Map<S3NativeFileIO.ClientKey, S3NativeClientProvider> clients;
    private S3NativeFileIO.ClientKey clientKey;
    private final List<GetObjectRequest> requests = new ArrayList<>();
    private String data = "ABCD";
    private String currentETag = ORIGINAL_ETAG;
    private boolean failFirstResponse;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() throws Exception {
        client = mock(S3Client.class);
        when(client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(
                        HeadObjectResponse.builder()
                                .contentLength(4L)
                                .eTag(ORIGINAL_ETAG)
                                .versionId("version-1")
                                .build());
        when(client.getObject(any(GetObjectRequest.class)))
                .thenAnswer(
                        invocation -> {
                            GetObjectRequest request = invocation.getArgument(0);
                            requests.add(request);
                            // Model the S3 service's If-Match check, not an implementation call
                            // order.
                            if (request.ifMatch() != null
                                    && !request.ifMatch().equals(currentETag)) {
                                throw S3Exception.builder()
                                        .statusCode(412)
                                        .message("Precondition Failed")
                                        .build();
                            }
                            int start = 0;
                            int end = data.length();
                            if (request.range() != null) {
                                String[] bounds = request.range().substring(6).split("-", -1);
                                start = Integer.parseInt(bounds[0]);
                                if (!bounds[1].isEmpty()) {
                                    end = Integer.parseInt(bounds[1]) + 1;
                                }
                            }
                            InputStream body =
                                    failFirstResponse && requests.size() == 1
                                            ? failingAfterOneByte()
                                            : new ByteArrayInputStream(
                                                    data.substring(start, end)
                                                            .getBytes(StandardCharsets.UTF_8));
                            GetObjectResponse.Builder response =
                                    GetObjectResponse.builder()
                                            .eTag(currentETag)
                                            .contentLength((long) end - start);
                            response.sdkHttpResponse(
                                    SdkHttpResponse.builder()
                                            .statusCode(request.range() == null ? 200 : 206)
                                            .build());
                            if (request.range() != null) {
                                response.contentRange(
                                        String.format(
                                                "bytes %d-%d/%d", start, end - 1, data.length()));
                            }
                            return new ResponseInputStream<>(response.build(), body);
                        });

        Options options = new Options();
        options.set("s3.region", "us-east-1");
        options.set("s3.endpoint", "http://read-consistency-" + UUID.randomUUID());
        fileIO = new S3NativeFileIO();
        fileIO.configure(CatalogContext.create(options));

        // Inject a client into the normal FileIO opening path without a real network or
        // credentials.
        Constructor<S3NativeClientProvider> constructor =
                S3NativeClientProvider.class.getDeclaredConstructor(
                        S3Client.class, S3AsyncClient.class, AwsCredentialsProvider.class);
        constructor.setAccessible(true);
        Field field = S3NativeFileIO.class.getDeclaredField("CLIENTS");
        field.setAccessible(true);
        clients = (Map<S3NativeFileIO.ClientKey, S3NativeClientProvider>) field.get(null);
        clientKey = S3NativeFileIO.ClientKey.from(S3NativeOptions.from(options));
        clients.put(clientKey, constructor.newInstance(client, mock(S3AsyncClient.class), null));
    }

    @AfterEach
    void cleanup() {
        clients.remove(clientKey);
    }

    @Test
    void testOverwriteBetweenHeadAndFirstReadFails() throws Exception {
        try (SeekableInputStream in = fileIO.newInputStream(PATH)) {
            overwrite();
            assertPreconditionFailure(in::read);
            assertThat(in.getPos()).isZero();
        }
    }

    @Test
    void testSocketRetryRejectsOverwrittenObject() throws Exception {
        failFirstResponse = true;
        try (SeekableInputStream in = fileIO.newInputStream(PATH)) {
            assertThat(in.read()).isEqualTo('A');
            overwrite();
            assertPreconditionFailure(in::read);
            assertThat(in.getPos()).isEqualTo(1);
        }
    }

    @Test
    void testBulkReadRetryRejectsOverwrittenObject() throws Exception {
        failFirstResponse = true;
        byte[] buffer = new byte[4];
        try (SeekableInputStream in = fileIO.newInputStream(PATH)) {
            assertThat(in.read(buffer, 0, 1)).isEqualTo(1);
            assertThat(buffer[0]).isEqualTo((byte) 'A');
            overwrite();
            assertPreconditionFailure(() -> in.read(buffer, 1, 3));
            assertThat(buffer).containsExactly((byte) 'A', (byte) 0, (byte) 0, (byte) 0);
            assertThat(in.getPos()).isEqualTo(1);
        }
    }

    @Test
    void testBackwardSeekRejectsOverwrittenObject() throws Exception {
        try (SeekableInputStream in = fileIO.newInputStream(PATH)) {
            assertThat(in.read()).isEqualTo('A');
            overwrite();
            in.seek(0);
            assertPreconditionFailure(in::read);
            assertThat(in.getPos()).isZero();
        }
    }

    @Test
    void testPositionedReadRejectsOverwrittenObject() throws Exception {
        try (SeekableInputStream in = fileIO.newInputStream(PATH)) {
            assertThat(in.read()).isEqualTo('A');
            overwrite();
            byte[] buffer = new byte[2];
            assertPreconditionFailure(() -> ((VectoredReadable) in).pread(2, buffer, 0, 2));
            assertThat(buffer).containsExactly((byte) 0, (byte) 0);
            assertThat(in.getPos()).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @NullAndEmptySource
    void testHeadWithoutETagRefusesToOpen(String eTag) {
        when(client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(HeadObjectResponse.builder().contentLength(4L).eTag(eTag).build());
        assertThatThrownBy(() -> fileIO.newInputStream(PATH))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("ETag");
        assertThat(requests).isEmpty();
    }

    @Test
    void testUnchangedObjectSupportsRetrySeekAndPositionedRead() throws Exception {
        failFirstResponse = true;
        try (SeekableInputStream in = fileIO.newInputStream(PATH)) {
            assertThat(in.read()).isEqualTo('A');
            assertThat(in.read()).isEqualTo('B'); // reopens after the first response fails
            in.seek(0);
            assertThat(in.read()).isEqualTo('A');
            byte[] buffer = new byte[2];
            assertThat(((VectoredReadable) in).pread(2, buffer, 0, 2)).isEqualTo(2);
            assertThat(buffer).containsExactly((byte) 'C', (byte) 'D');
            assertThat(in.getPos()).isEqualTo(1);
            assertThat(in.read()).isEqualTo('B');
        }
        assertThat(requests).hasSize(4);
        for (GetObjectRequest request : requests) {
            assertThat(request.ifMatch()).isEqualTo(ORIGINAL_ETAG);
            // A pinned ETag works with existing GetObject permission on versioned buckets too.
            assertThat(request.versionId()).isNull();
        }
    }

    private void overwrite() {
        data = "WXYZ"; // deliberately the same key and content length
        currentETag = "\"overwritten\"";
    }

    private static InputStream failingAfterOneByte() {
        return new InputStream() {
            private boolean served;

            @Override
            public int read() throws IOException {
                if (!served) {
                    served = true;
                    return 'A';
                }
                throw new SocketException("connection reset");
            }

            @Override
            public int read(byte[] buffer, int offset, int length) throws IOException {
                if (length == 0) {
                    return 0;
                }
                buffer[offset] = (byte) read();
                return 1;
            }
        };
    }

    private static void assertPreconditionFailure(
            org.assertj.core.api.ThrowableAssert.ThrowingCallable read) {
        assertThatThrownBy(read)
                .isInstanceOf(IOException.class)
                .hasCauseInstanceOf(S3Exception.class);
    }
}
