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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Validates object offsets and detects truncation before the length captured by HEAD. */
class S3NativeReadResponseTest {

    private static final String ETAG = "\"stable\"";

    static Stream<Arguments> invalidResponses() {
        // Each case is exercised by single-byte, bulk, and independent positioned reads.
        Stream<Arguments> invalidForAllReads =
                Stream.of(
                                new Object[] {0, 200, 1L, null},
                                new Object[] {0, 200, null, null},
                                new Object[] {0, 200, 2L, "bytes 0-1/2"},
                                new Object[] {0, 201, 2L, null},
                                new Object[] {0, 206, 1L, "bytes 0-0/2"},
                                new Object[] {0, 206, 2L, "bytes 0-1/3"},
                                new Object[] {1, 200, 2L, null},
                                new Object[] {1, 206, 1L, null},
                                new Object[] {1, 206, 1L, "bytes 0-0/2"},
                                new Object[] {1, 206, 1L, "bytes 1-2/2"},
                                new Object[] {1, 206, 1L, "bytes 1-1/3"},
                                new Object[] {1, 206, 2L, "bytes 1-1/2"},
                                new Object[] {1, 206, null, "bytes 1-1/2"},
                                new Object[] {1, 206, 1L, "bytes 1-1/*"},
                                new Object[] {1, 206, 1L, "bytes 1-1/9223372036854775808"},
                                new Object[] {1, 0, 1L, "bytes 1-1/2"})
                        .flatMap(
                                fields ->
                                        Stream.of("single", "bulk", "pread")
                                                .map(
                                                        method ->
                                                                Arguments.of(
                                                                        method, fields[0],
                                                                        fields[1], fields[2],
                                                                        fields[3])));
        // A complete 206 still needs a Range request; a full positioned read does request one.
        return Stream.concat(
                invalidForAllReads,
                Stream.of(
                        Arguments.of("single", 0, 206, 2L, "bytes 0-1/2"),
                        Arguments.of("bulk", 0, 206, 2L, "bytes 0-1/2")));
    }

    @ParameterizedTest
    @MethodSource("invalidResponses")
    void testInvalidResponseFailsBeforeExposingBytes(
            String method, int start, int status, Long length, String contentRange)
            throws Exception {
        TrackingBody body = new TrackingBody("AB");
        S3Client client = mock(S3Client.class);
        when(client.getObject(any(GetObjectRequest.class)))
                .thenReturn(response(status, length, contentRange, body));
        byte[] buffer = new byte[2];
        try (S3NativeSeekableInputStream in = open(client, 2, 1)) {
            int cursor = "pread".equals(method) ? 1 : start;
            in.seek(cursor);
            assertThatThrownBy(
                            () -> {
                                if ("pread".equals(method)) {
                                    in.pread(start, buffer, 0, 2 - start);
                                } else if ("bulk".equals(method)) {
                                    in.read(buffer, 0, 2);
                                } else {
                                    in.read();
                                }
                            })
                    .isInstanceOf(IOException.class);
            assertThat(in.getPos()).isEqualTo(cursor);
            assertThat(buffer).containsExactly(0, 0);
            assertThat(body.available()).isEqualTo(2);
            assertThat(body.aborted).isTrue();
            assertThat(body.closed).isTrue();
            verify(client, times(1)).getObject(any(GetObjectRequest.class));
        }
        ArgumentCaptor<GetObjectRequest> request = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(client).getObject(request.capture());
        assertThat(request.getValue().ifMatch()).isEqualTo(ETAG);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPrematureSequentialEofFailsAndReleasesResponse(boolean bulk) throws Exception {
        TrackingBody body = new TrackingBody("A");
        S3Client client = mock(S3Client.class);
        when(client.getObject(any(GetObjectRequest.class)))
                .thenReturn(response(200, 2L, null, body));
        byte[] buffer = new byte[2];
        try (S3NativeSeekableInputStream in = open(client, 2, 1)) {
            assertThat(bulk ? in.read(buffer, 0, 1) : in.read()).isEqualTo(bulk ? 1 : 'A');
            assertThatThrownBy(
                            () -> {
                                if (bulk) {
                                    in.read(buffer, 1, 1);
                                } else {
                                    in.read();
                                }
                            })
                    .isInstanceOf(EOFException.class);
            assertThat(in.getPos()).isEqualTo(1);
            assertThat(body.aborted).isTrue();
            assertThat(body.closed).isTrue();
            verify(client, times(1)).getObject(any(GetObjectRequest.class));
        }
    }

    @Test
    void testPrematurePositionedEofFailsWithoutChangingSequentialCursor() throws Exception {
        TrackingBody body = new TrackingBody("A");
        S3Client client = mock(S3Client.class);
        when(client.getObject(any(GetObjectRequest.class)))
                .thenReturn(response(206, 2L, "bytes 0-1/2", body));
        try (S3NativeSeekableInputStream in = open(client, 2, 1)) {
            in.seek(1);
            assertThatThrownBy(() -> in.pread(0, new byte[2], 0, 2))
                    .isInstanceOf(EOFException.class);
            assertThat(in.getPos()).isEqualTo(1);
            assertThat(body.aborted).isTrue();
            assertThat(body.closed).isTrue();
            verify(client, times(1)).getObject(any(GetObjectRequest.class));
        }
    }

    @Test
    void testPrematureEofDuringBufferedSeekFailsWithoutReopening() throws Exception {
        TrackingBody body = new TrackingBody("A");
        S3Client client = mock(S3Client.class);
        when(client.getObject(any(GetObjectRequest.class)))
                .thenReturn(response(200, 4L, null, body));
        try (S3NativeSeekableInputStream in = open(client, 4, 256)) {
            assertThat(in.read()).isEqualTo('A');
            in.seek(3);
            assertThatThrownBy(in::read).isInstanceOf(EOFException.class);
            assertThat(in.getPos()).isEqualTo(3);
            assertThat(body.aborted).isTrue();
            assertThat(body.closed).isTrue();
            verify(client, times(1)).getObject(any(GetObjectRequest.class));
        }
    }

    @Test
    void testReadRetryValidatesTheReopenedRange() throws Exception {
        InputStream resetAfterOneByte =
                new InputStream() {
                    private boolean served;

                    @Override
                    public int read() throws IOException {
                        if (!served) {
                            served = true;
                            return 'A';
                        }
                        throw new SocketException("reset");
                    }
                };
        TrackingBody ignoredRange = new TrackingBody("AB");
        S3Client client = mock(S3Client.class);
        when(client.getObject(any(GetObjectRequest.class)))
                .thenReturn(
                        response(200, 2L, null, resetAfterOneByte),
                        response(200, 2L, null, ignoredRange));
        try (S3NativeSeekableInputStream in = open(client, 2, 1)) {
            assertThat(in.read()).isEqualTo('A');
            assertThatThrownBy(in::read).isInstanceOf(IOException.class);
            assertThat(in.getPos()).isEqualTo(1);
            assertThat(ignoredRange.available()).isEqualTo(2);
            assertThat(ignoredRange.aborted).isTrue();
            assertThat(ignoredRange.closed).isTrue();
        }
        ArgumentCaptor<GetObjectRequest> requests = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(client, times(2)).getObject(requests.capture());
        assertThat(requests.getAllValues().get(1).range()).isEqualTo("bytes=1-");
        assertThat(requests.getAllValues().get(1).ifMatch()).isEqualTo(ETAG);
    }

    @Test
    void testBoundedPositionedReadRejectsAFullResponseAtOffsetZero() throws Exception {
        TrackingBody body = new TrackingBody("AB");
        S3Client client = mock(S3Client.class);
        when(client.getObject(any(GetObjectRequest.class)))
                .thenReturn(response(200, 2L, null, body));
        try (S3NativeSeekableInputStream in = open(client, 2, 1)) {
            byte[] buffer = new byte[1];
            assertThatThrownBy(() -> in.pread(0, buffer, 0, 1)).isInstanceOf(IOException.class);
            assertThat(buffer).containsExactly(0);
            assertThat(in.getPos()).isZero();
            assertThat(body.aborted).isTrue();
            assertThat(body.closed).isTrue();
        }
    }

    @Test
    void testFullPositionedReadAtZeroSupportsIgnoredRange() throws Exception {
        S3Client client = mock(S3Client.class);
        when(client.getObject(any(GetObjectRequest.class)))
                .thenReturn(response(200, 2L, null, new TrackingBody("AB")));
        try (S3NativeSeekableInputStream in = open(client, 2, 1)) {
            in.seek(1);
            byte[] buffer = new byte[2];
            assertThat(in.pread(0, buffer, 0, 2)).isEqualTo(2);
            assertThat(buffer).containsExactly('A', 'B');
            assertThat(in.getPos()).isEqualTo(1);
        }
        ArgumentCaptor<GetObjectRequest> request = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(client).getObject(request.capture());
        assertThat(request.getValue().range()).isEqualTo("bytes=0-1");
        assertThat(request.getValue().ifMatch()).isEqualTo(ETAG);
    }

    @Test
    void testCorrectFullAndRangedResponsesReadToActualEof() throws Exception {
        S3Client client = mock(S3Client.class);
        when(client.getObject(any(GetObjectRequest.class)))
                .thenReturn(
                        response(200, 3L, null, new TrackingBody("ABC")),
                        response(206, 2L, "bytes 1-2/3", new TrackingBody("BC")),
                        response(206, 1L, "bytes 2-2/3", new TrackingBody("C")));
        try (S3NativeSeekableInputStream in = open(client, 3, 1)) {
            assertThat(in.read()).isEqualTo('A');
            byte[] buffer = new byte[4];
            assertThat(in.pread(1, buffer, 0, 4)).isEqualTo(2);
            assertThat(buffer).containsExactly('B', 'C', 0, 0);
            assertThat(in.getPos()).isEqualTo(1);
            in.seek(2); // consume B in the existing stream
            assertThat(in.read()).isEqualTo('C');
            assertThat(in.read()).isEqualTo(-1);
            assertThat(in.read(buffer, 0, 4)).isEqualTo(-1);
            assertThat(in.pread(2, buffer, 0, 4)).isEqualTo(1);
            assertThat(buffer[0]).isEqualTo((byte) 'C');
            assertThat(in.getPos()).isEqualTo(3);
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 2})
    void testZeroLengthAndReadsAtMetadataEofDoNotRequestAnObject(long length) throws Exception {
        S3Client client = mock(S3Client.class);
        try (S3NativeSeekableInputStream in = open(client, length, 1)) {
            assertThat(in.read(new byte[0], 0, 0)).isZero();
            assertThat(in.pread(0, new byte[0], 0, 0)).isZero();
            in.seek(length);
            assertThat(in.read()).isEqualTo(-1);
            assertThat(in.read(new byte[1], 0, 1)).isEqualTo(-1);
            assertThat(in.pread(length, new byte[1], 0, 1)).isEqualTo(-1);
            assertThat(in.getPos()).isEqualTo(length);
            verify(client, times(0)).getObject(any(GetObjectRequest.class));
        }
    }

    private static S3NativeSeekableInputStream open(S3Client client, long length, int bufferSize) {
        return new S3NativeSeekableInputStream(
                client, "bucket", "key", length, bufferSize, S3NativeSse.NONE, ETAG);
    }

    private static ResponseInputStream<GetObjectResponse> response(
            int status, Long length, String contentRange, InputStream body) {
        GetObjectResponse.Builder response =
                GetObjectResponse.builder()
                        .contentLength(length)
                        .contentRange(contentRange)
                        .eTag(ETAG);
        if (status != 0) {
            response.sdkHttpResponse(SdkHttpResponse.builder().statusCode(status).build());
        }
        return new ResponseInputStream<>(
                response.build(),
                AbortableInputStream.create(
                        body,
                        () -> {
                            if (body instanceof TrackingBody) {
                                ((TrackingBody) body).aborted = true;
                            }
                        }));
    }

    private static class TrackingBody extends ByteArrayInputStream {
        private boolean aborted;
        private boolean closed;

        private TrackingBody(String data) {
            super(data.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
