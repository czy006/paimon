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
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the mid-stream-read retry ([PORTED-ICE I1]) and unclosed-stream finalization (I3). */
class S3NativeSeekableInputStreamTest {

    private static S3Client clientServing(
            java.util.function.BiFunction<Integer, Integer, InputStream> streamForCall) {
        S3Client client = mock(S3Client.class);
        AtomicInteger calls = new AtomicInteger();
        when(client.getObject(any(GetObjectRequest.class)))
                .thenAnswer(
                        invocation -> {
                            GetObjectRequest request = invocation.getArgument(0);
                            int rangeStart = 0;
                            String range = request.range();
                            if (range != null
                                    && range.startsWith("bytes=")
                                    && range.endsWith("-")) {
                                rangeStart =
                                        Integer.parseInt(
                                                range.substring(
                                                        "bytes=".length(), range.length() - 1));
                            }
                            return new ResponseInputStream<>(
                                    GetObjectResponse.builder().build(),
                                    streamForCall.apply(calls.getAndIncrement(), rangeStart));
                        });
        return client;
    }

    /** An InputStream whose first read throws; subsequent reads delegate to known bytes. */
    private static InputStream failingFirstRead(String delegateBytes) {
        return new InputStream() {
            private final InputStream delegate = new ByteArrayInputStream(delegateBytes.getBytes());
            private boolean failed;

            @Override
            public int read() throws IOException {
                if (!failed) {
                    failed = true;
                    throw new SocketException("connection reset");
                }
                return delegate.read();
            }
        };
    }

    @Test
    void testMidReadSocketFailureRetriedWithCorrectOffset() throws Exception {
        S3Client client =
                clientServing(
                        (call, rangeStart) ->
                                call == 0
                                        ? failingFirstRead("AB")
                                        : new ByteArrayInputStream("AB".getBytes()));

        S3NativeSeekableInputStream in =
                new S3NativeSeekableInputStream(client, "bucket", "key", 2, 256, S3NativeSse.NONE);
        assertThat(in.read()).isEqualTo('A');
        assertThat(in.read()).isEqualTo('B');
        // The reopen went through a fresh GetObject; the retried byte comes from the new stream.
        verify(client, times(2)).getObject(any(GetObjectRequest.class));
        in.close();
    }

    @Test
    void testRetriesExhaustedPropagates() throws Exception {
        S3Client client =
                clientServing(
                        (call, rangeStart) ->
                                new InputStream() {
                                    @Override
                                    public int read() throws IOException {
                                        throw new SocketTimeoutException("stalled");
                                    }
                                });

        S3NativeSeekableInputStream in =
                new S3NativeSeekableInputStream(client, "bucket", "key", 2, 256, S3NativeSse.NONE);
        assertThatThrownBy(in::read).isInstanceOf(IOException.class);
        // 1 initial + 2 retries, then propagate.
        verify(client, times(3)).getObject(any(GetObjectRequest.class));
        in.close();
    }

    @Test
    void testNonRetryableFailureNotRetried() throws Exception {
        S3Client client =
                clientServing(
                        (call, rangeStart) ->
                                new InputStream() {
                                    @Override
                                    public int read() throws IOException {
                                        throw new FileNotFoundException("gone");
                                    }
                                });

        S3NativeSeekableInputStream in =
                new S3NativeSeekableInputStream(client, "bucket", "key", 2, 256, S3NativeSse.NONE);
        assertThatThrownBy(in::read).isInstanceOf(FileNotFoundException.class);
        verify(client, times(1)).getObject(any(GetObjectRequest.class));
        in.close();
    }

    @Test
    void testSpeculativeSkipFailureReopensAtConfirmedOffset() throws Exception {
        // Serve exactly one byte then fail later refills: the first buffered fill gets 'X' only,
        // and the buffered skip for seek(3) fails while refilling. The stream must fall back to
        // a clean reopen with Range bytes=1-, so the byte after the skip is the correct 'Y'.
        String data = "XYZW";
        S3Client client =
                clientServing(
                        (call, rangeStart) ->
                                call == 0
                                        ? new InputStream() {
                                            private boolean served;

                                            @Override
                                            public int read() throws IOException {
                                                if (!served) {
                                                    served = true;
                                                    return 'X';
                                                }
                                                throw new SocketException("reset on refill");
                                            }
                                        }
                                        : new ByteArrayInputStream(
                                                data.substring(rangeStart).getBytes()));

        S3NativeSeekableInputStream in =
                new S3NativeSeekableInputStream(
                        client, "bucket", "key", data.length(), 256, S3NativeSse.NONE);
        assertThat(in.read()).isEqualTo('X');
        in.seek(3); // forward skip of 2 (< readBufferSize) triggers the failing refill
        // lazySeek already committed streamPos to the seek target, so the reopen Range is
        // bytes=3- and the next byte is the correct 'W'.
        assertThat(in.read()).isEqualTo('W');
        in.close();
        verify(client, times(2)).getObject(any(GetObjectRequest.class));
    }

    @Test
    void testFinalizeReleasesUnclosedStream() throws Exception {
        S3Client client =
                clientServing((call, rangeStart) -> new ByteArrayInputStream("AB".getBytes()));
        S3NativeSeekableInputStream in =
                new S3NativeSeekableInputStream(client, "bucket", "key", 2, 256, S3NativeSse.NONE);
        in.read();

        java.lang.reflect.Method finalize =
                S3NativeSeekableInputStream.class.getDeclaredMethod("finalize");
        finalize.setAccessible(true);
        finalize.invoke(in);
        // After finalization the stream is closed for good.
        assertThatThrownBy(in::read).isInstanceOf(IOException.class);
    }

    @Test
    void testRetryableReadFailures() {
        assertThat(S3NativeSeekableInputStream.isRetryableReadFailure(new SocketException("reset")))
                .isTrue();
        // SocketTimeoutException extends InterruptedIOException, not SocketException.
        assertThat(S3NativeSeekableInputStream.isRetryableReadFailure(new SocketTimeoutException()))
                .isTrue();
        assertThat(
                        S3NativeSeekableInputStream.isRetryableReadFailure(
                                new IOException("wrapped", new javax.net.ssl.SSLException("hs"))))
                .isTrue();
    }

    @Test
    void testNonRetryableReadFailures() {
        assertThat(
                        S3NativeSeekableInputStream.isRetryableReadFailure(
                                new FileNotFoundException("gone")))
                .isFalse();
        assertThat(S3NativeSeekableInputStream.isRetryableReadFailure(new IOException("plain")))
                .isFalse();
    }
}
