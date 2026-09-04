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

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * S3 input stream with read-ahead buffering, lazy seek, and automatic stream reopening.
 *
 * <p>{@link #seek(long)} only records the desired position without performing any I/O. All HTTP
 * work is deferred to the next read via {@link #lazySeek()}, so multiple seeks between reads
 * coalesce. A forward seek within {@code readBufferSize} bytes is skipped in-buffer instead of
 * reopening the HTTP connection.
 *
 * <p>[PORTED] Derived from Apache Flink flink-filesystems/flink-s3-fs-native (FLINK-38592, Apache
 * License 2.0), class org.apache.flink.fs.s3native.NativeS3InputStream. Local reference:
 * /Users/SL/javaProject/flink/flink-filesystems/flink-s3-fs-native/src/main/java/org/apache/
 * flink/fs/s3native/NativeS3InputStream.java
 *
 * <p>Also implements {@link VectoredReadable}: {@link #pread(long, byte[], int, int)} issues an
 * independent ranged GET that neither moves the stream cursor nor touches the shared buffer, so it
 * is thread-safe by construction; the inherited default {@code readVectored} parallelizes preads
 * via {@code VectoredReadUtils}.
 */
final class S3NativeSeekableInputStream extends SeekableInputStream implements VectoredReadable {

    private static final Logger LOG = LoggerFactory.getLogger(S3NativeSeekableInputStream.class);

    private final ReentrantLock lock = new ReentrantLock();

    private final S3Client client;
    private final String bucket;
    private final String key;
    private final long contentLength;
    private final int readBufferSize;

    /**
     * [PORTED-ICE Spec §14 I1] Connection failures during an in-flight read are not covered by SDK
     * retries (which only govern request setup); mirror Iceberg S3InputStream's bounded
     * reopen-and-retry for them.
     */
    /**
     * Total attempts per read (1 initial + 2 retries); Iceberg budgets 1+3. The smaller budget
     * trades one extra retry for a tighter failure latency.
     */
    private static final int MAX_READ_ATTEMPTS = 3;

    private ResponseInputStream<GetObjectResponse> currentStream;
    private BufferedInputStream bufferedStream;

    /** The position the caller expects to read from next; updated by seek/skip/read. */
    private long nextReadPos;

    /** The actual byte offset of the underlying stream cursor, reconciled lazily. */
    private long streamPos;

    private volatile boolean closed;

    /** Capture site for the unclosed-stream warning emitted by {@link #finalize()}. */
    private final StackTraceElement[] createStack;

    private final S3NativeSse sse;

    S3NativeSeekableInputStream(
            S3Client client,
            String bucket,
            String key,
            long contentLength,
            int readBufferSize,
            S3NativeSse sse) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
        this.contentLength = contentLength;
        this.readBufferSize = readBufferSize;
        this.sse = sse;
        this.nextReadPos = 0;
        this.streamPos = 0;
        this.createStack = Thread.currentThread().getStackTrace();
    }

    @Override
    public void seek(long desired) throws IOException {
        lock();
        try {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            if (desired < 0) {
                throw new EOFException("Cannot seek to negative position: " + desired);
            }
            if (desired > contentLength) {
                throw new EOFException(
                        "Cannot seek past end of stream: position="
                                + desired
                                + ", length="
                                + contentLength);
            }
            nextReadPos = desired;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public long getPos() throws IOException {
        lock();
        try {
            return nextReadPos;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public int read() throws IOException {
        lock();
        try {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            if (nextReadPos >= contentLength) {
                return -1;
            }
            lazySeek();
            ensureStreamOpen();
            int data;
            int attempts = 0;
            while (true) {
                try {
                    data = bufferedStream.read();
                    break;
                } catch (IOException e) {
                    if (++attempts >= MAX_READ_ATTEMPTS || !isRetryableReadFailure(e)) {
                        throw e;
                    }
                    LOG.warn(
                            "Retrying S3 read of {}/{} after connection failure (attempt {})",
                            bucket,
                            key,
                            attempts);
                    reopenAfterReadFailure();
                }
            }
            if (data != -1) {
                nextReadPos++;
                streamPos++;
            }
            return data;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException("Read buffer must not be null");
        }
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Range [off=%d, len=%d] out of bounds for buffer of length %d",
                            off, len, b.length));
        }
        if (len == 0) {
            return 0;
        }
        lock();
        try {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            if (nextReadPos >= contentLength) {
                return -1;
            }
            lazySeek();
            ensureStreamOpen();
            long remaining = contentLength - nextReadPos;
            int toRead = (int) Math.min(len, remaining);
            int bytesRead;
            int attempts = 0;
            while (true) {
                try {
                    bytesRead = bufferedStream.read(b, off, toRead);
                    break;
                } catch (IOException e) {
                    if (++attempts >= MAX_READ_ATTEMPTS || !isRetryableReadFailure(e)) {
                        throw e;
                    }
                    LOG.warn(
                            "Retrying S3 read of {}/{} after connection failure (attempt {})",
                            bucket,
                            key,
                            attempts);
                    reopenAfterReadFailure();
                }
            }
            if (bytesRead > 0) {
                nextReadPos += bytesRead;
                streamPos += bytesRead;
            }
            return bytesRead;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public int available() throws IOException {
        lock();
        try {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            long remaining = contentLength - nextReadPos;
            return (int) Math.min(remaining, Integer.MAX_VALUE);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public long skip(long n) throws IOException {
        // Skipping only moves the cursor; no I/O is issued (the next read reconciles lazily).
        lock();
        try {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            if (n <= 0) {
                return 0;
            }
            long newPos = Math.min(nextReadPos + n, contentLength);
            long skipped = newPos - nextReadPos;
            nextReadPos = newPos;
            return skipped;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            IOException exception = releaseStreams();
            if (exception != null) {
                throw exception;
            }
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Acquires the lock interruptibly, mapping interruption to IOException as callers expect.
     *
     * <p>[PORTED] NativeS3InputStream#lock.
     */
    private void lock() throws IOException {
        try {
            this.lock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while acquiring lock", e);
        }
    }

    @Override
    public int pread(long position, byte[] buffer, int offset, int length) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        if (buffer == null) {
            throw new NullPointerException("Read buffer must not be null");
        }
        if (position < 0 || offset < 0 || length < 0 || length > buffer.length - offset) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "pread position=%d, off=%d, len=%d out of bounds for buffer of length %d",
                            position, offset, length, buffer.length));
        }
        if (length == 0) {
            return 0;
        }
        if (position >= contentLength) {
            return -1;
        }
        int toRead = (int) Math.min(length, contentLength - position);

        // Independent ranged request: does not use or disturb the shared stream/cursor state.
        String range = String.format("bytes=%d-%d", position, position + toRead - 1);
        try (ResponseInputStream<GetObjectResponse> in =
                client.getObject(
                        sseApply(GetObjectRequest.builder().bucket(bucket).key(key).range(range))
                                .build())) {
            int readBytes = 0;
            while (readBytes < toRead) {
                int n = in.read(buffer, offset + readBytes, toRead - readBytes);
                if (n < 0) {
                    break;
                }
                readBytes += n;
            }
            return readBytes == 0 ? -1 : readBytes;
        } catch (IOException | RuntimeException e) {
            throw new IOException(
                    String.format("pread failed for %s/%s at %d", bucket, key, position), e);
        }
    }

    static String formatCreateStackTrace(StackTraceElement[] stack) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < stack.length; i++) {
            sb.append(stack[i]).append("\n\t");
        }
        return sb.toString();
    }

    /**
     * [PORTED-ICE Spec §14 I3] Last-resort release for streams a reader failed to close; mirrors
     * Iceberg S3InputStream#finalize.
     */
    @Override
    @SuppressWarnings({"Finalize", "deprecation"})
    protected void finalize() throws Throwable {
        super.finalize();
        if (!closed) {
            lock.lock();
            try {
                closed = true;
                releaseStreams();
            } finally {
                lock.unlock();
            }
            LOG.warn(
                    "Unclosed input stream created by:\n\t{}", formatCreateStackTrace(createStack));
        }
    }

    // ------------------------------------------------------------------------

    /** Reconciles {@link #nextReadPos} and {@link #streamPos} before reading bytes. */
    private void lazySeek() throws IOException {
        long targetPos = nextReadPos;

        if (currentStream == null) {
            streamPos = targetPos;
            return;
        }

        if (targetPos == streamPos) {
            return;
        }

        long diff = targetPos - streamPos;
        streamPos = targetPos;

        if (targetPos >= contentLength) {
            releaseStreams();
            return;
        }

        // BufferedInputStream does not expose how many bytes remain in its local array, so
        // readBufferSize is the skip threshold: at most readBufferSize bytes may be consumed
        // from the live HTTP connection before a range request becomes preferable.
        if (diff > 0 && diff <= (long) readBufferSize) {
            try {
                skipBytesInBuffer(diff);
                return;
            } catch (IOException e) {
                if (!isRetryableReadFailure(e)) {
                    throw e;
                }
                // Buffered skip hit the network mid-way, leaving the buffer state indeterminate;
                // fall through to a clean reopen at streamPos (Iceberg does the same in
                // positionStream).
            }
        }

        openStreamAtCurrentPosition();
    }

    /** Reopens the stream at the last confirmed position after a mid-read failure. */
    private void reopenAfterReadFailure() throws IOException {
        // Counters only advance on bytes returned to the caller (the speculative in-buffer skip
        // aside, which guards itself above), so streamPos already points at the recovery offset;
        // the failed stream's buffered bytes are simply discarded.
        openStreamAtCurrentPosition();
    }

    /**
     * Whether the failure looks like a dropped/stalled connection. Walks the cause chain, since SDK
     * and HTTP-client wrappers nest the socket exception.
     */
    static boolean isRetryableReadFailure(IOException e) {
        Throwable current = e;
        while (current != null) {
            if (current instanceof javax.net.ssl.SSLException
                    || current instanceof java.net.SocketException
                    // SocketTimeoutException extends InterruptedIOException, not SocketException.
                    || current instanceof java.net.SocketTimeoutException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private GetObjectRequest.Builder sseApply(GetObjectRequest.Builder builder) {
        sse.applyCustomer(builder);
        return builder;
    }

    private void ensureStreamOpen() throws IOException {
        if (currentStream == null && !closed) {
            openStreamAtCurrentPosition();
        }
    }

    private void skipBytesInBuffer(long n) throws IOException {
        long remaining = n;
        while (remaining > 0) {
            long skipped = bufferedStream.skip(remaining);
            if (skipped <= 0) {
                openStreamAtCurrentPosition();
                return;
            }
            remaining -= skipped;
        }
    }

    private void openStreamAtCurrentPosition() throws IOException {
        releaseStreams();
        try {
            GetObjectRequest.Builder requestBuilder =
                    sseApply(GetObjectRequest.builder().bucket(bucket).key(key));
            if (streamPos > 0) {
                requestBuilder.range(String.format("bytes=%d-", streamPos));
            }
            currentStream = client.getObject(requestBuilder.build());
            bufferedStream = new BufferedInputStream(currentStream, readBufferSize);
        } catch (Exception e) {
            releaseStreams();
            throw new IOException("Failed to open S3 stream for " + bucket + "/" + key, e);
        }
    }

    /** Aborts and closes both streams, nulling the references; returns the first failure. */
    private IOException releaseStreams() {
        // Abort the in-flight HTTP connection to avoid draining remaining bytes on close.
        if (currentStream != null) {
            try {
                currentStream.abort();
            } catch (RuntimeException e) {
                LOG.debug("Error aborting S3 response stream for {}/{}", bucket, key, e);
            }
        }
        IOException exception = null;
        if (bufferedStream != null) {
            try {
                bufferedStream.close();
            } catch (IOException e) {
                exception = e;
                LOG.warn("Error closing buffered stream for {}/{}", bucket, key, e);
            } finally {
                bufferedStream = null;
            }
        }
        if (currentStream != null) {
            try {
                currentStream.close();
            } catch (IOException e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
                LOG.warn("Error closing S3 response stream for {}/{}", bucket, key, e);
            } finally {
                currentStream = null;
            }
        }
        return exception;
    }
}
