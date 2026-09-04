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
 */
final class S3NativeSeekableInputStream extends SeekableInputStream {

    private static final Logger LOG = LoggerFactory.getLogger(S3NativeSeekableInputStream.class);

    private final ReentrantLock lock = new ReentrantLock();

    private final S3Client client;
    private final String bucket;
    private final String key;
    private final long contentLength;
    private final int readBufferSize;

    private ResponseInputStream<GetObjectResponse> currentStream;
    private BufferedInputStream bufferedStream;

    /** The position the caller expects to read from next; updated by seek/skip/read. */
    private long nextReadPos;

    /** The actual byte offset of the underlying stream cursor, reconciled lazily. */
    private long streamPos;

    private volatile boolean closed;

    S3NativeSeekableInputStream(
            S3Client client, String bucket, String key, long contentLength, int readBufferSize) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
        this.contentLength = contentLength;
        this.readBufferSize = readBufferSize;
        this.nextReadPos = 0;
        this.streamPos = 0;
    }

    @Override
    public void seek(long desired) throws IOException {
        lock.lock();
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
            lock.unlock();
        }
    }

    @Override
    public long getPos() throws IOException {
        lock.lock();
        try {
            return nextReadPos;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int read() throws IOException {
        lock.lock();
        try {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            if (nextReadPos >= contentLength) {
                return -1;
            }
            lazySeek();
            ensureStreamOpen();
            int data = bufferedStream.read();
            if (data != -1) {
                nextReadPos++;
                streamPos++;
            }
            return data;
        } finally {
            lock.unlock();
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
        lock.lock();
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
            int bytesRead = bufferedStream.read(b, off, toRead);
            if (bytesRead > 0) {
                nextReadPos += bytesRead;
                streamPos += bytesRead;
            }
            return bytesRead;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int available() throws IOException {
        lock.lock();
        try {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            long remaining = contentLength - nextReadPos;
            return (int) Math.min(remaining, Integer.MAX_VALUE);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long skip(long n) throws IOException {
        // Skipping only moves the cursor; no I/O is issued (the next read reconciles lazily).
        lock.lock();
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
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
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
            lock.unlock();
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
            skipBytesInBuffer(diff);
            return;
        }

        openStreamAtCurrentPosition();
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
                    GetObjectRequest.builder().bucket(bucket).key(key);
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
