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

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Regression tests through public FileIO and the real SDK/HTTP transport on localhost. */
class S3NativeReadResponseHttpTest {

    private static final Path PATH = new Path("s3://bucket/object");
    private static final String ETAG = "\"stable\"";

    @ParameterizedTest
    @CsvSource({"false, 1", "true, 1", "false, 2", "true, 2"})
    void testPartialResponseToFullGetFailsBeforeReturningData(boolean bulk, int bodyLength)
            throws Exception {
        AtomicInteger gets = new AtomicInteger();
        AtomicReference<String> ifMatch = new AtomicReference<>();
        AtomicReference<String> range = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    exchange.getResponseHeaders().set("ETag", ETAG);
                    exchange.getResponseHeaders().set("Connection", "close");
                    if ("HEAD".equals(exchange.getRequestMethod())) {
                        exchange.getResponseHeaders().set("Content-Length", "2");
                        exchange.sendResponseHeaders(200, -1);
                    } else {
                        gets.incrementAndGet();
                        ifMatch.set(exchange.getRequestHeaders().getFirst("If-Match"));
                        range.set(exchange.getRequestHeaders().getFirst("Range"));
                        exchange.getResponseHeaders()
                                .set("Content-Range", "bytes 0-" + (bodyLength - 1) + "/2");
                        exchange.sendResponseHeaders(206, bodyLength);
                        exchange.getResponseBody()
                                .write(bodyLength == 1 ? new byte[] {'A'} : new byte[] {'A', 'B'});
                    }
                    exchange.close();
                });
        server.start();
        try (S3NativeFileIO fileIO = fileIO(server);
                SeekableInputStream in = fileIO.newInputStream(PATH)) {
            byte[] buffer = new byte[2];
            assertThatThrownBy(
                            () -> {
                                if (bulk) {
                                    in.read(buffer, 0, 2);
                                } else {
                                    in.read();
                                }
                            })
                    .isInstanceOf(IOException.class);
            assertThat(buffer).containsExactly(0, 0);
            assertThat(in.getPos()).isZero();
            assertThat(gets.get()).isEqualTo(1);
            assertThat(ifMatch.get()).isEqualTo(ETAG);
            assertThat(range.get()).isNull();
        } finally {
            server.stop(0);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testIgnoredRangeFailsInsteadOfReturningTheWrongByte(boolean positioned) throws Exception {
        AtomicInteger gets = new AtomicInteger();
        AtomicReference<String> range = new AtomicReference<>();
        AtomicReference<String> ifMatch = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    exchange.getResponseHeaders().set("ETag", ETAG);
                    exchange.getResponseHeaders().set("Connection", "close");
                    if ("HEAD".equals(exchange.getRequestMethod())) {
                        exchange.getResponseHeaders().set("Content-Length", "2");
                        exchange.sendResponseHeaders(200, -1);
                    } else {
                        gets.incrementAndGet();
                        range.set(exchange.getRequestHeaders().getFirst("Range"));
                        ifMatch.set(exchange.getRequestHeaders().getFirst("If-Match"));
                        exchange.sendResponseHeaders(200, 2);
                        exchange.getResponseBody().write(new byte[] {'A', 'B'});
                    }
                    exchange.close();
                });
        server.start();
        try (S3NativeFileIO fileIO = fileIO(server);
                SeekableInputStream in = fileIO.newInputStream(PATH)) {
            in.seek(1);
            byte[] buffer = new byte[1];
            assertThatThrownBy(
                            () -> {
                                if (positioned) {
                                    ((VectoredReadable) in).pread(1, buffer, 0, 1);
                                } else {
                                    in.read();
                                }
                            })
                    .isInstanceOf(IOException.class);
            assertThat(buffer).containsExactly(0);
            assertThat(in.getPos()).isEqualTo(1);
            assertThat(gets.get()).isEqualTo(1);
            assertThat(range.get()).isEqualTo(positioned ? "bytes=1-1" : "bytes=1-");
            assertThat(ifMatch.get()).isEqualTo(ETAG);
        } finally {
            server.stop(0);
        }
    }

    private static S3NativeFileIO fileIO(HttpServer server) {
        Options options = new Options();
        options.set("s3.endpoint", "http://127.0.0.1:" + server.getAddress().getPort());
        options.set("s3.region", "us-east-1");
        options.set("s3.path-style-access", "true");
        options.set("s3.access-key", "synthetic-read-response-test-key");
        options.set("s3.secret-key", "synthetic-read-response-test-secret");
        options.set("s3.retry.max-num-retries", "0");
        S3NativeFileIO fileIO = new S3NativeFileIO();
        fileIO.configure(CatalogContext.create(options));
        return fileIO;
    }
}
