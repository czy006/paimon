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

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Uses actual SDK clients to ensure plaintext endpoints never receive customer keys. */
class S3NativeSseHttpsTest {

    private static final byte[] BODY = new byte[] {1};
    private static final String KEY = Base64.getEncoder().encodeToString(new byte[32]);

    private HttpServer server;
    private final AtomicInteger receivedRequests = new AtomicInteger();
    private String endpoint;
    private String oldLegacyEndpoint;
    private String oldSdkEndpoint;

    @BeforeEach
    void setup() throws Exception {
        oldLegacyEndpoint = System.getProperty("s3.endpoint");
        oldSdkEndpoint = System.getProperty("aws.endpointUrlS3");
        System.clearProperty("s3.endpoint");
        System.clearProperty("aws.endpointUrlS3");
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    receivedRequests.incrementAndGet();
                    byte[] buffer = new byte[1024];
                    while (exchange.getRequestBody().read(buffer) != -1) {
                        // Drain the request to let a real async SDK upload finish.
                    }
                    exchange.getResponseHeaders().add("ETag", "\"test-etag\"");
                    if (exchange.getRequestURI().getQuery() != null
                            && exchange.getRequestURI().getQuery().contains("uploadId=")) {
                        byte[] response =
                                ("<CopyPartResult><LastModified>2026-10-12T00:00:00Z</LastModified>"
                                                + "<ETag>\"test-etag\"</ETag></CopyPartResult>")
                                        .getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(200, response.length);
                        exchange.getResponseBody().write(response);
                    } else {
                        exchange.sendResponseHeaders(200, -1);
                    }
                    exchange.close();
                });
        server.start();
        endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void cleanup() {
        server.stop(0);
        restoreProperty("s3.endpoint", oldLegacyEndpoint);
        restoreProperty("aws.endpointUrlS3", oldSdkEndpoint);
    }

    @ParameterizedTest
    @ValueSource(strings = {"http://localhost:9000", "ftp://localhost:9000", "localhost:9000"})
    void testCustomerKeyConfigurationRejectsNonHttpsEndpoint(String endpoint) throws Exception {
        Options options = customerOptions();
        options.set("s3.endpoint", endpoint);
        assertThatThrownBy(() -> S3NativeOptions.from(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("HTTPS")
                .hasMessageNotContaining(KEY);
    }

    @Test
    void testLegacyEndpointFallbackAlsoRejectsCustomerKeyOverHttp() throws Exception {
        System.setProperty("s3.endpoint", endpoint);
        assertThatThrownBy(() -> S3NativeOptions.from(customerOptions()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("HTTPS");
    }

    @Test
    void testHttpsAndDefaultAwsEndpointsAllowCustomerConfiguration() throws Exception {
        Options options = customerOptions();
        options.set("s3.endpoint", "https://localhost:9000");
        assertThatCode(() -> S3NativeOptions.from(options)).doesNotThrowAnyException();
        assertThatCode(() -> S3NativeOptions.from(customerOptions())).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @CsvSource({
        "false, false, false",
        "false, true, false",
        "true, false, false",
        "true, true, false",
        "false, false, true",
        "false, true, true",
        "true, false, true",
        "true, true, true"
    })
    void testHttpCustomerAndCopySourceKeysNeverReachServer(
            boolean async, boolean copySource, boolean implicitEndpoint) throws Exception {
        Options options = clientOptions();
        if (implicitEndpoint) {
            // SDK-native endpoint overrides bypass the Paimon option's fail-fast validation.
            System.setProperty("aws.endpointUrlS3", endpoint);
        } else {
            options.set("s3.endpoint", endpoint);
        }
        // The provider is configured with SSE NONE, like a cached provider shared by catalogs.
        // Every request must be checked independently of the provider's initial SSE options.
        try (S3NativeClientProvider provider =
                S3NativeClientProvider.create(S3NativeOptions.from(options))) {
            assertThatThrownBy(() -> sendKeyRequest(provider, async, copySource))
                    .hasStackTraceContaining("HTTPS")
                    .hasStackTraceContaining("SSE-C");
            assertThat(receivedRequests.get()).isZero();
        }
    }

    @Test
    void testPlainHttpWithoutCustomerKeysStillSupportsSyncAndAsync() throws Exception {
        Options options = clientOptions();
        options.set("s3.endpoint", endpoint);
        try (S3NativeClientProvider provider =
                S3NativeClientProvider.create(S3NativeOptions.from(options))) {
            PutObjectRequest request =
                    PutObjectRequest.builder().bucket("bucket").key("key").build();
            provider.syncClient().putObject(request, RequestBody.fromBytes(BODY));
            provider.asyncClient().putObject(request, AsyncRequestBody.fromBytes(BODY)).join();
        }
        assertThat(receivedRequests.get()).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "x-amz-server-side-encryption-customer-key",
                "x-amz-copy-source-server-side-encryption-customer-key"
            })
    void testHttpsInterceptorPreservesCustomerKeyRequests(String keyHeader) throws Exception {
        try (S3NativeClientProvider provider =
                S3NativeClientProvider.create(S3NativeOptions.from(clientOptions()))) {
            ExecutionInterceptor interceptor =
                    provider.syncClient().serviceClientConfiguration().overrideConfiguration()
                            .executionInterceptors().stream()
                            .filter(
                                    i ->
                                            i.getClass()
                                                    .getSimpleName()
                                                    .equals("S3NativeSseHttpsInterceptor"))
                            .findFirst()
                            .orElseThrow(
                                    () -> new AssertionError("Missing SSE-C HTTPS interceptor"));
            SdkHttpRequest request =
                    SdkHttpFullRequest.builder()
                            .uri(URI.create("https://example.com/key"))
                            .method(SdkHttpMethod.PUT)
                            .putHeader(keyHeader, KEY)
                            .build();
            Context.ModifyHttpRequest modifyContext = mock(Context.ModifyHttpRequest.class);
            when(modifyContext.httpRequest()).thenReturn(request);
            Context.BeforeTransmission transmissionContext = mock(Context.BeforeTransmission.class);
            when(transmissionContext.httpRequest()).thenReturn(request);
            ExecutionAttributes attributes = new ExecutionAttributes();
            assertThat(interceptor.modifyHttpRequest(modifyContext, attributes)).isSameAs(request);
            assertThatCode(() -> interceptor.beforeTransmission(transmissionContext, attributes))
                    .doesNotThrowAnyException();
            assertThat(request.firstMatchingHeader(keyHeader)).contains(KEY);
        }
    }

    private static void sendKeyRequest(
            S3NativeClientProvider provider, boolean async, boolean copySource) throws Exception {
        String md5 =
                Base64.getEncoder()
                        .encodeToString(MessageDigest.getInstance("MD5").digest(new byte[32]));
        if (copySource) {
            UploadPartCopyRequest request =
                    UploadPartCopyRequest.builder()
                            .sourceBucket("bucket")
                            .sourceKey("source")
                            .destinationBucket("bucket")
                            .destinationKey("destination")
                            .uploadId("upload")
                            .partNumber(1)
                            .copySourceSSECustomerAlgorithm("AES256")
                            .copySourceSSECustomerKey(KEY)
                            .copySourceSSECustomerKeyMD5(md5)
                            .build();
            if (async) {
                provider.asyncClient().uploadPartCopy(request).join();
            } else {
                provider.syncClient().uploadPartCopy(request);
            }
        } else {
            PutObjectRequest request =
                    PutObjectRequest.builder()
                            .bucket("bucket")
                            .key("key")
                            .sseCustomerAlgorithm("AES256")
                            .sseCustomerKey(KEY)
                            .sseCustomerKeyMD5(md5)
                            .build();
            if (async) {
                provider.asyncClient().putObject(request, AsyncRequestBody.fromBytes(BODY)).join();
            } else {
                provider.syncClient().putObject(request, RequestBody.fromBytes(BODY));
            }
        }
    }

    private static Options customerOptions() throws Exception {
        Options options = new Options();
        options.set("s3.sse.type", "custom");
        options.set("s3.sse.key", KEY);
        options.set(
                "s3.sse.md5",
                Base64.getEncoder()
                        .encodeToString(MessageDigest.getInstance("MD5").digest(new byte[32])));
        return options;
    }

    private static Options clientOptions() {
        Options options = new Options();
        options.set("s3.region", "us-east-1");
        options.set("s3.access-key", "synthetic-test-access-key");
        options.set("s3.secret-key", "synthetic-test-secret-key");
        options.set("s3.path-style-access", "true");
        options.set("s3.chunked-encoding.enabled", "false");
        options.set("s3.retry.max-num-retries", "0");
        options.set("s3.socket.timeout", "2s");
        options.set("s3.connection.timeout", "2s");
        return options;
    }

    private static void restoreProperty(String name, String value) {
        if (value == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, value);
        }
    }
}
