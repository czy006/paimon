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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for asynchronous file-body signing when aws-chunked encoding is disabled.
 *
 * <p>SDK 2.44.4 could complete its checksum future before adding the checksum headers. The upstream
 * fix chains header insertion before signing:
 * https://github.com/aws/aws-sdk-java-v2/commit/cdec144e0f61f83bc300d977b9fee72edf6779a4
 */
class S3NativeAsyncSigningTest {

    private static final String ACCESS_KEY = "signing-test-access-key";
    private static final String SECRET_KEY = "signing-test-secret-key";

    @TempDir Path temporaryDirectory;

    static Stream<Arguments> checksumConfigurations() {
        return Stream.of(
                Arguments.of(true, false), Arguments.of(false, false), Arguments.of(false, true));
    }

    @ParameterizedTest
    @MethodSource("checksumConfigurations")
    void testFileUploadPartWithoutChunkEncoding(boolean checksumValidation, boolean explicitCrc32)
            throws Exception {
        byte[] payload = new byte[1024 * 1024 + 13];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i * 31 + 7);
        }
        Path file = temporaryDirectory.resolve("upload-part");
        Files.write(file, payload);
        AtomicReference<byte[]> receivedBody = new AtomicReference<>();
        AtomicReference<Map<String, List<String>>> receivedHeaders = new AtomicReference<>();
        AtomicReference<URI> receivedUri = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    try {
                        receivedBody.set(readFully(exchange.getRequestBody()));
                        Map<String, List<String>> headers = new HashMap<>();
                        exchange.getRequestHeaders()
                                .forEach(
                                        (key, values) -> headers.put(key, new ArrayList<>(values)));
                        receivedHeaders.set(headers);
                        receivedUri.set(exchange.getRequestURI());
                        exchange.getResponseHeaders().set("ETag", "\"test-part\"");
                        exchange.sendResponseHeaders(200, -1);
                    } finally {
                        exchange.close();
                    }
                });
        server.start();
        URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        Options raw = new Options();
        raw.set("s3.region", "us-east-1");
        raw.set("s3.endpoint", endpoint.toString());
        raw.set("s3.path-style-access", "true");
        raw.set("s3.access-key", ACCESS_KEY);
        raw.set("s3.secret-key", SECRET_KEY);
        raw.set("s3.chunked-encoding.enabled", "false");
        raw.set("s3.retry.max-num-retries", "0");
        if (!checksumValidation) {
            raw.set("s3.checksum-validation.enabled", "false");
        }
        try {
            try (S3NativeClientProvider provider =
                    S3NativeClientProvider.create(
                            S3NativeOptions.from(S3ConfigTranslator.translate(raw)))) {
                UploadPartRequest.Builder request =
                        UploadPartRequest.builder()
                                .bucket("test-bucket")
                                .key("part")
                                .uploadId("test-upload")
                                .partNumber(1)
                                .contentLength((long) payload.length);
                if (explicitCrc32) {
                    request.checksumAlgorithm(ChecksumAlgorithm.CRC32);
                }
                assertThat(
                                provider.asyncClient()
                                        .uploadPart(
                                                request.build(), AsyncRequestBody.fromFile(file))
                                        .get(10, TimeUnit.SECONDS)
                                        .eTag())
                        .isEqualTo("\"test-part\"");
            }
            assertThat(receivedBody.get()).containsExactly(payload);
            SdkHttpRequest captured =
                    SdkHttpRequest.builder()
                            .method(SdkHttpMethod.PUT)
                            .uri(endpoint.resolve(receivedUri.get()))
                            .headers(receivedHeaders.get())
                            .build();
            assertThat(captured.firstMatchingHeader("x-amz-content-sha256"))
                    .hasValue(hex(MessageDigest.getInstance("SHA-256").digest(receivedBody.get())));
            assertThat(captured.firstMatchingHeader("content-encoding")).isEmpty();
            assertThat(captured.firstMatchingHeader("x-amz-trailer")).isEmpty();
            if (checksumValidation || explicitCrc32) {
                CRC32 crc = new CRC32();
                crc.update(receivedBody.get());
                long value = crc.getValue();
                byte[] bytes = {
                    (byte) (value >>> 24), (byte) (value >>> 16),
                    (byte) (value >>> 8), (byte) value
                };
                assertThat(captured.firstMatchingHeader("x-amz-checksum-crc32"))
                        .hasValue(Base64.getEncoder().encodeToString(bytes));
            } else {
                // The SDK maps checksumValidation=false to WHEN_REQUIRED for requests too.
                assertThat(captured.firstMatchingHeader("x-amz-checksum-crc32")).isEmpty();
            }
            verifySignature(captured, receivedBody.get());
        } finally {
            server.stop(0);
        }
    }

    private static void verifySignature(SdkHttpRequest captured, byte[] body) {
        String originalAuthorization = captured.firstMatchingHeader("Authorization").get();
        assertThat(originalAuthorization)
                .contains("AWS4-HMAC-SHA256 Credential=" + ACCESS_KEY + "/")
                .contains("x-amz-content-sha256");
        if (captured.firstMatchingHeader("x-amz-checksum-crc32").isPresent()) {
            assertThat(originalAuthorization).contains("x-amz-checksum-crc32");
        }
        Instant signingTime =
                DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
                        .withZone(ZoneOffset.UTC)
                        .parse(captured.firstMatchingHeader("x-amz-date").get(), Instant::from);
        // Re-sign the bytes actually received using the public synchronous signer. Its
        // checksum calculation does not use the failing asynchronous completion chain.
        // https://github.com/aws/aws-sdk-java-v2/blob/2.55.1/core/http-auth-aws/src/main/java/software/amazon/awssdk/http/auth/aws/signer/AwsV4HttpSigner.java
        SignedRequest verified =
                AwsV4HttpSigner.create()
                        .sign(
                                request ->
                                        request.identity(
                                                        AwsBasicCredentials.create(
                                                                ACCESS_KEY, SECRET_KEY))
                                                .request(
                                                        captured.toBuilder()
                                                                .removeHeader("Authorization")
                                                                .build())
                                                .payload(ContentStreamProvider.fromByteArray(body))
                                                .putProperty(
                                                        AwsV4HttpSigner.SERVICE_SIGNING_NAME, "s3")
                                                .putProperty(
                                                        AwsV4HttpSigner.REGION_NAME, "us-east-1")
                                                .putProperty(
                                                        AwsV4HttpSigner.DOUBLE_URL_ENCODE, false)
                                                .putProperty(AwsV4HttpSigner.NORMALIZE_PATH, false)
                                                .putProperty(
                                                        AwsV4HttpSigner.CHUNK_ENCODING_ENABLED,
                                                        false)
                                                .putProperty(
                                                        AwsV4HttpSigner.PAYLOAD_SIGNING_ENABLED,
                                                        true)
                                                .putProperty(
                                                        AwsV4HttpSigner.SIGNING_CLOCK,
                                                        Clock.fixed(signingTime, ZoneOffset.UTC)));
        assertThat(verified.request().firstMatchingHeader("Authorization"))
                .hasValue(originalAuthorization);
    }

    private static byte[] readFully(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int length;
        while ((length = input.read(buffer)) != -1) {
            output.write(buffer, 0, length);
        }
        return output.toByteArray();
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(Character.forDigit((value >>> 4) & 15, 16));
            result.append(Character.forDigit(value & 15, 16));
        }
        return result.toString();
    }
}
