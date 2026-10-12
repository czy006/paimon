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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Credential regressions with synthetic credentials and loopback-only service endpoints. */
class S3NativeCredentialsTest {

    @TempDir Path temporaryDirectory;

    @Test
    void testExplicitSessionCredentialsSignRequestsWithSessionToken() throws Exception {
        AtomicReference<String> requestToken = new AtomicReference<>();
        HttpServer server = s3Server(requestToken);
        try {
            try (S3NativeClientProvider provider =
                    S3NativeClientProvider.create(
                            options(
                                    "s3.region", "us-east-1",
                                    "s3.endpoint", endpoint(server),
                                    "s3.access-key", "test-access-key",
                                    "s3.secret-key", "test-secret-key",
                                    "s3.session-token", "test-session-token",
                                    "s3.retry.max-num-retries", "0"))) {
                provider.syncClient().listBuckets();
                assertThat(requestToken.get()).isEqualTo("test-session-token");
            }
        } finally {
            server.stop(0);
        }
    }

    @Test
    void testBasicCredentialsContinueToSignWithoutSessionToken() throws Exception {
        AtomicReference<String> requestToken = new AtomicReference<>();
        HttpServer server = s3Server(requestToken);
        try {
            try (S3NativeClientProvider provider =
                    S3NativeClientProvider.create(
                            options(
                                    "s3.region", "us-east-1",
                                    "s3.endpoint", endpoint(server),
                                    "s3.access-key", "test-access-key",
                                    "s3.secret-key", "test-secret-key",
                                    "s3.retry.max-num-retries", "0"))) {
                provider.syncClient().listBuckets();
                assertThat(requestToken.get()).isNull();
            }
        } finally {
            server.stop(0);
        }
    }

    @Test
    void testSessionTokenRequiresBothExplicitKeys() {
        assertThatThrownBy(() -> options("s3.session-token", "test-token"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s3.session-token");
        assertThatThrownBy(
                        () ->
                                options(
                                        "s3.access-key", "test-access-key",
                                        "s3.session-token", "test-token"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s3.secret-key");
        assertThatThrownBy(
                        () ->
                                options(
                                        "s3.secret-key", "test-secret-key",
                                        "s3.session-token", "test-token"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s3.access-key");
        assertThatThrownBy(
                        () ->
                                options(
                                        "s3.access-key", " ",
                                        "s3.secret-key", "test-secret-key",
                                        "s3.session-token", "test-token"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEmptySessionTokenIsRejected() {
        for (String token : new String[] {"", " "}) {
            assertThatThrownBy(
                            () ->
                                    options(
                                            "s3.access-key", "test-access-key",
                                            "s3.secret-key", "test-secret-key",
                                            "s3.session-token", token))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("s3.session-token");
        }
    }

    @Test
    void testClientCacheDistinguishesTemporarySessions() {
        S3NativeFileIO.ClientKey first = sessionClientKey("first-token");
        S3NativeFileIO.ClientKey second = sessionClientKey("second-token");
        assertThat(first).isNotEqualTo(second);
        assertThat(first).isEqualTo(sessionClientKey("first-token"));
        assertThat(first.hashCode()).isEqualTo(sessionClientKey("first-token").hashCode());
        assertThat(first)
                .isNotEqualTo(
                        S3NativeFileIO.ClientKey.from(
                                options(
                                        "s3.access-key", "test-access-key",
                                        "s3.secret-key", "test-secret-key")));
    }

    @Test
    void testSessionTokenAliases() {
        for (String alias :
                new String[] {
                    "s3.session-token",
                    "s3.session.token",
                    "s3a.session-token",
                    "s3a.session.token",
                    "fs.s3a.session.token",
                    "fs.s3a.session-token"
                }) {
            Options raw = new Options();
            raw.set(alias, "test-token");
            assertThat(S3ConfigTranslator.translate(raw).get("s3.session-token"))
                    .as("session token alias %s", alias)
                    .isEqualTo("test-token");
        }
    }

    @Test
    void testCanonicalSessionTokenWinsOverFsS3aAlias() {
        Options raw = new Options();
        raw.set("fs.s3a.session.token", "lower-priority-token");
        raw.set("s3.session-token", "canonical-token");
        assertThat(S3ConfigTranslator.translate(raw).get("s3.session-token"))
                .isEqualTo("canonical-token");
    }

    @Test
    void testWebIdentityCredentialsResolveThroughBundledSts() throws Exception {
        Path tokenFile = temporaryDirectory.resolve("web-identity-token");
        Files.write(tokenFile, "synthetic-web-identity-token".getBytes(StandardCharsets.UTF_8));
        AtomicReference<String> request = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    request.set(
                            new String(
                                    readRequestBody(exchange.getRequestBody()),
                                    StandardCharsets.UTF_8));
                    byte[] response =
                            ("<AssumeRoleWithWebIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">"
                                            + "<AssumeRoleWithWebIdentityResult><Credentials>"
                                            + "<AccessKeyId>web-access-key</AccessKeyId>"
                                            + "<SecretAccessKey>web-secret-key</SecretAccessKey>"
                                            + "<SessionToken>web-session-token</SessionToken>"
                                            + "<Expiration>2099-01-01T00:00:00Z</Expiration>"
                                            + "</Credentials></AssumeRoleWithWebIdentityResult>"
                                            + "<ResponseMetadata><RequestId>test-request</RequestId>"
                                            + "</ResponseMetadata></AssumeRoleWithWebIdentityResponse>")
                                    .getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "text/xml");
                    exchange.sendResponseHeaders(200, response.length);
                    try (java.io.OutputStream body = exchange.getResponseBody()) {
                        body.write(response);
                    }
                });
        server.start();
        String previousRegion = System.setProperty("aws.region", "us-east-1");
        String previousEndpoint = System.setProperty("aws.endpointUrlSts", endpoint(server));
        try {
            try (WebIdentityTokenFileCredentialsProvider provider =
                    WebIdentityTokenFileCredentialsProvider.builder()
                            .roleArn("arn:aws:iam::123456789012:role/test-role")
                            .roleSessionName("test-session")
                            .webIdentityTokenFile(tokenFile)
                            .build()) {
                assertThat(provider.resolveCredentials())
                        .isInstanceOfSatisfying(
                                AwsSessionCredentials.class,
                                credentials -> {
                                    assertThat(credentials.accessKeyId())
                                            .isEqualTo("web-access-key");
                                    assertThat(credentials.secretAccessKey())
                                            .isEqualTo("web-secret-key");
                                    assertThat(credentials.sessionToken())
                                            .isEqualTo("web-session-token");
                                });
                assertThat(request.get())
                        .contains("Action=AssumeRoleWithWebIdentity")
                        .contains("WebIdentityToken=synthetic-web-identity-token");
            }
        } finally {
            restoreProperty("aws.region", previousRegion);
            restoreProperty("aws.endpointUrlSts", previousEndpoint);
            server.stop(0);
        }
    }

    private static byte[] readRequestBody(java.io.InputStream input) throws IOException {
        java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int read;
        while ((read = input.read(buffer)) != -1) {
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }

    private static HttpServer s3Server(AtomicReference<String> requestToken) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    requestToken.set(exchange.getRequestHeaders().getFirst("X-Amz-Security-Token"));
                    byte[] response =
                            ("<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                                            + "<Buckets/></ListAllMyBucketsResult>")
                                    .getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(200, response.length);
                    try (java.io.OutputStream body = exchange.getResponseBody()) {
                        body.write(response);
                    }
                });
        server.start();
        return server;
    }

    private static String endpoint(HttpServer server) {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private static S3NativeFileIO.ClientKey sessionClientKey(String sessionToken) {
        return S3NativeFileIO.ClientKey.from(
                options(
                        "s3.access-key", "test-access-key",
                        "s3.secret-key", "test-secret-key",
                        "s3.session-token", sessionToken));
    }

    private static S3NativeOptions options(String... pairs) {
        Options raw = new Options();
        for (int i = 0; i < pairs.length; i += 2) {
            raw.set(pairs[i], pairs[i + 1]);
        }
        return S3NativeOptions.from(S3ConfigTranslator.translate(raw));
    }

    private static void restoreProperty(String key, String previousValue) {
        if (previousValue == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previousValue);
        }
    }
}
