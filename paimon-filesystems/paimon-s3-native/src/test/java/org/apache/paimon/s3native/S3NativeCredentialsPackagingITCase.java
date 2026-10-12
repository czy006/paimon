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

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the bundled SDK's STS transport and reflective factories after impl shading. No
 * unrelocated SDK model or HTTP transport is needed on this test's classpath.
 */
class S3NativeCredentialsPackagingITCase {

    private static final String SDK = "org.apache.paimon.s3native.shaded.software.amazon.awssdk.";

    @TempDir Path temporaryDirectory;

    @Test
    @Timeout(15)
    void testBundledWebIdentityCredentialsResolveUsingBundledHttpTransport() throws Exception {
        Path token = temporaryDirectory.resolve("web-identity-token");
        Files.write(token, "synthetic-packaging-token".getBytes(StandardCharsets.UTF_8));
        AtomicInteger requests = new AtomicInteger();
        AtomicReference<String> requestBody = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    try {
                        requests.incrementAndGet();
                        requestBody.set(
                                new String(
                                        readFully(exchange.getRequestBody()),
                                        StandardCharsets.UTF_8));
                        byte[] response =
                                ("<AssumeRoleWithWebIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">"
                                                + "<AssumeRoleWithWebIdentityResult><Credentials>"
                                                + "<AccessKeyId>packaging-access-key</AccessKeyId>"
                                                + "<SecretAccessKey>packaging-secret-key</SecretAccessKey>"
                                                + "<SessionToken>packaging-session-token</SessionToken>"
                                                + "<Expiration>2099-01-01T00:00:00Z</Expiration>"
                                                + "</Credentials></AssumeRoleWithWebIdentityResult>"
                                                + "</AssumeRoleWithWebIdentityResponse>")
                                        .getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().set("Content-Type", "text/xml");
                        exchange.sendResponseHeaders(200, response.length);
                        try (OutputStream output = exchange.getResponseBody()) {
                            output.write(response);
                        }
                    } finally {
                        exchange.close();
                    }
                });
        server.start();
        String previousRegion = System.setProperty("aws.region", "us-east-1");
        // Exact SDK property name (service-specific suffix is CamelCase).
        String previousEndpoint =
                System.setProperty(
                        "aws.endpointUrlSts", "http://127.0.0.1:" + server.getAddress().getPort());
        try (URLClassLoader loader = isolatedSdkClassLoader()) {
            Object factory =
                    Class.forName(
                                    SDK + "auth.credentials.internal.WebIdentityCredentialsUtils",
                                    true,
                                    loader)
                            .getMethod("factory")
                            .invoke(null);
            assertThat(factory.getClass().getName())
                    .isEqualTo(
                            SDK + "services.sts.internal.StsWebIdentityCredentialsProviderFactory");
            assertThat(
                            Class.forName(
                                            SDK
                                                    + "services.sts.internal.StsProfileCredentialsProviderFactory",
                                            true,
                                            loader)
                                    .getConstructor()
                                    .newInstance())
                    .isNotNull();
            Class<?> providerClass =
                    Class.forName(
                            SDK + "auth.credentials.WebIdentityTokenFileCredentialsProvider",
                            true,
                            loader);
            Class<?> builderClass =
                    Class.forName(
                            SDK
                                    + "auth.credentials.WebIdentityTokenFileCredentialsProvider$Builder",
                            true,
                            loader);
            Object builder = providerClass.getMethod("builder").invoke(null);
            builderClass
                    .getMethod("roleArn", String.class)
                    .invoke(builder, "arn:aws:iam::123456789012:role/test-role");
            builderClass
                    .getMethod("roleSessionName", String.class)
                    .invoke(builder, "packaging-test");
            builderClass.getMethod("webIdentityTokenFile", Path.class).invoke(builder, token);
            Object provider = builderClass.getMethod("build").invoke(builder);
            try {
                Object credentials = providerClass.getMethod("resolveCredentials").invoke(provider);
                assertThat(credentials.getClass().getName())
                        .isEqualTo(SDK + "auth.credentials.AwsSessionCredentials");
                assertThat(credentials.getClass().getMethod("sessionToken").invoke(credentials))
                        .isEqualTo("packaging-session-token");
                assertThat(requests.get()).isEqualTo(1);
                assertThat(requestBody.get())
                        .contains("Action=AssumeRoleWithWebIdentity")
                        .contains("WebIdentityToken=synthetic-packaging-token");
            } finally {
                providerClass.getMethod("close").invoke(provider);
            }
        } finally {
            restoreProperty("aws.region", previousRegion);
            restoreProperty("aws.endpointUrlSts", previousEndpoint);
            server.stop(0);
        }
    }

    private static URLClassLoader isolatedSdkClassLoader() throws ClassNotFoundException {
        // Extra HTTP libraries on the Maven test classpath must not hide missing dependencies
        // in the shipped plugin. The platform parent supplies only JDK classes.
        URL bundledSdk =
                Class.forName(
                                SDK + "auth.credentials.internal.WebIdentityCredentialsUtils",
                                false,
                                S3NativeCredentialsPackagingITCase.class.getClassLoader())
                        .getProtectionDomain()
                        .getCodeSource()
                        .getLocation();
        URL loggingApi = LoggerFactory.class.getProtectionDomain().getCodeSource().getLocation();
        return new URLClassLoader(
                new URL[] {bundledSdk, loggingApi}, ClassLoader.getSystemClassLoader().getParent());
    }

    private static byte[] readFully(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = input.read(buffer)) != -1) {
            output.write(buffer, 0, length);
        }
        return output.toByteArray();
    }

    private static void restoreProperty(String key, String previous) {
        if (previous == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previous);
        }
    }
}
