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
import org.apache.paimon.options.Options;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Explicit credentials must be complete before client construction or identity selection. */
class S3NativeCredentialValidationTest {

    private static final String[][] KEY_ALIASES = {
        {"s3.access-key", "s3.secret-key"},
        {"s3.access.key", "s3.secret.key"},
        {"s3a.access-key", "s3a.secret-key"},
        {"s3a.access.key", "s3a.secret.key"},
        {"fs.s3a.access.key", "fs.s3a.secret.key"},
        {"fs.s3a.access-key", "fs.s3a.secret-key"}
    };

    @Test
    void testIncompleteExplicitPairFailsDuringConfigureEvenWithAmbientCredentials() {
        String previousAccess = System.setProperty("aws.accessKeyId", "synthetic-ambient-access");
        String previousSecret =
                System.setProperty("aws.secretAccessKey", "synthetic-ambient-secret");
        try {
            for (String[] aliases : KEY_ALIASES) {
                for (String alias : aliases) {
                    Options raw = new Options();
                    raw.set(alias, "synthetic-explicit-key");
                    assertInvalidConfigure(raw);
                }
            }
        } finally {
            restoreProperty("aws.accessKeyId", previousAccess);
            restoreProperty("aws.secretAccessKey", previousSecret);
        }
    }

    @Test
    void testBlankExplicitKeysFailDuringConfigure() {
        for (String[] aliases : KEY_ALIASES) {
            for (String blank : new String[] {"", " ", "\t"}) {
                for (String[] values :
                        new String[][] {
                            {blank, "synthetic-secret"},
                            {"synthetic-access", blank},
                            {blank, blank}
                        }) {
                    Options raw = new Options();
                    raw.set(aliases[0], values[0]);
                    raw.set(aliases[1], values[1]);
                    assertInvalidConfigure(raw);
                }
                for (String alias : aliases) {
                    Options raw = new Options();
                    raw.set(alias, blank);
                    assertInvalidConfigure(raw);
                }
            }
        }
    }

    @Test
    void testCompletePairIsAcceptedForEveryAlias() {
        for (String[] aliases : KEY_ALIASES) {
            Options raw = new Options();
            raw.set(aliases[0], "synthetic-access");
            raw.set(aliases[1], "synthetic-secret");
            assertThatCode(() -> new S3NativeFileIO().configure(CatalogContext.create(raw)))
                    .doesNotThrowAnyException();
            S3NativeOptions options = S3NativeOptions.from(S3ConfigTranslator.translate(raw));
            assertThat(options.accessKey).isEqualTo("synthetic-access");
            assertThat(options.secretKey).isEqualTo("synthetic-secret");
            assertThat(options.sessionToken).isNull();
        }
    }

    @Test
    void testOmittingBothKeysRetainsDefaultCredentialsChain() throws Exception {
        AtomicReference<String> authorization = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
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
        String previousAccess = System.setProperty("aws.accessKeyId", "synthetic-ambient-access");
        String previousSecret =
                System.setProperty("aws.secretAccessKey", "synthetic-ambient-secret");
        String previousToken = System.getProperty("aws.sessionToken");
        System.clearProperty("aws.sessionToken");
        try {
            Options raw = new Options();
            raw.set("s3.region", "us-east-1");
            raw.set("s3.endpoint", "http://127.0.0.1:" + server.getAddress().getPort());
            raw.set("s3.retry.max-num-retries", "0");
            assertThatCode(() -> new S3NativeFileIO().configure(CatalogContext.create(raw)))
                    .doesNotThrowAnyException();
            S3NativeOptions options = S3NativeOptions.from(S3ConfigTranslator.translate(raw));
            assertThat(options.accessKey).isNull();
            assertThat(options.secretKey).isNull();
            try (S3NativeClientProvider provider = S3NativeClientProvider.create(options)) {
                provider.syncClient().listBuckets();
            }
            assertThat(authorization.get()).contains("Credential=synthetic-ambient-access/");
        } finally {
            restoreProperty("aws.accessKeyId", previousAccess);
            restoreProperty("aws.secretAccessKey", previousSecret);
            restoreProperty("aws.sessionToken", previousToken);
            server.stop(0);
        }
    }

    private static void assertInvalidConfigure(Options raw) {
        assertThatThrownBy(() -> new S3NativeFileIO().configure(CatalogContext.create(raw)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s3.access-key")
                .hasMessageContaining("s3.secret-key");
    }

    private static void restoreProperty(String key, String previousValue) {
        if (previousValue == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previousValue);
        }
    }
}
