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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests for {@link S3NativeClientProvider} construction and lifecycle. */
class S3NativeClientProviderTest {

    private static S3NativeOptions options(String... keyValue) {
        Options raw = new Options();
        for (int i = 0; i < keyValue.length; i += 2) {
            raw.set(keyValue[i], keyValue[i + 1]);
        }
        return S3NativeOptions.from(S3ConfigTranslator.translate(raw));
    }

    @Test
    void testBuildsWithExplicitRegionAndEndpoint() {
        // Static credentials + explicit region + endpoint override must construct both clients
        // without touching any AWS metadata service.
        S3NativeClientProvider provider =
                S3NativeClientProvider.create(
                        options(
                                "s3.region",
                                "us-west-2",
                                "s3.endpoint",
                                "http://127.0.0.1:1",
                                "s3.access-key",
                                "ak",
                                "s3.secret-key",
                                "sk",
                                "s3.path-style-access",
                                "true"));
        try {
            assertThat(provider.syncClient()).isNotNull();
            assertThat(provider.asyncClient()).isNotNull();
            assertThatCode(provider.syncClient()::listBuckets)
                    .isInstanceOf(RuntimeException.class); // unreachable endpoint, fails fast
        } finally {
            provider.close();
        }
    }

    @Test
    void testBuildsWithRegionFromEnvironment() {
        String old = System.setProperty("aws.region", "us-east-2");
        try {
            S3NativeClientProvider provider =
                    S3NativeClientProvider.create(
                            options(
                                    "s3.endpoint",
                                    "http://127.0.0.1:1",
                                    "s3.access-key",
                                    "ak",
                                    "s3.secret-key",
                                    "sk"));
            try {
                assertThat(provider.syncClient()).isNotNull();
            } finally {
                provider.close();
            }
        } finally {
            if (old == null) {
                System.clearProperty("aws.region");
            } else {
                System.setProperty("aws.region", old);
            }
        }
    }

    @Test
    void testCloseIsIdempotentAndGuardsUseAfterClose() {
        S3NativeClientProvider provider =
                S3NativeClientProvider.create(
                        options(
                                "s3.region",
                                "us-east-1",
                                "s3.endpoint",
                                "http://127.0.0.1:1",
                                "s3.access-key",
                                "ak",
                                "s3.secret-key",
                                "sk"));
        provider.close();
        provider.close(); // second close is a no-op
        assertThatCode(provider::syncClient)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void testInvalidRegionFailsFastWhenUnresolvable() {
        // With an explicit region the provider never consults the environment; an unresolvable
        // region only surfaces on first use in the SDK, so construction must still succeed and
        // the first request must fail rather than hang.
        S3NativeClientProvider provider =
                S3NativeClientProvider.create(
                        options(
                                "s3.region",
                                "not-a-region",
                                "s3.endpoint",
                                "http://127.0.0.1:1",
                                "s3.access-key",
                                "ak",
                                "s3.secret-key",
                                "sk"));
        try {
            assertThatCode(() -> provider.syncClient().listBuckets())
                    .isInstanceOf(RuntimeException.class);
        } finally {
            provider.close();
        }
    }
}
