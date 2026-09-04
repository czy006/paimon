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

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link S3ConfigTranslator} and {@link S3NativeOptions} parsing. */
class S3ConfigTranslatorTest {

    @Test
    void testGenericPrefixesAndPriority() {
        Options raw = new Options();
        raw.set("s3.endpoint", "http://a");
        raw.set("s3a.endpoint", "http://b");
        raw.set("s3a.path-style-access", "true");

        Options normalized = S3ConfigTranslator.translate(raw);
        assertThat(normalized.get("s3.endpoint")).isEqualTo("http://a"); // s3.* wins over s3a.*
        assertThat(normalized.getBoolean("s3.path-style-access", false)).isTrue();
    }

    @Test
    void testDottedKeyAliases() {
        // Regression: s3.access.key / s3.secret.key / s3.path.style.access must map to the
        // canonical dashed keys — otherwise credentials silently fall through.
        Options raw = new Options();
        raw.set("s3.access.key", "ak");
        raw.set("s3.secret.key", "sk");
        raw.set("s3.path.style.access", "true");

        Options normalized = S3ConfigTranslator.translate(raw);
        assertThat(normalized.get("s3.access-key")).isEqualTo("ak");
        assertThat(normalized.get("s3.secret-key")).isEqualTo("sk");
        assertThat(normalized.getBoolean("s3.path-style-access", false)).isTrue();

        // s3a. prefix variant, and dashed s3.* still wins over dotted forms.
        Options raw2 = new Options();
        raw2.set("s3a.access.key", "ak2");
        raw2.set("s3.access-key", "winner");

        Options normalized2 = S3ConfigTranslator.translate(raw2);
        assertThat(normalized2.get("s3.access-key")).isEqualTo("winner");
        assertThat(normalized2.get("s3.access.key")).isNull();
    }

    @Test
    void testFsS3aAliases() {
        Options raw = new Options();
        raw.set("fs.s3a.access.key", "ak");
        raw.set("fs.s3a.secret-key", "sk");
        raw.set("fs.s3a.endpoint", "http://minio:9000");
        raw.set("fs.s3a.path.style.access", "true");
        raw.set("fs.s3a.multipart.size", "16 mb");
        raw.set("fs.s3a.attempts.maximum", "10");
        raw.set("fs.s3a.connection.maximum", "100");

        Options normalized = S3ConfigTranslator.translate(raw);
        assertThat(normalized.get("s3.access-key")).isEqualTo("ak");
        assertThat(normalized.get("s3.secret-key")).isEqualTo("sk");
        assertThat(normalized.get("s3.endpoint")).isEqualTo("http://minio:9000");
        assertThat(normalized.getBoolean("s3.path-style-access", false)).isTrue();
        assertThat(normalized.get("s3.upload.min.part.size")).isEqualTo("16 mb");
        // S3A attempts (10) become retries (9)
        assertThat(normalized.get("s3.retry.max-num-retries")).isEqualTo("9");
        assertThat(normalized.get("s3.connection.max")).isEqualTo("100");
    }

    @Test
    void testS3KeyBeatsFsS3aAlias() {
        Options raw = new Options();
        raw.set("s3.endpoint", "http://keep");
        raw.set("fs.s3a.endpoint", "http://drop");

        assertThat(S3ConfigTranslator.translate(raw).get("s3.endpoint")).isEqualTo("http://keep");
    }

    @Test
    void testUnknownFsS3aKeysDropped() {
        Options raw = new Options();
        raw.set("fs.s3a.some.unknown.option", "value");

        Options normalized = S3ConfigTranslator.translate(raw);
        assertThat(normalized.toMap()).isEmpty();
    }

    @Test
    void testOptionsDefaultsAndClamps() {
        S3NativeOptions options = S3NativeOptions.from(S3ConfigTranslator.translate(new Options()));

        assertThat(options.partSizeBytes).isEqualTo(5L << 20);
        assertThat(options.readBufferSize).isEqualTo(256 * 1024);
        assertThat(options.maxConnections).isEqualTo(50);
        assertThat(options.maxRetries).isEqualTo(3);
        assertThat(options.retryBaseDelay).isEqualTo(Duration.ofMillis(100));
        assertThat(options.retryMaxBackoff).isEqualTo(Duration.ofSeconds(20));
        assertThat(options.chunkedEncodingEnabled).isTrue();
        assertThat(options.checksumValidationEnabled).isTrue();
        assertThat(options.tmpDir).isEqualTo(System.getProperty("java.io.tmpdir"));
    }

    @Test
    void testOptionsValidation() {
        Options raw = new Options();
        raw.set("s3.upload.min.part.size", "1 mb");
        assertThatThrownBy(() -> S3NativeOptions.from(S3ConfigTranslator.translate(raw)))
                .isInstanceOf(IllegalArgumentException.class);

        Options raw2 = new Options();
        raw2.set("s3.connection.max", "0");
        assertThatThrownBy(() -> S3NativeOptions.from(S3ConfigTranslator.translate(raw2)))
                .isInstanceOf(IllegalArgumentException.class);

        Options raw3 = new Options();
        raw3.set("s3.retry.max-backoff", "50ms");
        raw3.set("s3.retry.throttle.base-delay", "1s");
        assertThatThrownBy(() -> S3NativeOptions.from(S3ConfigTranslator.translate(raw3)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testReadBufferSizeClamped() {
        Options raw = new Options();
        raw.set("s3.read.buffer.size", "100 kb");

        S3NativeOptions options = S3NativeOptions.from(S3ConfigTranslator.translate(raw));
        assertThat(options.readBufferSize).isEqualTo(256 * 1024);
    }

    @Test
    void testNegativeRetriesFailsFast() {
        Options raw = new Options();
        raw.set("s3.retry.max-num-retries", "-1");

        assertThatThrownBy(() -> S3NativeOptions.from(S3ConfigTranslator.translate(raw)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDurationFormats() {
        Options raw = new Options();
        raw.set("s3.connection.timeout", "30s");
        raw.set("s3.socket.timeout", "PT2M");
        raw.set("s3.connection.max-idle-time", "5000");

        S3NativeOptions options = S3NativeOptions.from(S3ConfigTranslator.translate(raw));
        assertThat(options.connectionTimeout).isEqualTo(Duration.ofSeconds(30));
        assertThat(options.socketTimeout).isEqualTo(Duration.ofMinutes(2));
        assertThat(options.connectionMaxIdleTime).isEqualTo(Duration.ofMillis(5000));
    }

    @Test
    void testSystemPropertyFallbacks() {
        String oldEndpoint = System.setProperty("s3.endpoint", "http://sysprop");
        String oldPathStyle = System.setProperty("s3.path.style.access", "true");
        try {
            S3NativeOptions options =
                    S3NativeOptions.from(S3ConfigTranslator.translate(new Options()));
            assertThat(options.endpoint).isEqualTo("http://sysprop");
            assertThat(options.pathStyleAccess).isTrue();
        } finally {
            if (oldEndpoint == null) {
                System.clearProperty("s3.endpoint");
            } else {
                System.setProperty("s3.endpoint", oldEndpoint);
            }
            if (oldPathStyle == null) {
                System.clearProperty("s3.path.style.access");
            } else {
                System.setProperty("s3.path.style.access", oldPathStyle);
            }
        }
    }
}
