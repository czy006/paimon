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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link S3NativeOptions} parsing and validation. */
class S3NativeOptionsTest {

    private static S3NativeOptions parse(String... pairs) {
        Options options = new Options();
        for (int i = 0; i < pairs.length; i += 2) {
            options.set(pairs[i], pairs[i + 1]);
        }
        return S3NativeOptions.from(S3ConfigTranslator.translate(options));
    }

    @Test
    void testThresholdFactorValidation() {
        assertThat(parse().multipartThresholdFactor).isEqualTo(1.5);
        assertThat(parse("s3.multipart.threshold", "2.0").multipartThresholdFactor).isEqualTo(2.0);
        assertThatThrownBy(() -> parse("s3.multipart.threshold", "0.9"))
                .isInstanceOf(IllegalArgumentException.class);
        // NaN/Infinity slip a plain < 1.0 check; both must be rejected with the key named.
        assertThatThrownBy(() -> parse("s3.multipart.threshold", "NaN"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s3.multipart.threshold");
        assertThatThrownBy(() -> parse("s3.multipart.threshold", "Infinity"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> parse("s3.multipart.threshold", "abc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s3.multipart.threshold");
    }

    @Test
    void testWriteAttributeTypoFailsFast() {
        // The SDK's fromValue maps unknown values to UNKNOWN_TO_SDK_VERSION instead of
        // throwing; options parsing must reject typos before any client is built.
        assertThatThrownBy(() -> parse("s3.write.storage-class", "GLACIERR"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s3.write.storage-class");
        assertThatThrownBy(() -> parse("s3.acl", "public_read"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s3.acl");

        assertThat(parse("s3.write.storage-class", "GLACIER").writeStorageClass.toString())
                .isEqualTo("GLACIER");
        assertThat(parse("s3.acl", "public-read-write").acl.toString())
                .isEqualTo("public-read-write");
        assertThat(parse().writeStorageClass).isNull();
        assertThat(parse().acl).isNull();
    }

    @Test
    void testTagParsing() {
        assertThat(parse().writeTags).isEmpty();
        assertThat(parse("s3.write.tags", "team:data,env:prod").writeTags)
                .containsEntry("team", "data")
                .containsEntry("env", "prod")
                .hasSize(2);
        // Empty values allowed (S3 permits them); trailing comma tolerated; duplicates last-win.
        assertThat(parse("s3.write.tags", "empty:,a:1,").writeTags)
                .containsEntry("empty", "")
                .containsEntry("a", "1");
        assertThat(parse("s3.write.tags", "k:old,k:new").writeTags).containsEntry("k", "new");
        assertThatThrownBy(() -> parse("s3.write.tags", "no-colon"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s3.write.tags");
    }

    @Test
    void testSseValidation() {
        assertThat(parse().sse).isSameAs(S3NativeSse.NONE);
        assertThatThrownBy(() -> parse("s3.sse.type", "bogus"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s3.sse.type");
        assertThatThrownBy(() -> parse("s3.sse.type", "custom", "s3.sse.key", "a2V5"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s3.sse.md5");
    }

    @Test
    void testDeleteOptionsValidation() {
        assertThat(parse().deleteBatchSize).isEqualTo(1000);
        assertThat(parse("s3.delete.batch-size", "7").deleteBatchSize).isEqualTo(7);
        assertThatThrownBy(() -> parse("s3.delete.batch-size", "0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s3.delete.batch-size");
        assertThatThrownBy(() -> parse("s3.delete.batch-size", "1001"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> parse("s3.delete.num-threads", "0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("s3.delete.num-threads");
        assertThat(parse().deleteThreads).isEqualTo(Runtime.getRuntime().availableProcessors());
    }
}
