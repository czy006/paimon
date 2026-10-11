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

package org.apache.paimon.operation;

import org.apache.paimon.fs.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pure URI boundary tests for archive storage roots, without filesystem access. */
class ArchivePathTest {

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(
            strings = {
                "relative/orders",
                "/cold/orders",
                "file:relative",
                "s3:orders",
                "file://localhost/cold/orders",
                "s3:///cold/orders",
                "s3://bucket",
                "s3://bucket/",
                "file:///",
                "s3://user:password@bucket/cold/orders",
                "s3://bucket/cold/orders?secret=value",
                "s3://bucket/cold/orders#fragment",
                "s3://bucket/cold/./orders",
                "s3://bucket/cold/../orders",
                "s3://bucket/cold/%2e/orders",
                "s3://bucket/cold/%2E%2e/orders",
                "s3://bucket/cold%2forders",
                "s3://bucket/cold%5Corders",
                "s3://bucket/cold/%252e%252e/orders",
                "s3://bucket/cold//orders",
                "s3://bucket/cold/%00orders",
                "s3://bucket/cold/orders%zz"
            })
    void rejectsUnsafeRoots(String location) {
        assertThatThrownBy(() -> ArchivePath.parseRoot(location))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void canonicalizesSchemeAndTrailingSlashWithoutLosingEncoding() {
        Path root = ArchivePath.parseRoot("S3://bucket/cold/sales%20orders/");

        assertThat(root.toUri()).isEqualTo(URI.create("s3://bucket/cold/sales%20orders"));
        assertThat(root.toUri().getPath()).isEqualTo("/cold/sales orders");
        assertThat(root.toUri().getRawPath()).isEqualTo("/cold/sales%20orders");
    }

    @Test
    void acceptsLocalAbsoluteAndRemoteRoots() {
        assertThat(ArchivePath.parseRoot("file:///cold/orders").toUri().getScheme())
                .isEqualTo("file");
        assertThat(ArchivePath.parseRoot("hdfs://namenode:8020/cold/orders").toUri().getAuthority())
                .isEqualTo("namenode:8020");
    }

    @Test
    void containsOnlyMatchingSchemeAuthorityAndCompletePathSegments() {
        Path root = ArchivePath.parseRoot("s3://bucket/cold/orders");

        assertThat(ArchivePath.contains(root, new Path("s3://bucket/cold/orders"))).isTrue();
        assertThat(
                        ArchivePath.contains(
                                root, new Path("s3://bucket/cold/orders/dt=1/bucket-0/data")))
                .isTrue();
        assertThat(ArchivePath.contains(root, new Path("s3://bucket/cold/orders2/data"))).isFalse();
        assertThat(ArchivePath.contains(root, new Path("s3://other/cold/orders/data"))).isFalse();
        assertThat(ArchivePath.contains(root, new Path("oss://bucket/cold/orders/data"))).isFalse();
        assertThat(
                        ArchivePath.contains(
                                root,
                                new Path(URI.create("s3://bucket/cold/orders%2fescape/data"))))
                .isFalse();
    }

    @Test
    void detectsEqualAndAncestorRootsInBothDirections() {
        Path parent = ArchivePath.parseRoot("file:///warehouse/orders");
        Path child = ArchivePath.parseRoot("file:///warehouse/orders/cold");
        Path neighbor = ArchivePath.parseRoot("file:///warehouse/orders2");

        assertThat(ArchivePath.overlaps(parent, parent)).isTrue();
        assertThat(ArchivePath.overlaps(parent, child)).isTrue();
        assertThat(ArchivePath.overlaps(child, parent)).isTrue();
        assertThat(ArchivePath.overlaps(parent, neighbor)).isFalse();
        assertThat(
                        ArchivePath.overlaps(
                                parent, ArchivePath.parseRoot("s3://bucket/warehouse/orders")))
                .isFalse();
    }

    @Test
    void rejectsCredentialsWithoutEchoingTheInputOrCause() {
        assertThatThrownBy(() -> ArchivePath.parseRoot("s3://user:do-not-leak@bucket/cold/orders"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageNotContaining("do-not-leak")
                .hasNoCause();
        assertThatThrownBy(() -> ArchivePath.parseRoot("s3://bucket/%zz?do-not-leak"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageNotContaining("do-not-leak")
                .hasNoCause();
    }

    @Test
    void treatsHostCaseAndNumericPortAliasesAsTheSameAuthority() {
        Path parent = ArchivePath.parseRoot("hdfs://NameNode:8020/cold/orders");
        Path same = ArchivePath.parseRoot("hdfs://namenode:08020/cold/orders");
        Path child = ArchivePath.parseRoot("hdfs://NAMENODE:8020/cold/orders/dt=1");

        assertThat(ArchivePath.overlaps(parent, same)).isTrue();
        assertThat(ArchivePath.overlaps(parent, child)).isTrue();
        assertThat(ArchivePath.contains(parent, new Path("hdfs://NAMENODE:08020/cold/orders/data")))
                .isTrue();
        assertThat(ArchivePath.contains(parent, new Path("hdfs://namenode:8021/cold/orders/data")))
                .isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"%FF", "%FE", "%C0%AF", "%E4%BD", "%ED%A0%80"})
    void rejectsMalformedUtf8InsteadOfChangingTheRoot(String escaped) {
        assertThatThrownBy(() -> ArchivePath.parseRoot("s3://bucket/cold/" + escaped))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acceptsWellFormedUtf8AndLiteralReplacementCharacter() {
        assertThat(
                        ArchivePath.parseRoot("s3://bucket/cold/%E5%86%B7%E6%95%B0%E6%8D%AE")
                                .toUri()
                                .getPath())
                .isEqualTo("/cold/冷数据");
        assertThat(ArchivePath.parseRoot("s3://bucket/cold/%EF%BF%BD").toUri().getPath())
                .isEqualTo("/cold/\uFFFD");
        String supplementary = new String(Character.toChars(0x1F4BE));
        assertThat(ArchivePath.parseRoot("s3://bucket/cold/" + supplementary).toUri().getPath())
                .isEqualTo("/cold/" + supplementary);
    }

    @ParameterizedTest
    @ValueSource(ints = {0xD800, 0xDC00})
    void rejectsUnpairedSurrogates(int value) {
        assertThatThrownBy(
                        () ->
                                ArchivePath.parseRoot(
                                        "s3://bucket/cold/" + Character.toString((char) value)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
