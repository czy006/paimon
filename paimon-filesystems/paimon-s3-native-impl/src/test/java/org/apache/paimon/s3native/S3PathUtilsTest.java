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

import org.apache.paimon.fs.Path;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link S3PathUtils}. */
class S3PathUtilsTest {

    @Test
    void testBucketAndKey() {
        Path path = new Path("s3://my-bucket/path/to/object");
        assertThat(S3PathUtils.bucket(path)).isEqualTo("my-bucket");
        assertThat(S3PathUtils.key(path)).isEqualTo("path/to/object");
    }

    @Test
    void testBucketRoot() {
        Path path = new Path("s3://my-bucket");
        assertThat(S3PathUtils.bucket(path)).isEqualTo("my-bucket");
        assertThat(S3PathUtils.key(path)).isEmpty();
    }

    @Test
    void testMissingBucketFails() {
        assertThatThrownBy(() -> S3PathUtils.bucket(new Path("/relative/path")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testMarkerKeys() {
        assertThat(S3PathUtils.markerKey("a/b")).isEqualTo("a/b/");
        assertThat(S3PathUtils.markerKey("")).isEqualTo("/");
        assertThat(S3PathUtils.isMarkerKey("a/b/")).isTrue();
        assertThat(S3PathUtils.isMarkerKey("a/b")).isFalse();
        assertThat(S3PathUtils.isMarkerKey("")).isFalse();
    }
}
