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

package org.apache.paimon.spark;

import org.apache.paimon.fs.Path;
import org.apache.paimon.s3.MinioTestContainer;
import org.apache.paimon.testutils.junit.DockerImageVersions;

import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Map;
import java.util.UUID;

/** Real local metadata/hot files plus S3-compatible MinIO cold files, queried together by Spark. */
public class MixedStoragePartitionRelocationS3ITCase extends PartitionRelocationTestBase {

    @RegisterExtension public static final MinioTestContainer MINIO_CONTAINER = minio();

    private static MinioTestContainer minio() {
        MinioTestContainer container = new MinioTestContainer();
        container.setDockerImageName(
                System.getProperty("paimon.test.minio.image", DockerImageVersions.MINIO));
        // A private writable directory also supports images running as a non-root user.
        container.withCommand("server", "/tmp/paimon-minio-data");
        return container;
    }

    @Override
    protected Path coldRoot() {
        return new Path(MINIO_CONTAINER.getS3UriForDefaultBucket() + "/" + UUID.randomUUID());
    }

    @Override
    protected Map<String, String> storageOptions() {
        return MINIO_CONTAINER.getS3ConfigOptions();
    }
}
