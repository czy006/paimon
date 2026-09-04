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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOBehaviorTestBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.BeforeAll;

import java.util.Map;

/** Behavior suite for {@link S3NativeFileIO} against MinIO. */
class S3NativeFileIOBehaviorTest extends FileIOBehaviorTestBase {

    public static final S3NativeMinioContainer MINIO_CONTAINER = new S3NativeMinioContainer();

    @BeforeAll
    static void startContainer() {
        MINIO_CONTAINER.start();
    }

    @Override
    protected FileIO getFileSystem() {
        Map<String, String> config = MINIO_CONTAINER.getS3ConfigOptions();
        S3NativeFileIO fileIO = new S3NativeFileIO();
        fileIO.configure(CatalogContext.create(Options.fromMap(config)));
        return fileIO;
    }

    @Override
    protected Path getBasePath() {
        // Keep the trailing slash: a bucket-root URI without a path component breaks
        // Path(Path, String) resolution (the child would glue onto the authority).
        return new Path(MINIO_CONTAINER.getS3UriForDefaultBucket() + "/");
    }
}
