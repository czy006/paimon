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

import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;

/**
 * {@link FileStatus} for S3 objects and simulated directories.
 *
 * <p>[PORTED] Derived from Apache Flink flink-filesystems/flink-s3-fs-native (FLINK-38592, Apache
 * License 2.0), class org.apache.flink.fs.s3native.S3FileStatus.
 */
final class S3NativeFileStatus implements FileStatus {

    private final long len;
    private final boolean dir;
    private final Path path;
    private final long modificationTime;

    private S3NativeFileStatus(long len, boolean dir, Path path, long modificationTime) {
        this.len = len;
        this.dir = dir;
        this.path = path;
        this.modificationTime = modificationTime;
    }

    static S3NativeFileStatus file(long len, long modificationTime, Path path) {
        return new S3NativeFileStatus(len, false, path, modificationTime);
    }

    static S3NativeFileStatus directory(Path path) {
        // Directory timestamps are best-effort on object stores.
        return new S3NativeFileStatus(0, true, path, System.currentTimeMillis());
    }

    @Override
    public long getLen() {
        return len;
    }

    @Override
    public boolean isDir() {
        return dir;
    }

    @Override
    public Path getPath() {
        return path;
    }

    @Override
    public long getModificationTime() {
        return modificationTime;
    }
}
