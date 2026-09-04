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
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for {@link S3NativeFileIO} rename semantics (D2/D7) and stream round-trips
 * (multipart, small file, empty file, lazy seek) against MinIO.
 */
class S3NativeFileIOIntegrationTest {

    public static final S3NativeMinioContainer MINIO_CONTAINER = new S3NativeMinioContainer();

    private static FileIO fs;
    private static Path base;

    @BeforeAll
    static void start() throws IOException {
        MINIO_CONTAINER.start();
        Map<String, String> config = MINIO_CONTAINER.getS3ConfigOptions();
        // Small part size so multi-part uploads are exercised with modest data volumes.
        config.put("s3.upload.min.part.size", "5 mb");
        fs = createFileIO(Options.fromMap(config));
        base = new Path(MINIO_CONTAINER.getS3UriForDefaultBucket() + "/it");
        fs.mkdirs(base);
    }

    private static FileIO createFileIO(Options options) {
        S3NativeFileIO fileIO = new S3NativeFileIO();
        fileIO.configure(CatalogContext.create(options));
        return fileIO;
    }

    private static Path file(String name) {
        return new Path(base, name);
    }

    private static byte[] randomBytes(int size, long seed) {
        byte[] data = new byte[size];
        new Random(seed).nextBytes(data);
        return data;
    }

    private static void writeFile(Path path, byte[] data) throws IOException {
        try (org.apache.paimon.fs.PositionOutputStream out = fs.newOutputStream(path, true)) {
            out.write(data);
        }
    }

    private static byte[] readFile(Path path) throws IOException {
        try (SeekableInputStream in = fs.newInputStream(path)) {
            java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
            byte[] chunk = new byte[64 * 1024];
            int n;
            while ((n = in.read(chunk, 0, chunk.length)) >= 0) {
                buffer.write(chunk, 0, n);
            }
            return buffer.toByteArray();
        }
    }

    // ---------------------------------------------------------------- rename

    @Test
    void testFileRenameMovesContent() throws Exception {
        Path src = file("rename-src");
        Path dst = file("rename-dst");
        byte[] data = randomBytes(1024, 1);
        writeFile(src, data);

        assertThat(fs.rename(src, dst)).isTrue();
        assertThat(fs.exists(src)).isFalse();
        assertThat(readFile(dst)).isEqualTo(data);
        fs.delete(dst, true);
    }

    @Test
    void testRenameMissingSourceReturnsFalse() throws Exception {
        assertThat(fs.rename(file("no-such-src"), file("no-such-dst"))).isFalse();
    }

    @Test
    void testRenameOntoExistingDestinationReturnsFalse() throws Exception {
        Path src = file("rename-ex-src");
        Path dst = file("rename-ex-dst");
        writeFile(src, randomBytes(16, 2));
        writeFile(dst, randomBytes(16, 3));

        assertThat(fs.rename(src, dst)).isFalse();
        // dst content untouched
        assertThat(readFile(dst)).isEqualTo(randomBytes(16, 3));
        fs.delete(src, true);
        fs.delete(dst, true);
    }

    @Test
    void testDirectoryRenameRecursive() throws Exception {
        Path srcDir = new Path(base, "dir-src");
        fs.mkdirs(new Path(srcDir, "sub"));
        byte[] a = randomBytes(128, 4);
        byte[] b = randomBytes(5 * 1024 * 1024 + 11, 5); // > one part, multipart path
        writeFile(new Path(srcDir, "a"), a);
        writeFile(new Path(srcDir, "sub/b"), b);

        Path dstDir = new Path(base, "dir-dst");
        assertThat(fs.rename(srcDir, dstDir)).isTrue();
        assertThat(fs.exists(srcDir)).isFalse();
        assertThat(readFile(new Path(dstDir, "a"))).isEqualTo(a);
        assertThat(readFile(new Path(dstDir, "sub/b"))).isEqualTo(b);

        fs.delete(dstDir, true);
    }

    @Test
    void testRenameDirectoryIntoItselfReturnsFalse() throws Exception {
        Path dir = new Path(base, "self-dir");
        fs.mkdirs(dir);
        writeFile(new Path(dir, "x"), randomBytes(8, 6));

        assertThat(fs.rename(dir, new Path(dir, "child"))).isFalse();
        fs.delete(dir, true);
    }

    @Test
    void testCrossBucketRenameReturnsFalse() throws Exception {
        String otherBucket = createSecondBucket();
        Path src = file("cross-src");
        writeFile(src, randomBytes(8, 7));

        assertThat(fs.rename(src, new Path("s3://" + otherBucket + "/cross-dst"))).isFalse();
        // Source untouched — the copy must not have landed in the source bucket either.
        assertThat(fs.exists(src)).isTrue();
        fs.delete(src, true);
    }

    private String createSecondBucket() {
        Map<String, String> config = MINIO_CONTAINER.getS3ConfigOptions();
        String bucket = "second-bucket-" + System.nanoTime();
        software.amazon.awssdk.services.s3.S3Client client =
                software.amazon.awssdk.services.s3.S3Client.builder()
                        .endpointOverride(URI.create(config.get("s3.endpoint")))
                        .region(software.amazon.awssdk.regions.Region.of(config.get("s3.region")))
                        .forcePathStyle(true)
                        .credentialsProvider(
                                software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
                                        .create(
                                                software.amazon.awssdk.auth.credentials
                                                        .AwsBasicCredentials.create(
                                                        config.get("s3.access.key"),
                                                        config.get("s3.secret.key"))))
                        .build();
        client.createBucket(
                software.amazon.awssdk.services.s3.model.CreateBucketRequest.builder()
                        .bucket(bucket)
                        .build());
        client.close();
        return bucket;
    }

    // ---------------------------------------------------------------- streams

    @Test
    void testMultipartWriteAndReadBack() throws Exception {
        Path path = file("mp");
        // 2 full parts + a tail: crosses part boundaries, exercises concurrent UploadPart.
        byte[] data = randomBytes(5 * 1024 * 1024 * 2 + 12345, 8);
        writeFile(path, data);

        assertThat(fs.getFileStatus(path).getLen()).isEqualTo(data.length);
        assertThat(readFile(path)).isEqualTo(data);
        fs.delete(path, true);
    }

    @Test
    void testSmallAndEmptyFiles() throws Exception {
        Path small = file("small");
        writeFile(small, randomBytes(8, 9));
        assertThat(readFile(small)).hasSize(8);

        Path empty = file("empty");
        writeFile(empty, new byte[0]);
        assertThat(fs.getFileStatus(empty).getLen()).isZero();
        assertThat(readFile(empty)).isEmpty();

        fs.delete(small, true);
        fs.delete(empty, true);
    }

    @Test
    void testLazySeekBackwardAndForward() throws Exception {
        Path path = file("seek");
        byte[] data = randomBytes(1024 * 1024, 10);
        writeFile(path, data);

        try (SeekableInputStream in = fs.newInputStream(path)) {
            // forward small skip stays in-buffer
            in.seek(100);
            assertThat(in.getPos()).isEqualTo(100);
            byte[] buf = new byte[4];
            in.read(buf, 0, 4);
            assertThat(buf).isEqualTo(java.util.Arrays.copyOfRange(data, 100, 104));

            // backward seek reopens with a range request
            in.seek(0);
            in.read(buf, 0, 4);
            assertThat(buf).isEqualTo(java.util.Arrays.copyOfRange(data, 0, 4));

            // seek to exact EOF then read -> -1
            in.seek(data.length);
            assertThat(in.read()).isEqualTo(-1);
        }
        fs.delete(path, true);
    }

    @Test
    void testWriteAfterCloseThrows() throws Exception {
        Path path = file("closed");
        org.apache.paimon.fs.PositionOutputStream out = fs.newOutputStream(path, true);
        out.write(42);
        out.close();
        assertThatThrownBy(() -> out.write(1)).isInstanceOf(IOException.class);
        fs.delete(path, true);
    }

    @Test
    void testOverwriteFalseProtectsExistingEntries() throws Exception {
        Path path = file("protected");
        writeFile(path, randomBytes(8, 11));
        assertThatThrownBy(() -> fs.newOutputStream(path, false)).isInstanceOf(IOException.class);
        // A directory also blocks creation.
        Path dir = new Path(base, "protected-dir");
        fs.mkdirs(dir);
        assertThatThrownBy(() -> fs.newOutputStream(dir, false)).isInstanceOf(IOException.class);
        fs.delete(path, true);
        fs.delete(dir, true);
    }

    // ---------------------------------------------------------------- vectored read

    @Test
    void testPreadDoesNotMoveCursor() throws Exception {
        Path path = file("pread");
        byte[] data = randomBytes(256 * 1024, 14);
        writeFile(path, data);

        try (SeekableInputStream in = fs.newInputStream(path)) {
            in.seek(1000);
            byte[] buf = new byte[64];

            int n = ((org.apache.paimon.fs.VectoredReadable) in).pread(50, buf, 0, buf.length);
            assertThat(n).isEqualTo(64);
            assertThat(java.util.Arrays.copyOfRange(buf, 0, 64))
                    .isEqualTo(java.util.Arrays.copyOfRange(data, 50, 114));
            // Cursor untouched.
            assertThat(in.getPos()).isEqualTo(1000);

            // Sequential read still continues from the cursor.
            in.read(buf, 0, 4);
            assertThat(java.util.Arrays.copyOfRange(buf, 0, 4))
                    .isEqualTo(java.util.Arrays.copyOfRange(data, 1000, 1004));

            // EOF and clamped tail.
            assertThat(((org.apache.paimon.fs.VectoredReadable) in).pread(data.length, buf, 0, 64))
                    .isEqualTo(-1);
            int tail =
                    ((org.apache.paimon.fs.VectoredReadable) in)
                            .pread(data.length - 10, buf, 0, 64);
            assertThat(tail).isEqualTo(10);
        }
        fs.delete(path, true);
    }

    @Test
    void testConcurrentPreads() throws Exception {
        Path path = file("pread-concurrent");
        byte[] data = randomBytes(1024 * 1024, 15);
        writeFile(path, data);

        try (SeekableInputStream in = fs.newInputStream(path)) {
            java.util.List<java.util.concurrent.CompletableFuture<Boolean>> checks =
                    new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                final int slot = i;
                checks.add(
                        java.util.concurrent.CompletableFuture.supplyAsync(
                                () -> {
                                    try {
                                        int offset = slot * 100_000;
                                        byte[] buf = new byte[4096];
                                        int n =
                                                ((org.apache.paimon.fs.VectoredReadable) in)
                                                        .pread(offset, buf, 0, buf.length);
                                        return n == 4096
                                                && java.util.Arrays.equals(
                                                        buf,
                                                        java.util.Arrays.copyOfRange(
                                                                data, offset, offset + 4096));
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                }));
            }
            for (java.util.concurrent.CompletableFuture<Boolean> check : checks) {
                assertThat(check.get()).isTrue();
            }
        }
        fs.delete(path, true);
    }

    @Test
    void testReadVectoredViaDefault() throws Exception {
        Path path = file("vectored");
        byte[] data = randomBytes(1024 * 1024, 16);
        writeFile(path, data);

        try (SeekableInputStream in = fs.newInputStream(path)) {
            org.apache.paimon.fs.FileRange r1 =
                    org.apache.paimon.fs.FileRange.createFileRange(0, 1024);
            org.apache.paimon.fs.FileRange r2 =
                    org.apache.paimon.fs.FileRange.createFileRange(500_000, 2048);
            ((org.apache.paimon.fs.VectoredReadable) in)
                    .readVectored(java.util.Arrays.asList(r1, r2));

            assertThat(r1.getData().get()).isEqualTo(java.util.Arrays.copyOfRange(data, 0, 1024));
            assertThat(r2.getData().get())
                    .isEqualTo(java.util.Arrays.copyOfRange(data, 500_000, 502_048));
        }
        fs.delete(path, true);
    }

    @Test
    void testRecursiveDeleteWithSmallBatchSize() throws Exception {
        Map<String, String> config = new HashMap<>(MINIO_CONTAINER.getS3ConfigOptions());
        config.put("s3.upload.min.part.size", "5 mb");
        // Force many small DeleteObjects batches through the parallel path (I7).
        config.put("s3.delete.batch-size", "3");
        config.put("s3.delete.num-threads", "4");
        FileIO smallBatchIO = createFileIO(Options.fromMap(config));

        Path dir = new Path(base, "small-batch");
        smallBatchIO.mkdirs(dir);
        for (int i = 0; i < 10; i++) {
            writeFile(new Path(dir, "f" + i), randomBytes(8, 20 + i));
        }
        assertThat(smallBatchIO.exists(new Path(dir, "f9"))).isTrue();

        assertThat(smallBatchIO.delete(dir, true)).isTrue();
        assertThat(smallBatchIO.exists(dir)).isFalse();
    }

    @Test
    void testListStatusShapes() throws Exception {
        Path dir = new Path(base, "shapes");
        fs.mkdirs(dir);
        writeFile(new Path(dir, "f1"), randomBytes(8, 12));
        fs.mkdirs(new Path(dir, "d1"));
        writeFile(new Path(dir, "d1/f2"), randomBytes(8, 13));

        FileStatus[] statuses = fs.listStatus(dir);
        assertThat(statuses).hasSize(2);
        long dirs = 0;
        long files = 0;
        for (FileStatus status : statuses) {
            if (status.isDir()) {
                dirs++;
                assertThat(status.getPath().getName()).isEqualTo("d1");
            } else {
                files++;
                assertThat(status.getPath().getName()).isEqualTo("f1");
            }
        }
        assertThat(dirs).isEqualTo(1);
        assertThat(files).isEqualTo(1);

        fs.delete(dir, true);
    }
}
