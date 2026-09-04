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
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.options.Options;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Manual throughput benchmark for {@link S3NativeFileIO}. Not picked up by surefire (no Test
 * suffix). Build the module, then run {@code org.apache.paimon.s3native.S3NativeBench} on the test
 * classpath with optional arguments {@code [endpoint] [accessKey] [secretKey] [bucket]} (defaults
 * target a local MinIO with minioadmin credentials).
 *
 * <p>Local MinIO numbers are regression indicators only (disk-bound); acceptance numbers per the
 * Spec come from a real S3 endpoint.
 */
public class S3NativeBench {

    private static final int MB = 1024 * 1024;
    private static final int WRITE_FILES = 4;
    private static final int WRITE_SIZE = 64 * MB;
    private static final int SMALL_FILES = 200;
    private static final int SMALL_SIZE = 64 * 1024;

    public static void main(String[] args) throws Exception {
        String endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
        String accessKey = args.length > 1 ? args[1] : "minioadmin";
        String secretKey = args.length > 2 ? args[2] : "minioadmin";
        String bucket = args.length > 3 ? args[3] : "bench-bucket";

        Map<String, String> config = new HashMap<>();
        config.put("s3.endpoint", endpoint);
        config.put("s3.access.key", accessKey);
        config.put("s3.secret.key", secretKey);
        config.put("s3.path.style.access", "true");
        config.put("s3.region", "us-east-1");
        String readBuffer = System.getenv("S3_READ_BUFFER");
        if (readBuffer != null) {
            config.put("s3.read.buffer.size", readBuffer);
        }
        config.put("s3.upload.min.part.size", "8 mb");
        config.put(
                "s3.upload.max.concurrent.uploads",
                String.valueOf(Runtime.getRuntime().availableProcessors()));

        FileIO fs = new S3NativeFileIO();
        fs.configure(CatalogContext.create(Options.fromMap(config)));

        Path base = new Path("s3://" + bucket + "/bench-" + System.currentTimeMillis());
        fs.mkdirs(base);
        byte[] data = new byte[WRITE_SIZE];
        new Random(42).nextBytes(data);
        byte[] small = new byte[SMALL_SIZE];
        new Random(43).nextBytes(small);

        try {
            System.out.printf(
                    "java=%s cores=%d part=8MB%n",
                    System.getProperty("java.version"), Runtime.getRuntime().availableProcessors());

            // Sequential large writes (multipart path).
            long t0 = System.nanoTime();
            for (int i = 0; i < WRITE_FILES; i++) {
                try (org.apache.paimon.fs.PositionOutputStream out =
                        fs.newOutputStream(new Path(base, "big-" + i), true)) {
                    for (int off = 0; off < data.length; off += 4 * MB) {
                        out.write(data, off, Math.min(4 * MB, data.length - off));
                    }
                }
            }
            report(
                    "write large (64MB x " + WRITE_FILES + ", 4MB chunks)",
                    t0,
                    (long) WRITE_SIZE * WRITE_FILES);

            // Sequential small writes (single-put path).
            t0 = System.nanoTime();
            for (int i = 0; i < SMALL_FILES; i++) {
                try (org.apache.paimon.fs.PositionOutputStream out =
                        fs.newOutputStream(new Path(base, "small-" + i), true)) {
                    out.write(small);
                }
            }
            report("write small (64KB x " + SMALL_FILES + ")", t0, (long) SMALL_SIZE * SMALL_FILES);

            // Sequential read of one large object.
            t0 = System.nanoTime();
            long readTotal = 0;
            try (SeekableInputStream in = fs.newInputStream(new Path(base, "big-0"))) {
                byte[] buf = new byte[4 * MB];
                int n;
                while ((n = in.read(buf, 0, buf.length)) >= 0) {
                    readTotal += n;
                }
            }
            report("read sequential (" + (readTotal / MB) + "MB)", t0, readTotal);

            // Vectored read: 8 ranges in parallel.
            VectoredReadable vectored =
                    (VectoredReadable) fs.newInputStream(new Path(base, "big-1"));
            List<org.apache.paimon.fs.FileRange> ranges = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                ranges.add(org.apache.paimon.fs.FileRange.createFileRange(i * 8L * MB, 4 * MB));
            }
            t0 = System.nanoTime();
            vectored.readVectored(ranges);
            long vectoredTotal = 0;
            for (org.apache.paimon.fs.FileRange range : ranges) {
                vectoredTotal += range.getData().get().length;
            }
            report("read vectored (8 x 4MB)", t0, vectoredTotal);
            ((java.io.Closeable) vectored).close();

            // Delete throughput.
            t0 = System.nanoTime();
            fs.delete(base, true);
            report(
                    "delete recursive (" + (WRITE_FILES + SMALL_FILES) + " objects)",
                    t0,
                    WRITE_FILES + SMALL_FILES);
        } finally {
            fs.delete(base, true);
        }
    }

    private static void report(String label, long startNanos, long bytesOrOps) {
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        double mbps = bytesOrOps / (1024.0 * 1024.0) / (elapsedMs / 1000.0);
        System.out.printf("%-45s %6d ms   %10.1f MB/s%n", label, elapsedMs, mbps);
    }
}
