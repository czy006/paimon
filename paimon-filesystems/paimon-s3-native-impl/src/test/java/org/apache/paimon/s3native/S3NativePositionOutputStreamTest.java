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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link S3NativePositionOutputStream} failure paths and part boundaries, using
 * mocked S3 clients (fault injection without a container).
 */
class S3NativePositionOutputStreamTest {

    @TempDir Path tmpDir;

    private static final long PART_SIZE = 5L * 1024 * 1024;

    /** Options matching the pre-threshold behavior (submit as soon as a part fills). */
    private S3NativeOptions options(String... keyValue) {
        org.apache.paimon.options.Options raw = new org.apache.paimon.options.Options();
        raw.set("s3.upload.tmp.dir", tmpDir.toString());
        raw.set("s3.multipart.threshold", "1");
        for (int i = 0; i < keyValue.length; i += 2) {
            raw.set(keyValue[i], keyValue[i + 1]);
        }
        return S3NativeOptions.from(raw);
    }

    @Test
    void testSmallFileUsesSinglePutObject() throws Exception {
        S3Client sync = mock(S3Client.class);
        S3AsyncClient async = mock(S3AsyncClient.class);
        when(sync.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().eTag("e").build());

        S3NativePositionOutputStream out =
                new S3NativePositionOutputStream(sync, async, "bucket", "key", options());
        out.write(new byte[128]);
        out.close();

        verify(sync).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        verify(sync, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        // Temp files are cleaned up.
        assertThat(tmpDir.toFile().listFiles()).isNullOrEmpty();
    }

    @Test
    void testMultipartSubmitsPartsAndCompletes() throws Exception {
        S3Client sync = mock(S3Client.class);
        S3AsyncClient async = mock(S3AsyncClient.class);
        when(sync.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder().uploadId("u1").build());
        when(async.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                UploadPartResponse.builder().eTag("etag").build()));

        List<software.amazon.awssdk.services.s3.model.CompletedPart> captured = new ArrayList<>();
        when(sync.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenAnswer(
                        invocation -> {
                            captured.addAll(
                                    invocation
                                            .getArgument(0, CompleteMultipartUploadRequest.class)
                                            .multipartUpload()
                                            .parts());
                            return null;
                        });

        S3NativePositionOutputStream out =
                new S3NativePositionOutputStream(sync, async, "bucket", "key", options());
        // Part size is a submit threshold (as in Flink's writer): a single huge write becomes one
        // oversized part, so write in chunks like real writers do — two full parts + a tail.
        int total = (int) PART_SIZE * 2 + 10;
        byte[] chunk = new byte[1024 * 1024];
        int written = 0;
        while (written < total) {
            int n = Math.min(chunk.length, total - written);
            out.write(chunk, 0, n);
            written += n;
        }
        assertThat(out.getPos()).isEqualTo(total);
        out.close();

        // 3 parts: two full + the tail, part numbers ascending.
        assertThat(captured).hasSize(3);
        assertThat(captured.get(0).partNumber()).isEqualTo(1);
        assertThat(captured.get(2).partNumber()).isEqualTo(3);
        verify(sync, never()).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        assertThat(tmpDir.toFile().listFiles()).isNullOrEmpty();
    }

    @Test
    void testSingleLargeWriteSplitsIntoParts() throws Exception {
        // [PORTED-ICE I2] One big write() must not produce one oversized part.
        S3Client sync = mock(S3Client.class);
        S3AsyncClient async = mock(S3AsyncClient.class);
        when(sync.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder().uploadId("u1").build());
        when(async.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                UploadPartResponse.builder().eTag("etag").build()));

        List<software.amazon.awssdk.services.s3.model.CompletedPart> captured = new ArrayList<>();
        when(sync.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenAnswer(
                        invocation -> {
                            captured.addAll(
                                    invocation
                                            .getArgument(0, CompleteMultipartUploadRequest.class)
                                            .multipartUpload()
                                            .parts());
                            return null;
                        });

        java.util.List<Long> partLengths = new ArrayList<>();
        when(async.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
                .thenAnswer(
                        invocation -> {
                            partLengths.add(
                                    invocation
                                            .getArgument(1, AsyncRequestBody.class)
                                            .contentLength()
                                            .get());
                            return CompletableFuture.completedFuture(
                                    UploadPartResponse.builder().eTag("etag").build());
                        });

        S3NativePositionOutputStream out =
                new S3NativePositionOutputStream(sync, async, "bucket", "key", options());
        out.write(new byte[(int) PART_SIZE * 2 + 10]); // single 2.5-part write
        out.close();

        assertThat(captured).hasSize(3);
        assertThat(captured.get(0).partNumber()).isEqualTo(1);
        assertThat(captured.get(2).partNumber()).isEqualTo(3);
        // The actual I2 invariant: parts are exactly part-sized, plus the 10-byte tail.
        assertThat(partLengths).containsExactly(PART_SIZE, PART_SIZE, 10L);
        assertThat(tmpDir.toFile().listFiles()).isNullOrEmpty();
    }

    @Test
    void testExactPartSizeWriteYieldsOnePart() throws Exception {
        S3Client sync = mock(S3Client.class);
        S3AsyncClient async = mock(S3AsyncClient.class);
        when(sync.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder().uploadId("u1").build());
        AtomicInteger parts = new AtomicInteger();
        when(async.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
                .thenAnswer(
                        invocation -> {
                            parts.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    UploadPartResponse.builder().eTag("etag").build());
                        });
        when(sync.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenReturn(null);

        S3NativePositionOutputStream out =
                new S3NativePositionOutputStream(sync, async, "bucket", "key", options());
        out.write(new byte[(int) PART_SIZE]); // exactly one part, no tail
        out.close();

        assertThat(parts.get()).isEqualTo(1);
    }

    @Test
    void testFinalizeAbortsUnclosedStream() throws Exception {
        // [PORTED-ICE I3] GC of an unclosed stream aborts the upload and cleans temp files.
        S3Client sync = mock(S3Client.class);
        S3AsyncClient async = mock(S3AsyncClient.class);
        when(sync.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder().uploadId("u1").build());
        when(async.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                UploadPartResponse.builder().eTag("etag").build()));

        S3NativePositionOutputStream out =
                new S3NativePositionOutputStream(sync, async, "bucket", "key", options());
        out.write(new byte[(int) PART_SIZE]); // starts a multipart upload
        assertThat(tmpDir.toFile().listFiles()).isNotNull().isNotEmpty();

        java.lang.reflect.Method finalize =
                S3NativePositionOutputStream.class.getDeclaredMethod("finalize");
        finalize.setAccessible(true);
        finalize.invoke(out);

        verify(sync).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        verify(sync, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        assertThat(tmpDir.toFile().listFiles()).isNullOrEmpty();
        // Double-close after finalization is a no-op.
        out.close();
    }

    @Test
    void testFailedPartAbortsAndPoisonsStream() throws Exception {
        S3Client sync = mock(S3Client.class);
        S3AsyncClient async = mock(S3AsyncClient.class);
        when(sync.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder().uploadId("u1").build());
        CompletableFuture<UploadPartResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("boom"));
        when(async.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(failed);

        S3NativePositionOutputStream out =
                new S3NativePositionOutputStream(sync, async, "bucket", "key", options());
        out.write(new byte[(int) PART_SIZE]); // triggers submit of part 1
        // The failure surfaces on close (when futures are joined).
        assertThatThrownBy(out::close).isInstanceOf(IOException.class);
        verify(sync).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        // Stream is poisoned: further writes fail fast, second close is a no-op.
        assertThatThrownBy(() -> out.write(1)).isInstanceOf(IOException.class);
        out.close();
        assertThat(tmpDir.toFile().listFiles()).isNullOrEmpty();
    }

    @Test
    void testBelowThresholdUsesSinglePutObject() throws Exception {
        // [PORTED-ICE I5] Default threshold 1.5x: a 6MB write (< 7.5MB) stays on one PutObject
        // even though it crossed the 5MB part size (two staging files are sequenced).
        S3Client sync = mock(S3Client.class);
        S3AsyncClient async = mock(S3AsyncClient.class);
        org.apache.paimon.options.Options raw = new org.apache.paimon.options.Options();
        raw.set("s3.upload.tmp.dir", tmpDir.toString()); // threshold defaults to 1.5
        when(sync.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().eTag("e").build());

        S3NativePositionOutputStream out =
                new S3NativePositionOutputStream(
                        sync, async, "bucket", "key", S3NativeOptions.from(raw));
        out.write(new byte[6 * 1024 * 1024]);
        out.close();

        org.mockito.ArgumentCaptor<RequestBody> body =
                org.mockito.ArgumentCaptor.forClass(RequestBody.class);
        verify(sync).putObject(any(PutObjectRequest.class), body.capture());
        assertThat(body.getValue().contentLength()).isEqualTo(6L * 1024 * 1024);
        verify(sync, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        assertThat(tmpDir.toFile().listFiles()).isNullOrEmpty();
    }

    @Test
    void testChecksumAddsContentMd5ToPutObject() throws Exception {
        // [PORTED-ICE I4] Whole-object MD5 on the single-put path.
        S3Client sync = mock(S3Client.class);
        S3AsyncClient async = mock(S3AsyncClient.class);
        when(sync.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().eTag("e").build());
        byte[] data = new byte[1024];
        new java.util.Random(7).nextBytes(data);
        java.security.MessageDigest md5 = java.security.MessageDigest.getInstance("MD5");
        String expected = Base64.getEncoder().encodeToString(md5.digest(data));

        S3NativePositionOutputStream out =
                new S3NativePositionOutputStream(
                        sync, async, "bucket", "key", options("s3.checksum-enabled", "true"));
        out.write(data);
        out.close();

        org.mockito.ArgumentCaptor<PutObjectRequest> request =
                org.mockito.ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(sync).putObject(request.capture(), any(RequestBody.class));
        assertThat(request.getValue().contentMD5()).isEqualTo(expected);
    }

    @Test
    void testChecksumAddsContentMd5ToParts() throws Exception {
        // [PORTED-ICE I4] Part-level MD5 on the multipart path.
        S3Client sync = mock(S3Client.class);
        S3AsyncClient async = mock(S3AsyncClient.class);
        when(sync.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder().uploadId("u1").build());
        when(async.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                UploadPartResponse.builder().eTag("etag").build()));
        when(sync.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenReturn(null);

        S3NativePositionOutputStream out =
                new S3NativePositionOutputStream(
                        sync, async, "bucket", "key", options("s3.checksum-enabled", "true"));
        out.write(new byte[(int) PART_SIZE + 7]);
        out.close();

        org.mockito.ArgumentCaptor<UploadPartRequest> request =
                org.mockito.ArgumentCaptor.forClass(UploadPartRequest.class);
        verify(async, org.mockito.Mockito.times(2))
                .uploadPart(request.capture(), any(AsyncRequestBody.class));
        for (UploadPartRequest part : request.getAllValues()) {
            assertThat(part.contentMD5()).isNotEmpty();
        }
    }

    @Test
    void testWriteAttributesAppliedToPutObjectPath() throws Exception {
        // [PORTED-ICE I6] Tags/storage-class/ACL must shape the single-put path too, not only
        // CreateMultipartUpload.
        S3Client sync = mock(S3Client.class);
        S3AsyncClient async = mock(S3AsyncClient.class);
        when(sync.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().eTag("e").build());

        S3NativePositionOutputStream out =
                new S3NativePositionOutputStream(
                        sync,
                        async,
                        "bucket",
                        "key",
                        options(
                                "s3.write.tags",
                                "team:data,env:prod",
                                "s3.write.storage-class",
                                "INTELLIGENT_TIERING",
                                "s3.acl",
                                "public-read-write"));
        out.write(new byte[128]);
        out.close();

        org.mockito.ArgumentCaptor<PutObjectRequest> request =
                org.mockito.ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(sync).putObject(request.capture(), any(RequestBody.class));
        assertThat(request.getValue().storageClass().toString()).isEqualTo("INTELLIGENT_TIERING");
        assertThat(request.getValue().acl().toString()).isEqualTo("public-read-write");
        // The SDK serializes tagging as an XML URL-encoded string.
        String tagging = request.getValue().tagging();
        assertThat(tagging).contains("team").contains("data").contains("env").contains("prod");

        verify(sync, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
    }

    @Test
    void testPartMd5ExactValues() throws Exception {
        // [PORTED-ICE I4] Pin the exact per-part digests so a digest-reuse bug across part
        // rolls would fail, not just an empty-value check.
        S3Client sync = mock(S3Client.class);
        S3AsyncClient async = mock(S3AsyncClient.class);
        when(sync.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder().uploadId("u1").build());
        when(async.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                UploadPartResponse.builder().eTag("etag").build()));
        when(sync.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenReturn(null);

        byte[] data = new byte[(int) PART_SIZE + 7];
        new java.util.Random(11).nextBytes(data);
        java.security.MessageDigest md5 = java.security.MessageDigest.getInstance("MD5");
        String firstPartMd5 =
                Base64.getEncoder()
                        .encodeToString(md5.digest(java.util.Arrays.copyOf(data, (int) PART_SIZE)));
        md5.reset();
        String tailMd5 =
                Base64.getEncoder()
                        .encodeToString(
                                md5.digest(
                                        java.util.Arrays.copyOfRange(
                                                data, (int) PART_SIZE, data.length)));

        S3NativePositionOutputStream out =
                new S3NativePositionOutputStream(
                        sync, async, "bucket", "key", options("s3.checksum-enabled", "true"));
        out.write(data);
        out.close();

        java.util.List<UploadPartRequest> requests =
                org.mockito.Mockito.mockingDetails(sync).getInvocations().stream()
                        .filter(i -> i.getMethod().getName().equals("uploadPart"))
                        .map(i -> (UploadPartRequest) i.getArgument(0))
                        .collect(java.util.stream.Collectors.toList());
        // uploadPart goes through the async client; capture there instead.
        org.mockito.ArgumentCaptor<UploadPartRequest> asyncRequest =
                org.mockito.ArgumentCaptor.forClass(UploadPartRequest.class);
        verify(async, org.mockito.Mockito.times(2))
                .uploadPart(asyncRequest.capture(), any(AsyncRequestBody.class));
        assertThat(asyncRequest.getAllValues().get(0).partNumber()).isEqualTo(1);
        assertThat(asyncRequest.getAllValues().get(0).contentMD5()).isEqualTo(firstPartMd5);
        assertThat(asyncRequest.getAllValues().get(1).partNumber()).isEqualTo(2);
        assertThat(asyncRequest.getAllValues().get(1).contentMD5()).isEqualTo(tailMd5);
    }

    @Test
    void testExactThresholdBoundaryWithSubPartWrites() throws Exception {
        // [PORTED-ICE I5] threshold=1.5 x 5MB = 7.5MB. Sub-part increments (1MB chunks) must
        // trip the threshold check at the tail of every write: one byte below the boundary
        // stays on single PutObject, past it starts the MPU.
        S3Client sync = mock(S3Client.class);
        S3AsyncClient async = mock(S3AsyncClient.class);
        when(sync.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().eTag("e").build());
        when(sync.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder().uploadId("u1").build());
        when(async.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                UploadPartResponse.builder().eTag("etag").build()));
        when(sync.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenReturn(null);

        byte[] chunk = new byte[1024 * 1024];
        // Threshold = 5MiB part x 1.5 = 7,864,320 = 7 chunks (7MiB) + 512KiB.
        // Just below: 7MiB + (512KiB - 1).
        S3NativePositionOutputStream below =
                new S3NativePositionOutputStream(
                        sync, async, "bucket", "below", options("s3.multipart.threshold", "1.5"));
        for (int i = 0; i < 7; i++) {
            below.write(chunk);
        }
        below.write(new byte[512 * 1024 - 1]);
        below.close();
        verify(sync).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        verify(sync, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));

        // Just above: 7MiB + (512KiB + 1).
        S3NativePositionOutputStream above =
                new S3NativePositionOutputStream(
                        sync, async, "bucket", "above", options("s3.multipart.threshold", "1.5"));
        for (int i = 0; i < 7; i++) {
            above.write(chunk);
        }
        above.write(new byte[512 * 1024 + 1]);
        above.close();
        verify(sync).createMultipartUpload(any(CreateMultipartUploadRequest.class));
    }

    @Test
    void testWriteTagsAndStorageClassApplied() throws Exception {
        // [PORTED-ICE I6]
        S3Client sync = mock(S3Client.class);
        S3AsyncClient async = mock(S3AsyncClient.class);
        when(sync.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder().uploadId("u1").build());
        when(async.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                UploadPartResponse.builder().eTag("etag").build()));
        when(sync.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenReturn(null);

        S3NativePositionOutputStream out =
                new S3NativePositionOutputStream(
                        sync,
                        async,
                        "bucket",
                        "key",
                        options(
                                "s3.write.tags", "team:paimon,env:prod",
                                "s3.write.storage-class", "INTELLIGENT_TIERING"));
        out.write(new byte[(int) PART_SIZE + 1]);
        out.close();

        org.mockito.ArgumentCaptor<CreateMultipartUploadRequest> request =
                org.mockito.ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);
        verify(sync).createMultipartUpload(request.capture());
        assertThat(request.getValue().storageClass())
                .isEqualTo(
                        software.amazon.awssdk.services.s3.model.StorageClass.INTELLIGENT_TIERING);
        // The SDK's tagging() getter exposes the XML string form of the Tagging we set.
        assertThat(request.getValue().tagging()).contains("team").contains("paimon");
    }

    @Test
    void testSynchronousUploadPartFailureAbortsImmediately() throws Exception {
        S3Client sync = mock(S3Client.class);
        S3AsyncClient async = mock(S3AsyncClient.class);
        when(sync.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(CreateMultipartUploadResponse.builder().uploadId("u1").build());
        when(async.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
                .thenThrow(new RuntimeException("sync failure"));

        S3NativePositionOutputStream out =
                new S3NativePositionOutputStream(sync, async, "bucket", "key", options());
        assertThatThrownBy(() -> out.write(new byte[(int) PART_SIZE]))
                .isInstanceOf(IOException.class);
        verify(sync).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        // Poisoned: subsequent write fails without touching the client again.
        assertThatThrownBy(() -> out.write(1)).isInstanceOf(IOException.class);
        assertThat(tmpDir.toFile().listFiles()).isNullOrEmpty();
    }
}
