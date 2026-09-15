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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for the parallel batch deletion in {@link S3NativeObjectOperations}. */
class S3NativeObjectOperationsTest {

    private static S3NativeObjectOperations ops(S3Client client) {
        return new S3NativeObjectOperations(client, "bucket");
    }

    private static List<String> keys(int n) {
        List<String> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add("k-" + i);
        }
        return keys;
    }

    @Test
    void testBatchSizeSplitsRequests() throws IOException {
        S3Client client = mock(S3Client.class);
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenReturn(DeleteObjectsResponse.builder().build());

        ops(client).deleteBatch(keys(5), 2, 4);

        // 5 keys with batch size 2 -> 3 DeleteObjects calls, each within the size limit.
        verify(client, times(3)).deleteObjects(any(DeleteObjectsRequest.class));
    }

    @Test
    void testParallelBatchesAllExecuted() throws IOException {
        S3Client client = mock(S3Client.class);
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenReturn(DeleteObjectsResponse.builder().build());

        List<String> keys = keys(20);
        ops(client).deleteBatch(keys, 3, 8);

        verify(client, times(7)).deleteObjects(any(DeleteObjectsRequest.class));
    }

    @Test
    void testNotFoundPerKeyErrorsAreSuccess() throws IOException {
        S3Client client = mock(S3Client.class);
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenReturn(
                        DeleteObjectsResponse.builder()
                                .errors(
                                        S3Error.builder().key("k-0").code("NoSuchKey").build(),
                                        S3Error.builder().key("k-1").code("NotFound").build())
                                .build());

        ops(client).deleteBatch(keys(2), 1000, 2);

        // No exception: per-key NotFound means the object is already gone (Spec D6).
        verify(client, times(1)).deleteObjects(any(DeleteObjectsRequest.class));
    }

    @Test
    void testFailuresAggregatedAcrossBatches() {
        S3Client client = mock(S3Client.class);
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenAnswer(
                        invocation -> {
                            DeleteObjectsRequest request = invocation.getArgument(0);
                            String firstKey = request.delete().objects().get(0).key();
                            if (firstKey.endsWith("0") || firstKey.endsWith("3")) {
                                return DeleteObjectsResponse.builder()
                                        .errors(
                                                S3Error.builder()
                                                        .key(firstKey)
                                                        .code("AccessDenied")
                                                        .message("denied")
                                                        .build())
                                        .build();
                            }
                            return DeleteObjectsResponse.builder().build();
                        });

        List<String> keys = keys(5);
        // batch size 1 -> one request per key; keys k-0 and k-3 fail.
        assertThatThrownBy(() -> ops(client).deleteBatch(keys, 1, 4))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("2 keys")
                .hasMessageContaining("k-0");
        verify(client, times(5)).deleteObjects(any(DeleteObjectsRequest.class));
    }

    @Test
    void testWholeRequestFailureFailsAllItsKeys() {
        S3Exception boom =
                (S3Exception) S3Exception.builder().message("boom").statusCode(500).build();
        S3Client client = mock(S3Client.class);
        when(client.deleteObjects(any(DeleteObjectsRequest.class))).thenThrow(boom);

        assertThatThrownBy(() -> ops(client).deleteBatch(keys(3), 2, 4))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("3 keys");
    }

    @Test
    void testNetworkFailureAggregatedWithCause() {
        // SdkClientException does not extend S3Exception; it must still be aggregated (all
        // keys failed) and preserved as the cause of the reported IOException.
        software.amazon.awssdk.core.exception.SdkClientException networkFailure =
                software.amazon.awssdk.core.exception.SdkClientException.create(
                        "Connection refused");
        S3Client client = mock(S3Client.class);
        when(client.deleteObjects(any(DeleteObjectsRequest.class))).thenThrow(networkFailure);

        assertThatThrownBy(() -> ops(client).deleteBatch(keys(3), 1000, 2))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("3 keys")
                .hasRootCauseInstanceOf(
                        software.amazon.awssdk.core.exception.SdkClientException.class);
    }

    @Test
    void testDuplicateKeysDeduplicated() throws IOException {
        S3Client client = mock(S3Client.class);
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenReturn(DeleteObjectsResponse.builder().build());

        List<String> withDuplicates = new ArrayList<>(keys(3));
        withDuplicates.add("k-1");
        withDuplicates.add("k-2");
        ops(client).deleteBatch(withDuplicates, 1000, 2);

        org.mockito.ArgumentCaptor<DeleteObjectsRequest> request =
                org.mockito.ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(client).deleteObjects(request.capture());
        assertThat(request.getValue().delete().objects()).hasSize(3);
    }

    @Test
    void testDeletePrefixStreamingPaginatesAndBatches() throws IOException {
        S3Client client = mock(S3Client.class);
        // 3 pages x 3 unique keys = 9 keys, batch size 4 -> 3 DeleteObjects batches (4+4+1).
        java.util.concurrent.atomic.AtomicInteger pageCalls =
                new java.util.concurrent.atomic.AtomicInteger();
        java.util.List<String> tokens = java.util.Arrays.asList("t1", "t2", null);
        when(client.listObjectsV2(
                        any(software.amazon.awssdk.services.s3.model.ListObjectsV2Request.class)))
                .thenAnswer(
                        invocation -> {
                            int call = pageCalls.getAndIncrement();
                            software.amazon.awssdk.services.s3.model.ListObjectsV2Request request =
                                    invocation.getArgument(0);
                            // Continuation token chained from page 2 onwards.
                            if (call > 0) {
                                org.assertj.core.api.Assertions.assertThat(
                                                request.continuationToken())
                                        .isEqualTo("t" + call);
                            }
                            // A real listing returns each key once; page-index the keys.
                            java.util.List<software.amazon.awssdk.services.s3.model.S3Object>
                                    uniquePage =
                                            java.util.Arrays.asList(
                                                    software.amazon.awssdk.services.s3.model
                                                            .S3Object.builder()
                                                            .key("k-" + call + "-0")
                                                            .build(),
                                                    software.amazon.awssdk.services.s3.model
                                                            .S3Object.builder()
                                                            .key("k-" + call + "-1")
                                                            .build(),
                                                    software.amazon.awssdk.services.s3.model
                                                            .S3Object.builder()
                                                            .key("k-" + call + "-2")
                                                            .build());
                            return software.amazon.awssdk.services.s3.model.ListObjectsV2Response
                                    .builder()
                                    .contents(uniquePage)
                                    .nextContinuationToken(tokens.get(call))
                                    .build();
                        });
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenReturn(DeleteObjectsResponse.builder().build());

        ops(client).deletePrefixStreaming("p/", 4, 4);

        assertThat(pageCalls.get()).isEqualTo(3);
        java.util.List<DeleteObjectsRequest> deletes =
                org.mockito.Mockito.mockingDetails(client).getInvocations().stream()
                        .filter(i -> i.getMethod().getName().equals("deleteObjects"))
                        .map(i -> (DeleteObjectsRequest) i.getArgument(0))
                        .collect(java.util.stream.Collectors.toList());
        assertThat(deletes).hasSize(3);
        // Keys must be partitioned without overlap and cover all 9.
        java.util.Set<String> allKeys = new java.util.HashSet<>();
        for (DeleteObjectsRequest request : deletes) {
            for (ObjectIdentifier id : request.delete().objects()) {
                allKeys.add(id.key());
            }
        }
        assertThat(allKeys).hasSize(9);
    }

    @Test
    void testDeletePrefixStreamingEmptyPrefixIsNoop() throws IOException {
        S3Client client = mock(S3Client.class);
        when(client.listObjectsV2(
                        any(software.amazon.awssdk.services.s3.model.ListObjectsV2Request.class)))
                .thenReturn(
                        software.amazon.awssdk.services.s3.model.ListObjectsV2Response.builder()
                                .build());

        ops(client).deletePrefixStreaming("empty/", 1000, 4);
        verify(client, times(1))
                .listObjectsV2(
                        any(software.amazon.awssdk.services.s3.model.ListObjectsV2Request.class));
        verify(client, times(0)).deleteObjects(any(DeleteObjectsRequest.class));
    }

    @Test
    void testDeletePrefixStreamingFailuresAggregate() {
        S3Client client = mock(S3Client.class);
        when(client.listObjectsV2(
                        any(software.amazon.awssdk.services.s3.model.ListObjectsV2Request.class)))
                .thenReturn(
                        software.amazon.awssdk.services.s3.model.ListObjectsV2Response.builder()
                                .contents(
                                        software.amazon.awssdk.services.s3.model.S3Object.builder()
                                                .key("a")
                                                .build(),
                                        software.amazon.awssdk.services.s3.model.S3Object.builder()
                                                .key("b")
                                                .build())
                                .build());
        S3Exception boom =
                (S3Exception) S3Exception.builder().message("boom").statusCode(500).build();
        when(client.deleteObjects(any(DeleteObjectsRequest.class))).thenThrow(boom);

        // Both keys land in one batch whose whole request fails -> 2 keys failed, cause kept.
        assertThatThrownBy(() -> ops(client).deletePrefixStreaming("p/", 10, 2))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("2 keys")
                .hasRootCauseInstanceOf(S3Exception.class);
    }

    @Test
    void testDeletePrefixStreamingListFailureFailsLoud() {
        S3Client client = mock(S3Client.class);
        when(client.listObjectsV2(
                        any(software.amazon.awssdk.services.s3.model.ListObjectsV2Request.class)))
                .thenThrow(
                        (S3Exception)
                                S3Exception.builder().message("list boom").statusCode(500).build());

        assertThatThrownBy(() -> ops(client).deletePrefixStreaming("p/", 10, 2))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("deletePrefixStreaming");
    }

    @Test
    void testProbeDirectoryRequestShapeAndMembership() throws IOException {
        S3Client client = mock(S3Client.class);
        java.util.List<software.amazon.awssdk.services.s3.model.ListObjectsV2Request> requests =
                java.util.Collections.synchronizedList(new java.util.ArrayList<>());

        // Marker listed first (sorted order) -> MARKER_DIR.
        when(client.listObjectsV2(
                        any(software.amazon.awssdk.services.s3.model.ListObjectsV2Request.class)))
                .thenAnswer(
                        invocation -> {
                            requests.add(invocation.getArgument(0));
                            return software.amazon.awssdk.services.s3.model.ListObjectsV2Response
                                    .builder()
                                    .contents(
                                            software.amazon.awssdk.services.s3.model.S3Object
                                                    .builder()
                                                    .key("a/b/")
                                                    .build(),
                                            software.amazon.awssdk.services.s3.model.S3Object
                                                    .builder()
                                                    .key("a/b/c")
                                                    .build())
                                    .build();
                        });
        assertThat(ops(client).probeDirectory("a/b"))
                .isEqualTo(S3NativeObjectOperations.DirectoryProbe.MARKER_DIR);

        // Other keys only -> PREFIX_DIR.
        when(client.listObjectsV2(
                        any(software.amazon.awssdk.services.s3.model.ListObjectsV2Request.class)))
                .thenReturn(
                        software.amazon.awssdk.services.s3.model.ListObjectsV2Response.builder()
                                .contents(
                                        software.amazon.awssdk.services.s3.model.S3Object.builder()
                                                .key("a/b/c")
                                                .build())
                                .build());
        assertThat(ops(client).probeDirectory("a/b"))
                .isEqualTo(S3NativeObjectOperations.DirectoryProbe.PREFIX_DIR);

        // Membership check must not depend on order: marker appearing second still wins.
        when(client.listObjectsV2(
                        any(software.amazon.awssdk.services.s3.model.ListObjectsV2Request.class)))
                .thenReturn(
                        software.amazon.awssdk.services.s3.model.ListObjectsV2Response.builder()
                                .contents(
                                        software.amazon.awssdk.services.s3.model.S3Object.builder()
                                                .key("a/b/ z") // simulates an unsorted response
                                                // (directory buckets); membership
                                                // must not rely on order
                                                .build(),
                                        software.amazon.awssdk.services.s3.model.S3Object.builder()
                                                .key("a/b/")
                                                .build())
                                .build());
        assertThat(ops(client).probeDirectory("a/b"))
                .isEqualTo(S3NativeObjectOperations.DirectoryProbe.MARKER_DIR);

        // Empty -> MISSING.
        when(client.listObjectsV2(
                        any(software.amazon.awssdk.services.s3.model.ListObjectsV2Request.class)))
                .thenReturn(
                        software.amazon.awssdk.services.s3.model.ListObjectsV2Response.builder()
                                .build());
        assertThat(ops(client).probeDirectory("a/b"))
                .isEqualTo(S3NativeObjectOperations.DirectoryProbe.MISSING);

        // Request shape: prefix "a/b/", maxKeys=2, and NO delimiter (the delimiter form omits
        // the self-marker per AWS docs Example 8).
        assertThat(requests).isNotEmpty();
        software.amazon.awssdk.services.s3.model.ListObjectsV2Request request = requests.get(0);
        assertThat(request.prefix()).isEqualTo("a/b/");
        assertThat(request.maxKeys()).isEqualTo(2);
        assertThat(request.delimiter()).isNull();
    }

    @Test
    void testSseCustomerHeadersOnHeadObject() {
        S3Client client = mock(S3Client.class);
        when(client.headObject(
                        any(software.amazon.awssdk.services.s3.model.HeadObjectRequest.class)))
                .thenThrow(
                        (software.amazon.awssdk.services.s3.model.NoSuchKeyException)
                                software.amazon.awssdk.services.s3.model.NoSuchKeyException
                                        .builder()
                                        .build());

        org.apache.paimon.options.Options options = new org.apache.paimon.options.Options();
        options.set("s3.sse.type", "custom");
        options.set("s3.sse.key", "c2VjcmV0LWtleQ==");
        options.set("s3.sse.md5", "tT1l8pJFI9r1hE0A5fFQjg==");
        S3NativeObjectOperations sseOps =
                new S3NativeObjectOperations(client, "bucket", S3NativeSse.from(options));
        org.assertj.core.api.Assertions.assertThatCode(() -> sseOps.headObjectOrNull("k"))
                .doesNotThrowAnyException();

        org.mockito.ArgumentCaptor<software.amazon.awssdk.services.s3.model.HeadObjectRequest>
                request =
                        org.mockito.ArgumentCaptor.forClass(
                                software.amazon.awssdk.services.s3.model.HeadObjectRequest.class);
        verify(client).headObject(request.capture());
        assertThat(request.getValue().sseCustomerAlgorithm()).isEqualTo("AES256");
        assertThat(request.getValue().sseCustomerKey()).isEqualTo("c2VjcmV0LWtleQ==");
    }

    @Test
    void testListingPaginationAcrossPages() throws IOException {
        // Three pages of 2 keys each via continuation tokens; all 6 keys must be collected.
        S3Client client = mock(S3Client.class);
        software.amazon.awssdk.services.s3.model.S3Object obj =
                software.amazon.awssdk.services.s3.model.S3Object.builder().build();
        when(client.listObjectsV2(
                        any(software.amazon.awssdk.services.s3.model.ListObjectsV2Request.class)))
                .thenReturn(
                        software.amazon.awssdk.services.s3.model.ListObjectsV2Response.builder()
                                .contents(
                                        software.amazon.awssdk.services.s3.model.S3Object.builder()
                                                .key("k-0")
                                                .build(),
                                        software.amazon.awssdk.services.s3.model.S3Object.builder()
                                                .key("k-1")
                                                .build())
                                .isTruncated(true)
                                .nextContinuationToken("t1")
                                .build(),
                        software.amazon.awssdk.services.s3.model.ListObjectsV2Response.builder()
                                .contents(
                                        software.amazon.awssdk.services.s3.model.S3Object.builder()
                                                .key("k-2")
                                                .build(),
                                        software.amazon.awssdk.services.s3.model.S3Object.builder()
                                                .key("k-3")
                                                .build())
                                .isTruncated(true)
                                .nextContinuationToken("t2")
                                .build(),
                        software.amazon.awssdk.services.s3.model.ListObjectsV2Response.builder()
                                .contents(
                                        software.amazon.awssdk.services.s3.model.S3Object.builder()
                                                .key("k-4")
                                                .build(),
                                        software.amazon.awssdk.services.s3.model.S3Object.builder()
                                                .key("k-5")
                                                .build())
                                .isTruncated(false)
                                .build());

        assertThat(ops(client).listAllKeys("prefix/"))
                .containsExactly("k-0", "k-1", "k-2", "k-3", "k-4", "k-5");
        verify(client, times(3))
                .listObjectsV2(
                        any(software.amazon.awssdk.services.s3.model.ListObjectsV2Request.class));
    }

    @Test
    void testEmptyKeyListIsNoop() throws IOException {
        S3Client client = mock(S3Client.class);
        ops(client).deleteBatch(new ArrayList<>(), 1000, 4);
        verify(client, times(0)).deleteObjects(any(DeleteObjectsRequest.class));
    }

    @Test
    void testRequestShapeRespectsBatchSize() throws IOException {
        S3Client client = mock(S3Client.class);
        // Concurrent pool threads record requests; a plain ArrayList can drop concurrent adds.
        List<DeleteObjectsRequest> captured =
                java.util.Collections.synchronizedList(new ArrayList<>());
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenAnswer(
                        invocation -> {
                            captured.add(invocation.getArgument(0));
                            return DeleteObjectsResponse.builder().build();
                        });

        ops(client).deleteBatch(keys(4), 3, 2);

        // Parallel execution completes batches in arbitrary order; assert by content.
        assertThat(captured).hasSize(2);
        List<Integer> batchSizes = new ArrayList<>();
        for (DeleteObjectsRequest request : captured) {
            assertThat(request.bucket()).isEqualTo("bucket");
            batchSizes.add(request.delete().objects().size());
        }
        assertThat(batchSizes).containsExactlyInAnyOrder(3, 1);
    }
}
