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
