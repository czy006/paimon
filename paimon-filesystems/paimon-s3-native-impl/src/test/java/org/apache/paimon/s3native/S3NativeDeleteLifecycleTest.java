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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** API completion must also mean submitted deletion workers have stopped issuing requests. */
@Timeout(15)
class S3NativeDeleteLifecycleTest {

    static Stream<RuntimeException> paginationFailures() {
        return Stream.of(
                (S3Exception)
                        S3Exception.builder().statusCode(503).message("list unavailable").build(),
                SdkClientException.create("listing connection failed"));
    }

    @ParameterizedTest
    @MethodSource("paginationFailures")
    void testPaginationFailureWaitsForActiveDeletionAndSkipsQueuedBatch(
            RuntimeException pageFailure) throws Exception {
        verifyPaginationFailure(pageFailure, null);
    }

    @Test
    void testPaginationFailurePreservesPrimaryCauseAndAggregatesCleanupFailure() throws Exception {
        RuntimeException pageFailure = SdkClientException.create("listing connection failed");
        RuntimeException deleteFailure = SdkClientException.create("deletion connection failed");
        verifyPaginationFailure(pageFailure, deleteFailure);
    }

    private static void verifyPaginationFailure(
            RuntimeException pageFailure, RuntimeException deleteFailure) throws Exception {
        S3Client client = mock(S3Client.class);
        CountDownLatch deletionStarted = new CountDownLatch(1);
        CountDownLatch releaseDeletion = new CountDownLatch(1);
        CountDownLatch paginationFailed = new CountDownLatch(1);
        AtomicInteger deletionCalls = new AtomicInteger();
        AtomicInteger pages = new AtomicInteger();
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenAnswer(
                        invocation -> {
                            deletionCalls.incrementAndGet();
                            deletionStarted.countDown();
                            awaitUninterruptibly(releaseDeletion);
                            if (deleteFailure != null) {
                                throw deleteFailure;
                            }
                            return DeleteObjectsResponse.builder().build();
                        });
        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenAnswer(
                        invocation -> {
                            if (pages.getAndIncrement() == 0) {
                                return page("next", "p/a", "p/b");
                            }
                            await(deletionStarted);
                            paginationFailed.countDown();
                            throw pageFailure;
                        });
        ExecutorService caller = Executors.newSingleThreadExecutor();
        Future<Result> result =
                caller.submit(
                        () ->
                                capture(
                                        () ->
                                                new S3NativeObjectOperations(client, "bucket")
                                                        .deletePrefixStreaming("p/", 1, 1)));
        try {
            await(paginationFailed);
            assertThatThrownBy(() -> result.get(200, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            releaseDeletion.countDown();
            Result returned = result.get(10, TimeUnit.SECONDS);
            assertThat(returned.failure).isInstanceOf(IOException.class).hasCause(pageFailure);
            assertThat(returned.interrupted).isFalse();
            assertThat(deletionCalls.get()).isEqualTo(1);
            if (deleteFailure != null) {
                assertThat(returned.failure.getSuppressed()).hasSize(1);
                assertThat(returned.failure.getSuppressed()[0])
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("1 keys")
                        .hasCause(deleteFailure);
            }
        } finally {
            releaseDeletion.countDown();
            result.get(10, TimeUnit.SECONDS);
            caller.shutdownNow();
        }
    }

    @Test
    void testBackpressureInterruptionKeepsFirstFutureAndRestoresInterrupt() throws Exception {
        S3Client client = mock(S3Client.class);
        CountDownLatch deletionStarted = new CountDownLatch(1);
        CountDownLatch releaseDeletion = new CountDownLatch(1);
        CountDownLatch interruptedCaller = new CountDownLatch(1);
        AtomicInteger deletionCalls = new AtomicInteger();
        AtomicInteger pages = new AtomicInteger();
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenAnswer(
                        invocation -> {
                            deletionCalls.incrementAndGet();
                            deletionStarted.countDown();
                            awaitUninterruptibly(releaseDeletion);
                            return DeleteObjectsResponse.builder().build();
                        });
        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenAnswer(
                        invocation -> {
                            if (pages.getAndIncrement() == 0) {
                                return page("next", "p/a", "p/b");
                            }
                            await(deletionStarted);
                            // The third batch crosses the two-batch limit. get() must throw
                            // without removing the blocked first batch from cleanup ownership.
                            Thread.currentThread().interrupt();
                            interruptedCaller.countDown();
                            return page(null, "p/c");
                        });
        ExecutorService caller = Executors.newSingleThreadExecutor();
        Future<Result> result =
                caller.submit(
                        () ->
                                capture(
                                        () ->
                                                new S3NativeObjectOperations(client, "bucket")
                                                        .deletePrefixStreaming("p/", 1, 1)));
        try {
            await(interruptedCaller);
            assertThatThrownBy(() -> result.get(200, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            releaseDeletion.countDown();
            Result returned = result.get(10, TimeUnit.SECONDS);
            assertThat(returned.failure)
                    .isInstanceOf(IOException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(returned.interrupted).isTrue();
            assertThat(deletionCalls.get()).isEqualTo(1);
        } finally {
            releaseDeletion.countDown();
            result.get(10, TimeUnit.SECONDS);
            caller.shutdownNow();
        }
    }

    @Test
    void testBatchInterruptionWaitsForWorkerInsteadOfCancellingItsFuture() throws Exception {
        S3Client client = mock(S3Client.class);
        CountDownLatch deletionStarted = new CountDownLatch(1);
        CountDownLatch releaseDeletion = new CountDownLatch(1);
        AtomicInteger deletionCalls = new AtomicInteger();
        AtomicReference<Thread> callingThread = new AtomicReference<>();
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenAnswer(
                        invocation -> {
                            deletionCalls.incrementAndGet();
                            deletionStarted.countDown();
                            // Transport cancellation is best-effort. Model a request whose
                            // worker keeps running after a thread interruption.
                            awaitUninterruptibly(releaseDeletion);
                            return DeleteObjectsResponse.builder().build();
                        });
        ExecutorService caller = Executors.newSingleThreadExecutor();
        Future<Result> result =
                caller.submit(
                        () -> {
                            callingThread.set(Thread.currentThread());
                            return capture(
                                    () ->
                                            new S3NativeObjectOperations(client, "bucket")
                                                    .deleteBatch(
                                                            Arrays.asList("a", "b", "c"), 1, 1));
                        });
        try {
            await(deletionStarted);
            callingThread.get().interrupt();
            assertThatThrownBy(() -> result.get(200, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            releaseDeletion.countDown();
            Result returned = result.get(10, TimeUnit.SECONDS);
            assertThat(returned.failure)
                    .isInstanceOf(IOException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(returned.interrupted).isTrue();
            assertThat(deletionCalls.get()).isEqualTo(1);
        } finally {
            releaseDeletion.countDown();
            result.get(10, TimeUnit.SECONDS);
            caller.shutdownNow();
        }
    }

    private static ListObjectsV2Response page(String token, String... keys) {
        List<S3Object> contents = new java.util.ArrayList<>();
        for (String key : keys) {
            contents.add(S3Object.builder().key(key).build());
        }
        return ListObjectsV2Response.builder()
                .contents(contents)
                .nextContinuationToken(token)
                .build();
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    await(latch);
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static Result capture(IORunnable operation) {
        try {
            operation.run();
            return new Result(null, Thread.currentThread().isInterrupted());
        } catch (Throwable failure) {
            return new Result(failure, Thread.currentThread().isInterrupted());
        }
    }

    private interface IORunnable {
        void run() throws IOException;
    }

    private static final class Result {
        private final Throwable failure;
        private final boolean interrupted;

        private Result(Throwable failure, boolean interrupted) {
            this.failure = failure;
            this.interrupted = interrupted;
        }
    }
}
