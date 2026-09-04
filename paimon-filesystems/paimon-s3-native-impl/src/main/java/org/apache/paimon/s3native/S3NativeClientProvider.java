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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.utils.SdkAutoCloseable;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Builds and owns the sync ({@link S3Client}, Apache HTTP) and async ({@link S3AsyncClient}, Netty)
 * S3 clients from {@link S3NativeOptions}.
 *
 * <p>[PORTED] Derived from Apache Flink flink-filesystems/flink-s3-fs-native (FLINK-38592, Apache
 * License 2.0), class org.apache.flink.fs.s3native.S3ClientProvider (trimmed: no CRT transport, no
 * STS assume-role, no delegation tokens, no metrics, no TransferManager — parallel multipart is
 * issued via bounded-concurrency async UploadPart calls instead, see Spec deviation D3/D8). Local
 * reference: /Users/SL/javaProject/flink/flink-filesystems/flink-s3-fs-native/src/main/java/org/
 * apache/flink/fs/s3native/S3ClientProvider.java
 *
 * <p>Thread-safe after construction; {@link #close()} is idempotent.
 */
final class S3NativeClientProvider implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(S3NativeClientProvider.class);

    private final S3Client syncClient;
    private final S3AsyncClient asyncClient;
    private final AwsCredentialsProvider credentialsProvider;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private S3NativeClientProvider(
            S3Client syncClient,
            S3AsyncClient asyncClient,
            AwsCredentialsProvider credentialsProvider) {
        this.syncClient = syncClient;
        this.asyncClient = asyncClient;
        this.credentialsProvider = credentialsProvider;
    }

    static S3NativeClientProvider create(S3NativeOptions options) {
        AwsCredentialsProvider credentialsProvider = buildCredentialsProvider(options);
        Region region = resolveRegion(options.region);
        URI endpointUri = options.endpoint != null ? URI.create(options.endpoint) : null;

        // [PORTED] S3ClientProvider.Builder#build — service config shared by both clients.
        S3Configuration s3Config =
                S3Configuration.builder()
                        .pathStyleAccessEnabled(options.pathStyleAccess)
                        .chunkedEncodingEnabled(options.chunkedEncodingEnabled)
                        .checksumValidationEnabled(options.checksumValidationEnabled)
                        .build();

        // [PORTED] S3ClientProvider.Builder#build — StandardRetryStrategy with exponential
        // backoff; maxAttempts counts the initial attempt, retries exclude it.
        ClientOverrideConfiguration overrideConfig =
                ClientOverrideConfiguration.builder()
                        .retryStrategy(
                                StandardRetryStrategy.builder()
                                        .maxAttempts(options.maxRetries + 1)
                                        .backoffStrategy(
                                                BackoffStrategy.exponentialDelay(
                                                        options.retryBaseDelay,
                                                        options.retryMaxBackoff))
                                        .throttlingBackoffStrategy(
                                                BackoffStrategy.exponentialDelay(
                                                        options.retryThrottleBaseDelay,
                                                        options.retryMaxBackoff))
                                        .circuitBreakerEnabled(false)
                                        .build())
                        .build();

        // [PORTED] S3ClientProvider#buildSyncClient (non-CRT branch)
        S3ClientBuilder syncBuilder =
                S3Client.builder()
                        .credentialsProvider(credentialsProvider)
                        .region(region)
                        .serviceConfiguration(s3Config)
                        .overrideConfiguration(overrideConfig)
                        .httpClientBuilder(
                                ApacheHttpClient.builder()
                                        .maxConnections(options.maxConnections)
                                        .connectionTimeout(options.connectionTimeout)
                                        .socketTimeout(options.socketTimeout)
                                        .tcpKeepAlive(true)
                                        .connectionMaxIdleTime(options.connectionMaxIdleTime));
        if (endpointUri != null) {
            syncBuilder.endpointOverride(endpointUri);
        }
        S3Client syncClient = syncBuilder.build();

        // [PORTED] S3ClientProvider#buildAsyncClient (non-CRT branch)
        S3AsyncClientBuilder asyncBuilder =
                S3AsyncClient.builder()
                        .credentialsProvider(credentialsProvider)
                        .region(region)
                        .serviceConfiguration(s3Config)
                        .overrideConfiguration(overrideConfig)
                        .httpClientBuilder(
                                NettyNioAsyncHttpClient.builder()
                                        .maxConcurrency(options.maxConnections)
                                        .connectionTimeout(options.connectionTimeout)
                                        .readTimeout(options.socketTimeout)
                                        .connectionAcquisitionTimeout(options.connectionTimeout));
        if (endpointUri != null) {
            asyncBuilder.endpointOverride(endpointUri);
        }
        S3AsyncClient asyncClient;
        try {
            asyncClient = asyncBuilder.build();
        } catch (RuntimeException e) {
            // Do not leak the already-built sync client's connection pool on partial failure.
            try {
                syncClient.close();
            } catch (Exception closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }

        return new S3NativeClientProvider(syncClient, asyncClient, credentialsProvider);
    }

    private static AwsCredentialsProvider buildCredentialsProvider(S3NativeOptions options) {
        // [PORTED] S3ClientProvider#buildBaseCredentialsProvider — static credentials first,
        // then the default chain (env vars, profiles, instance metadata). [DEVIATION D8]
        // delegation-token and custom provider classes are out of scope.
        List<AwsCredentialsProvider> chain = new ArrayList<>();
        if (options.accessKey != null && options.secretKey != null) {
            chain.add(
                    StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(options.accessKey, options.secretKey)));
        }
        chain.add(DefaultCredentialsProvider.create());
        return AwsCredentialsProviderChain.builder().credentialsProviders(chain).build();
    }

    /**
     * [PORTED] S3ClientProvider#resolveRegion — explicit region, else AWS SDK region detection
     * (AWS_REGION env var, ~/.aws/config, EC2 metadata); no silent fallback.
     */
    private static Region resolveRegion(@Nullable String explicitRegion) {
        if (explicitRegion != null && !explicitRegion.trim().isEmpty()) {
            return Region.of(explicitRegion.trim());
        }
        try {
            return DefaultAwsRegionProviderChain.builder().build().getRegion();
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "AWS region could not be determined. Set the 's3.region' option, "
                            + "the AWS_REGION environment variable, or ~/.aws/config",
                    e);
        }
    }

    S3Client syncClient() {
        checkNotClosed();
        return syncClient;
    }

    S3AsyncClient asyncClient() {
        checkNotClosed();
        return asyncClient;
    }

    /** [PORTED] S3ClientProvider#closeAsync — sequential close, each guarded. */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            asyncClient.close();
        } catch (Exception e) {
            LOG.warn("Error closing S3 async client", e);
        }
        try {
            syncClient.close();
        } catch (Exception e) {
            LOG.warn("Error closing S3 sync client", e);
        }
        if (credentialsProvider instanceof SdkAutoCloseable) {
            try {
                ((SdkAutoCloseable) credentialsProvider).close();
            } catch (Exception e) {
                LOG.warn("Error closing credentials provider", e);
            }
        }
    }

    private void checkNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException("S3NativeClientProvider has been closed");
        }
    }
}
