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
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
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
 * <p>Reference: Apache Flink (Apache License 2.0), S3ClientProvider.Builder#build,
 * #buildSyncClient, #buildAsyncClient and #buildBaseCredentialsProvider, from the reviewed master
 * snapshot:
 * https://github.com/apache/flink/blob/6ad337697826a995789a6dafc7d379f64548e34e/flink-filesystems/flink-s3-fs-native/src/main/java/org/apache/flink/fs/s3native/S3ClientProvider.java#L646-L942
 *
 * <p>Paimon uses bounded async UploadPart calls and the SDK default credential chain, without
 * Flink's recoverable writer, delegation-token provider, explicit assume-role options or CRT
 * transport. Thread-safe after construction; {@link #close()} is idempotent.
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
                        // Providers are shared across SSE configurations: check every request,
                        // including SDK-resolved endpoints, on both sync and async clients.
                        .addExecutionInterceptor(new S3NativeSseHttpsInterceptor())
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
                                        // [ADAPTED] Circuit breaker stays at the Flink default
                                        // (off); the toggle option is out of scope.
                                        .circuitBreakerEnabled(false)
                                        .build())
                        .build();

        // [PORTED] S3ClientProvider.Builder#buildSyncClient (non-CRT branch)
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

        // [PORTED] S3ClientProvider.Builder#buildAsyncClient (non-CRT branch)
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
            // [ADAPTED] Flink leaks the sync client here; we close it before rethrowing.
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
        // Reference: Flink S3ClientProvider.Builder#buildBaseCredentialsProvider (link above).
        // SDK 2.55.1 DefaultCredentialsProvider#createChain provides web identity and role
        // profiles;
        // static session credentials must use AwsSessionCredentials#create to carry the token.
        // https://github.com/aws/aws-sdk-java-v2/blob/2.55.1/core/auth/src/main/java/software/amazon/awssdk/auth/credentials/DefaultCredentialsProvider.java#L93-L121
        // https://github.com/aws/aws-sdk-java-v2/blob/2.55.1/core/auth/src/main/java/software/amazon/awssdk/auth/credentials/AwsSessionCredentials.java#L47-L72
        List<AwsCredentialsProvider> chain = new ArrayList<>();
        if (options.accessKey != null && options.secretKey != null) {
            chain.add(
                    StaticCredentialsProvider.create(
                            // AWS temporary credentials require the session token in signing.
                            // https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-explicit.html
                            options.sessionToken == null
                                    ? AwsBasicCredentials.create(
                                            options.accessKey, options.secretKey)
                                    : AwsSessionCredentials.create(
                                            options.accessKey,
                                            options.secretKey,
                                            options.sessionToken)));
        }
        chain.add(DefaultCredentialsProvider.builder().build());
        return AwsCredentialsProviderChain.builder().credentialsProviders(chain).build();
    }

    /**
     * [PORTED] S3ClientProvider.Builder#resolveRegion — explicit region, else AWS SDK region
     * detection (AWS_REGION env var, ~/.aws/config, EC2 metadata); no silent fallback.
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

    /**
     * [PORTED] S3ClientProvider#closeAsync — sequential close, each guarded. [ADAPTED] Synchronous
     * instead of a time-bounded async close ({@code orTimeout} is Java 9+), so a hung client close
     * blocks the caller. Providers are cached across FileIO instances.
     *
     * <p>Reference: Flink S3ClientProvider#closeAsync, with synchronous close as a Java 8
     * adaptation:
     * https://github.com/apache/flink/blob/6ad337697826a995789a6dafc7d379f64548e34e/flink-filesystems/flink-s3-fs-native/src/main/java/org/apache/flink/fs/s3native/S3ClientProvider.java#L356-L391
     */
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
