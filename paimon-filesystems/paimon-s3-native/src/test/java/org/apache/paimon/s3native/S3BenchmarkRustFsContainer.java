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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.github.dockerjava.api.command.InspectContainerResponse;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;

import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * RustFS container for the benchmark comparison tests in this module. Creates its default bucket
 * with the AWS SDK v1 client that ships (unrelocated) inside the paimon-s3-impl fat jar — the
 * SDK-v2-based container of the impl module's test-jar cannot be reused here because its {@code
 * software.amazon.awssdk} references only exist in relocated form on this classpath.
 *
 * <p>[NEW] Structure follows {@code org.apache.paimon.s3.MinioTestContainer}.
 */
public class S3BenchmarkRustFsContainer extends GenericContainer<S3BenchmarkRustFsContainer>
        implements BeforeAllCallback, AfterAllCallback {

    private static final int DEFAULT_PORT = 9000;

    private static final String RUSTFS_ACCESS_KEY = "RUSTFS_ACCESS_KEY";
    private static final String RUSTFS_SECRET_KEY = "RUSTFS_SECRET_KEY";

    private final String accessKey;
    private final String secretKey;
    private final String bucketName;

    public S3BenchmarkRustFsContainer() {
        this(randomString("bench-bucket", 6));
    }

    private S3BenchmarkRustFsContainer(String bucketName) {
        // Pin the public RustFS release; see
        // https://docs.rustfs.com/en/installation/container/docker.
        super("rustfs/rustfs:1.0.1");

        this.accessKey = randomString("accessKey", 10);
        this.secretKey = randomString("secret", 10);
        this.bucketName = bucketName;

        withNetworkAliases(randomString("rustfs", 6));
        addExposedPort(DEFAULT_PORT);
        withEnv(RUSTFS_ACCESS_KEY, this.accessKey);
        withEnv(RUSTFS_SECRET_KEY, this.secretKey);
        withEnv("RUSTFS_CONSOLE_ENABLE", "false");
        withCommand("/data");
        setWaitStrategy(
                new HttpWaitStrategy()
                        .forPort(DEFAULT_PORT)
                        .forPath("/health")
                        .withStartupTimeout(Duration.ofMinutes(2)));
        withStartupAttempts(3);
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        super.containerIsStarted(containerInfo);
        createDefaultBucket();
    }

    private static String randomString(String prefix, int length) {
        return String.format("%s-%s", prefix, Base58.randomString(length).toLowerCase(Locale.ROOT));
    }

    /** Configuration accepted by both FileIO implementations. */
    public Map<String, String> getConfigOptions() {
        Map<String, String> config = new HashMap<>();
        config.put("s3.endpoint", getHttpEndpoint());
        config.put("s3.path.style.access", "true");
        config.put("s3.access.key", accessKey);
        config.put("s3.secret.key", secretKey);
        return config;
    }

    public String getBucketName() {
        return bucketName;
    }

    private String getHttpEndpoint() {
        return String.format("http://%s:%s", getHost(), getMappedPort(DEFAULT_PORT));
    }

    private void createDefaultBucket() {
        AmazonS3 client =
                AmazonS3Client.builder()
                        .withCredentials(
                                new AWSStaticCredentialsProvider(
                                        new BasicAWSCredentials(accessKey, secretKey)))
                        .withPathStyleAccessEnabled(true)
                        .withEndpointConfiguration(
                                new AwsClientBuilder.EndpointConfiguration(
                                        getHttpEndpoint(), "us-east-1"))
                        .build();
        try {
            client.createBucket(bucketName);
        } finally {
            client.shutdown();
        }
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        super.close();
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        super.start();
    }
}
