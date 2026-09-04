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

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * {@code MinioTestContainer} provides a MinIO instance for the native S3 FileIO tests.
 *
 * <p>[NEW] Structure copied from {@code org.apache.paimon.s3.MinioTestContainer} (paimon-s3
 * module); depending on its test-jar would create a Maven cycle (paimon-s3 packages paimon-s3-impl
 * at runtime). Bucket creation uses this module's own AWS SDK v2 instead of SDK v1.
 */
public class S3NativeMinioContainer extends GenericContainer<S3NativeMinioContainer>
        implements BeforeAllCallback, AfterAllCallback {

    private static final int DEFAULT_PORT = 9000;

    private static final String MINIO_ACCESS_KEY = "MINIO_ROOT_USER";
    private static final String MINIO_SECRET_KEY = "MINIO_ROOT_PASSWORD";

    private static final String DEFAULT_STORAGE_DIRECTORY = "/data";
    private static final String HEALTH_ENDPOINT = "/minio/health/ready";

    private final String accessKey;
    private final String secretKey;
    private final String bucketName;

    public S3NativeMinioContainer() {
        this(randomString("bucket", 6));
    }

    public S3NativeMinioContainer(String bucketName) {
        // Newer than the repo-wide pinned DockerImageVersions.MINIO (2022-02): that release
        // rejects chunked-encoding uploads with an empty body (XAmzContentSHA256Mismatch),
        // which SDK v2 emits for directory markers.
        super("minio/minio:latest");

        this.accessKey = randomString("accessKey", 10);
        // secrets must have at least 8 characters
        this.secretKey = randomString("secret", 10);
        this.bucketName = bucketName;

        withNetworkAliases(randomString("minio", 6));
        addExposedPort(DEFAULT_PORT);
        withEnv(MINIO_ACCESS_KEY, this.accessKey);
        withEnv(MINIO_SECRET_KEY, this.secretKey);
        withCommand("server", DEFAULT_STORAGE_DIRECTORY);
        setWaitStrategy(
                new HttpWaitStrategy()
                        .forPort(DEFAULT_PORT)
                        .forPath(HEALTH_ENDPOINT)
                        .withStartupTimeout(Duration.ofMinutes(2)));
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        super.containerIsStarted(containerInfo);
        createDefaultBucket();
    }

    private static String randomString(String prefix, int length) {
        return String.format("%s-%s", prefix, Base58.randomString(length).toLowerCase(Locale.ROOT));
    }

    public Map<String, String> getS3ConfigOptions() {
        Map<String, String> config = new HashMap<>();
        config.put("s3.endpoint", getHttpEndpoint());
        config.put("s3.path.style.access", "true");
        config.put("s3.access.key", accessKey);
        config.put("s3.secret.key", secretKey);
        config.put("s3.region", "us-east-1");
        return config;
    }

    public String getS3UriForDefaultBucket() {
        return "s3://" + bucketName;
    }

    private String getHttpEndpoint() {
        return String.format("http://%s:%s", getHost(), getMappedPort(DEFAULT_PORT));
    }

    private void createDefaultBucket() {
        try (S3Client client =
                S3Client.builder()
                        .endpointOverride(URI.create(getHttpEndpoint()))
                        .region(Region.US_EAST_1)
                        .forcePathStyle(true)
                        .credentialsProvider(
                                StaticCredentialsProvider.create(
                                        AwsBasicCredentials.create(accessKey, secretKey)))
                        .build()) {
            client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
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
