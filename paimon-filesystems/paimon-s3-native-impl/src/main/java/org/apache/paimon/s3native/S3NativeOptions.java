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

import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Typed, immutable view of the canonical {@code s3.*} option set with validation and clamping.
 *
 * <p>[PORTED] Defaults and validation rules are taken from Apache Flink flink-filesystems/
 * flink-s3-fs-native (FLINK-38592), class org.apache.flink.fs.s3native.NativeS3FileSystemFactory.
 * Local reference: /Users/SL/javaProject/flink/flink-filesystems/flink-s3-fs-native/src/main/java/
 * org/apache/flink/fs/s3native/NativeS3FileSystemFactory.java
 */
final class S3NativeOptions {

    static final long MIN_PART_SIZE_BYTES = 5L << 20;
    static final long MAX_PART_SIZE_BYTES = 5L << 30;
    static final int MIN_READ_BUFFER_BYTES = 256 * 1024;
    static final int DEFAULT_READ_BUFFER_BYTES = 256 * 1024;
    static final int DEFAULT_MAX_CONNECTIONS = 50;
    static final int MAX_BATCH_DELETE_KEYS = 1000;

    @Nullable final String accessKey;
    @Nullable final String secretKey;
    @Nullable final String region;
    @Nullable final String endpoint;
    final boolean pathStyleAccess;
    final boolean chunkedEncodingEnabled;
    final boolean checksumValidationEnabled;
    final long partSizeBytes;
    final int maxConcurrentUploads;
    final int readBufferSize;
    final int maxConnections;
    final Duration connectionTimeout;
    final Duration socketTimeout;
    final Duration connectionMaxIdleTime;
    final int maxRetries;
    final Duration retryBaseDelay;
    final Duration retryThrottleBaseDelay;
    final Duration retryMaxBackoff;
    final String tmpDir;

    // [PORTED-ICE Spec §14 I4-I6] Write-path enhancements adopted from Iceberg S3FileIOProperties.
    /** Multipart switches on only above partSize * factor (Iceberg default 1.5). */
    final double multipartThresholdFactor;
    /** When true, part-level and whole-object MD5 are sent as Content-MD5. */
    final boolean checksumEnabled;
    /** Storage class for new objects (e.g. GLACIER, INTELLIGENT_TIERING); null = bucket default. */
    @Nullable final String writeStorageClass;
    /** Tags for new objects, "k1:v1,k2:v2"; empty = none. */
    final Map<String, String> writeTags;
    /** Canned ACL for new objects; null = none. */
    @Nullable final String acl;

    // [PORTED-ICE Spec §14 I7] Parallel batch deletion adopted from Iceberg S3FileIOProperties.
    /** Keys per DeleteObjects request, 1..1000 (S3 API limit). */
    final int deleteBatchSize;
    /** Concurrent DeleteObjects requests during bulk deletes (Iceberg default: CPU cores). */
    final int deleteThreads;

    private S3NativeOptions(
            @Nullable String accessKey,
            @Nullable String secretKey,
            @Nullable String region,
            @Nullable String endpoint,
            boolean pathStyleAccess,
            boolean chunkedEncodingEnabled,
            boolean checksumValidationEnabled,
            long partSizeBytes,
            int maxConcurrentUploads,
            int readBufferSize,
            int maxConnections,
            Duration connectionTimeout,
            Duration socketTimeout,
            Duration connectionMaxIdleTime,
            int maxRetries,
            Duration retryBaseDelay,
            Duration retryThrottleBaseDelay,
            Duration retryMaxBackoff,
            String tmpDir,
            double multipartThresholdFactor,
            boolean checksumEnabled,
            String writeStorageClass,
            Map<String, String> writeTags,
            String acl,
            int deleteBatchSize,
            int deleteThreads) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.region = region;
        this.endpoint = endpoint;
        this.pathStyleAccess = pathStyleAccess;
        this.chunkedEncodingEnabled = chunkedEncodingEnabled;
        this.checksumValidationEnabled = checksumValidationEnabled;
        this.partSizeBytes = partSizeBytes;
        this.maxConcurrentUploads = maxConcurrentUploads;
        this.readBufferSize = readBufferSize;
        this.maxConnections = maxConnections;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
        this.connectionMaxIdleTime = connectionMaxIdleTime;
        this.maxRetries = maxRetries;
        this.retryBaseDelay = retryBaseDelay;
        this.retryThrottleBaseDelay = retryThrottleBaseDelay;
        this.retryMaxBackoff = retryMaxBackoff;
        this.tmpDir = tmpDir;
        this.multipartThresholdFactor = multipartThresholdFactor;
        this.checksumEnabled = checksumEnabled;
        this.writeStorageClass = writeStorageClass;
        this.writeTags = writeTags;
        this.acl = acl;
        this.deleteBatchSize = deleteBatchSize;
        this.deleteThreads = deleteThreads;
    }

    /**
     * Parses a normalized {@link Options} (canonical {@code s3.*} keys only, see {@link
     * S3ConfigTranslator}). Applies defaults, clamping and validation; invalid values fail fast.
     *
     * <p>System property fallbacks for {@code s3.endpoint} / {@code s3.path.style.access} keep test
     * harnesses configurable without touching Paimon options.
     */
    static S3NativeOptions from(Options normalized) {
        String endpoint = normalized.get("s3.endpoint");
        if (endpoint == null) {
            endpoint = System.getProperty("s3.endpoint");
        }
        boolean pathStyle = normalized.getBoolean("s3.path-style-access", false);
        if (!normalized.containsKey("s3.path-style-access")) {
            // [ADAPTED] Flink lets the sysprop override an explicit option; fallback-only is the
            // safer precedence (an explicit option always wins).
            pathStyle = Boolean.parseBoolean(System.getProperty("s3.path.style.access"));
        }

        // [PORTED] NativeS3FileSystemFactory#create — part size must be within 5MB..5GB.
        long partSize = parseSize("s3.upload.min.part.size", normalized, MIN_PART_SIZE_BYTES);
        if (partSize < MIN_PART_SIZE_BYTES || partSize > MAX_PART_SIZE_BYTES) {
            throw new IllegalArgumentException(
                    "s3.upload.min.part.size must be between 5MB and 5GB, but was: " + partSize);
        }

        int maxConcurrent =
                normalized.getInteger(
                        "s3.upload.max.concurrent.uploads",
                        Runtime.getRuntime().availableProcessors());
        if (maxConcurrent <= 0) {
            throw new IllegalArgumentException(
                    "s3.upload.max.concurrent.uploads must be positive, but was: " + maxConcurrent);
        }

        // [PORTED] NativeS3FileSystemFactory#create — read buffer is clamped to at least 256KB.
        int readBuffer =
                (int)
                        Math.min(
                                Integer.MAX_VALUE,
                                parseSize(
                                        "s3.read.buffer.size",
                                        normalized,
                                        DEFAULT_READ_BUFFER_BYTES));
        if (readBuffer < MIN_READ_BUFFER_BYTES) {
            readBuffer = MIN_READ_BUFFER_BYTES;
        }

        int maxConnections = normalized.getInteger("s3.connection.max", DEFAULT_MAX_CONNECTIONS);
        if (maxConnections <= 0) {
            throw new IllegalArgumentException(
                    "s3.connection.max must be positive, but was: " + maxConnections);
        }

        Duration retryBase = parseDuration("s3.retry.base-delay", normalized, "100ms");
        Duration retryThrottleBase =
                parseDuration("s3.retry.throttle.base-delay", normalized, "1000ms");
        Duration retryMax = parseDuration("s3.retry.max-backoff", normalized, "20000ms");
        if (retryMax.compareTo(retryBase) < 0 || retryMax.compareTo(retryThrottleBase) < 0) {
            throw new IllegalArgumentException(
                    "s3.retry.max-backoff ("
                            + retryMax
                            + ") must be >= s3.retry.base-delay ("
                            + retryBase
                            + ") and s3.retry.throttle.base-delay ("
                            + retryThrottleBase
                            + ")");
        }

        return new S3NativeOptions(
                normalized.get("s3.access-key"),
                normalized.get("s3.secret-key"),
                normalized.get("s3.region"),
                endpoint,
                pathStyle,
                normalized.getBoolean("s3.chunked-encoding.enabled", true),
                normalized.getBoolean("s3.checksum-validation.enabled", true),
                partSize,
                maxConcurrent,
                readBuffer,
                maxConnections,
                parseDuration("s3.connection.timeout", normalized, "60000ms"),
                parseDuration("s3.socket.timeout", normalized, "60000ms"),
                parseDuration("s3.connection.max-idle-time", normalized, "60000ms"),
                validatedRetries(normalized),
                retryBase,
                retryThrottleBase,
                retryMax,
                normalized.getString("s3.upload.tmp.dir", System.getProperty("java.io.tmpdir")),
                validatedThresholdFactor(normalized),
                normalized.getBoolean("s3.checksum-enabled", false),
                normalized.get("s3.write.storage-class"),
                parseTags(normalized.get("s3.write.tags")),
                normalized.get("s3.acl"),
                validatedDeleteBatchSize(normalized),
                validatedDeleteThreads(normalized));
    }

    /** [PORTED-ICE Spec §14 I7] Keys per DeleteObjects request, 1..1000 (S3 API limit). */
    private static int validatedDeleteBatchSize(Options options) {
        int batchSize = options.getInteger("s3.delete.batch-size", MAX_BATCH_DELETE_KEYS);
        if (batchSize < 1 || batchSize > MAX_BATCH_DELETE_KEYS) {
            throw new IllegalArgumentException(
                    "s3.delete.batch-size must be between 1 and 1000, but was: " + batchSize);
        }
        return batchSize;
    }

    /** [PORTED-ICE Spec §14 I7] Concurrent DeleteObjects requests; Iceberg default = CPU cores. */
    private static int validatedDeleteThreads(Options options) {
        int threads =
                options.getInteger(
                        "s3.delete.num-threads", Runtime.getRuntime().availableProcessors());
        if (threads < 1) {
            throw new IllegalArgumentException(
                    "s3.delete.num-threads must be positive, but was: " + threads);
        }
        return threads;
    }

    private static double validatedThresholdFactor(Options options) {
        double factor = Double.parseDouble(options.getString("s3.multipart.threshold", "1.5"));
        if (factor < 1.0) {
            throw new IllegalArgumentException(
                    "s3.multipart.threshold must be >= 1.0, but was: " + factor);
        }
        return factor;
    }

    private static Map<String, String> parseTags(String tags) {
        Map<String, String> result = new LinkedHashMap<>();
        if (tags == null || tags.trim().isEmpty()) {
            return result;
        }
        for (String pair : tags.split(",")) {
            int sep = pair.indexOf(':');
            if (sep <= 0 || sep == pair.length() - 1) {
                throw new IllegalArgumentException(
                        "Invalid s3.write.tags entry '" + pair + "', expected key:value pairs");
            }
            result.put(pair.substring(0, sep).trim(), pair.substring(sep + 1).trim());
        }
        return result;
    }

    private static int validatedRetries(Options options) {
        int retries = options.getInteger("s3.retry.max-num-retries", 3);
        if (retries < 0) {
            throw new IllegalArgumentException(
                    "s3.retry.max-num-retries must be >= 0, but was: " + retries);
        }
        return retries;
    }

    private static long parseSize(String key, Options options, long defaultBytes) {
        String value = options.get(key);
        if (value == null) {
            return defaultBytes;
        }
        try {
            return MemorySize.parse(value.trim()).getBytes();
        } catch (Exception e) {
            try {
                return Long.parseLong(value.trim());
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException("Invalid size value for " + key + ": " + value);
            }
        }
    }

    /**
     * Accepts plain milliseconds, Hadoop-style durations ({@code 60s}, {@code 2m}, {@code 1h}) and
     * ISO-8601 ({@code PT60S}).
     */
    private static Duration parseDuration(String key, Options options, String defaultValue) {
        String value = options.get(key);
        if (value == null) {
            value = defaultValue;
        }
        String trimmed = value.trim();
        try {
            if (trimmed.startsWith("P")) {
                return Duration.parse(trimmed);
            }
            long multiplier = 1L;
            String number = trimmed;
            String lower = trimmed.toLowerCase(java.util.Locale.ROOT);
            if (lower.endsWith("ms")) {
                number = trimmed.substring(0, trimmed.length() - 2);
            } else if (lower.endsWith("s") || lower.endsWith("m") || lower.endsWith("h")) {
                number = trimmed.substring(0, trimmed.length() - 1);
                char unit = lower.charAt(lower.length() - 1);
                multiplier = unit == 's' ? 1000L : unit == 'm' ? 60_000L : 3_600_000L;
            }
            return Duration.ofMillis(Long.parseLong(number.trim()) * multiplier);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid duration value for "
                            + key
                            + ": "
                            + value
                            + " (expected e.g. 500, 60s, 2m or PT2M)");
        }
    }
}
