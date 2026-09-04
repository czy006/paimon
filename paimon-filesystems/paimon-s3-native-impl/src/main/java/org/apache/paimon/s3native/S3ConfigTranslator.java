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

import org.apache.paimon.options.Options;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Translates user-facing option keys ({@code s3.*}, {@code s3a.*} and a whitelist of {@code
 * fs.s3a.*} aliases) into the canonical {@code s3.*} option set consumed by {@link
 * S3NativeOptions}.
 *
 * <p>Priority: {@code s3.*} &gt; {@code s3a.*} &gt; {@code fs.s3a.*}. Unrecognized {@code fs.s3a.*}
 * keys are warned about and dropped: S3A semantics cannot be forwarded to AWS SDK v2, and failing
 * loudly beats silently mis-configuring.
 *
 * <p>[NEW] The prefix-mirroring idea is taken from {@code org.apache.paimon.s3.S3FileIO}
 * (paimon-s3-impl), the alias whitelist from the compatibility layer of the porting Spec (§6.3).
 */
final class S3ConfigTranslator {

    private static final Logger LOG = LoggerFactory.getLogger(S3ConfigTranslator.class);

    private static final String S3_PREFIX = "s3.";
    private static final String S3A_PREFIX = "s3a.";
    private static final String FS_S3A_PREFIX = "fs.s3a.";

    /**
     * Dotted {@code s3.*} key forms that map to a canonical key (both {@code s3.} and {@code s3a.}
     * prefixes). Without this mapping, {@code s3.access.key} would pass through unchanged and never
     * be read by {@link S3NativeOptions}, silently dropping credentials.
     */
    private static final Map<String, String> CANONICAL_KEY_ALIASES = new HashMap<>();

    /**
     * Canonical {@code s3.*} key to {@code fs.s3a.*} aliases it accepts. Keys not listed here are
     * generic: every {@code s3a.<k>} is an alias of {@code s3.<k>}.
     */
    private static final Map<String, String[]> FS_S3A_ALIASES = new HashMap<>();

    static {
        CANONICAL_KEY_ALIASES.put("s3.access.key", "s3.access-key");
        CANONICAL_KEY_ALIASES.put("s3.secret.key", "s3.secret-key");
        CANONICAL_KEY_ALIASES.put("s3.path.style.access", "s3.path-style-access");
        FS_S3A_ALIASES.put(
                "s3.access-key", new String[] {"fs.s3a.access.key", "fs.s3a.access-key"});
        FS_S3A_ALIASES.put(
                "s3.secret-key", new String[] {"fs.s3a.secret.key", "fs.s3a.secret-key"});
        FS_S3A_ALIASES.put("s3.endpoint", new String[] {"fs.s3a.endpoint"});
        FS_S3A_ALIASES.put("s3.region", new String[] {"fs.s3a.endpoint.region"});
        FS_S3A_ALIASES.put(
                "s3.path-style-access",
                new String[] {"fs.s3a.path.style.access", "fs.s3a.path-style-access"});
        FS_S3A_ALIASES.put("s3.upload.min.part.size", new String[] {"fs.s3a.multipart.size"});
        FS_S3A_ALIASES.put("s3.read.buffer.size", new String[] {"fs.s3a.readahead.range"});
        FS_S3A_ALIASES.put("s3.connection.max", new String[] {"fs.s3a.connection.maximum"});
        FS_S3A_ALIASES.put("s3.connection.timeout", new String[] {"fs.s3a.connection.timeout"});
        FS_S3A_ALIASES.put("s3.socket.timeout", new String[] {"fs.s3a.socket.timeout"});
        // S3A counts attempts, SDK v2 counts retries; translated by -1 during parsing.
        FS_S3A_ALIASES.put("s3.retry.max-num-retries", new String[] {"fs.s3a.attempts.maximum"});
    }

    private static String canonicalize(String s3Key) {
        return CANONICAL_KEY_ALIASES.getOrDefault(s3Key, s3Key);
    }

    /** Value translation for aliases whose semantics differ from the canonical key. */
    private static String translateValue(String alias, String value) {
        if ("fs.s3a.attempts.maximum".equals(alias)) {
            // S3A counts total attempts; SDK v2 retries exclude the first attempt.
            try {
                return String.valueOf(Integer.parseInt(value.trim()) - 1);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid value for " + alias + ": " + value, e);
            }
        }
        return value;
    }

    private S3ConfigTranslator() {}

    /** Returns a fresh {@link Options} containing only canonical {@code s3.*} keys. */
    static Options translate(Options raw) {
        Options normalized = new Options();

        // 1. Generic prefixes: s3.* wins over s3a.*; dotted aliases map to canonical keys.
        for (String key : raw.keySet()) {
            String value = raw.get(key);
            if (key.startsWith(S3_PREFIX)) {
                normalized.set(canonicalize(key), value);
            } else if (key.startsWith(S3A_PREFIX)) {
                String canonical = canonicalize(S3_PREFIX + key.substring(S3A_PREFIX.length()));
                if (normalized.get(canonical) == null) {
                    normalized.set(canonical, value);
                }
            }
        }

        // 2. fs.s3a.* whitelist, lowest priority; remember recognized aliases for step 3.
        Set<String> recognized = new HashSet<>();
        for (Map.Entry<String, String[]> entry : FS_S3A_ALIASES.entrySet()) {
            String canonical = entry.getKey();
            if (normalized.get(canonical) != null) {
                recognized.addAll(Arrays.asList(entry.getValue()));
                continue;
            }
            for (String alias : entry.getValue()) {
                String value = raw.get(alias);
                recognized.add(alias);
                if (value != null) {
                    normalized.set(canonical, translateValue(alias, value));
                    break;
                }
            }
        }

        // 3. Warn about fs.s3a.* keys we cannot honor.
        for (String key : raw.keySet()) {
            if (key.startsWith(FS_S3A_PREFIX) && !recognized.contains(key)) {
                LOG.warn(
                        "Option '{}' is not supported by paimon-s3-native and is ignored. "
                                + "See the S3 Native section of the filesystems documentation "
                                + "for supported options.",
                        key);
            }
        }

        return normalized;
    }
}
