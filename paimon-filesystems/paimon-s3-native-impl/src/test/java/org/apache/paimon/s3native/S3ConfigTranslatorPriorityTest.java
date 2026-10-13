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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Regressions for conflicting native S3 option aliases. */
class S3ConfigTranslatorPriorityTest {

    @Test
    void testConflictingAliasesHaveStablePriority() {
        String[][] groups = {
            {"access-key", "access.key"},
            {"secret-key", "secret.key"},
            {"session-token", "session.token"},
            {"path-style-access", "path.style.access"}
        };
        for (String[] group : groups) {
            String[] keys = {
                "s3." + group[0],
                "s3." + group[1],
                "s3a." + group[0],
                "s3a." + group[1],
                "fs.s3a." + group[1],
                "fs.s3a." + group[0]
            };
            // Remove higher-priority tiers successively to verify every adjacent priority.
            for (int first = 0; first < keys.length; first++) {
                for (boolean reverse : new boolean[] {false, true}) {
                    for (int unrelatedCount : new int[] {0, 128}) {
                        Options raw = new Options();
                        for (int offset = 0; offset < keys.length - first; offset++) {
                            int index = reverse ? keys.length - 1 - offset : first + offset;
                            raw.set(keys[index], "value-" + index);
                        }
                        // Growing the backing HashMap must not change the selected identity.
                        for (int i = 0; i < unrelatedCount; i++) {
                            raw.set("unrelated.option." + i, "unrelated-" + i);
                        }

                        Options normalized = S3ConfigTranslator.translate(raw);
                        assertThat(normalized.toMap())
                                .as(
                                        "%s first=%s reverse=%s unrelated=%s",
                                        group[0], first, reverse, unrelatedCount)
                                .containsExactlyEntriesOf(
                                        java.util.Collections.singletonMap(
                                                keys[0], "value-" + first));
                    }
                }
            }
        }
    }

    @Test
    void testUnrelatedNativeOptionsKeepTheirValues() {
        Options raw = new Options();
        raw.set("s3.access-key", "canonical-access");
        raw.set("s3.access.key", "dotted-access");
        raw.set("s3a.upload.min.part.size", "16 mb");
        raw.set("s3.custom.option", "unchanged");
        raw.set("fs.s3a.attempts.maximum", "10");
        raw.set("table.option", "ignored");

        assertThat(S3ConfigTranslator.translate(raw).toMap())
                .containsEntry("s3.access-key", "canonical-access")
                .containsEntry("s3.upload.min.part.size", "16 mb")
                .containsEntry("s3.custom.option", "unchanged")
                .containsEntry("s3.retry.max-num-retries", "9")
                .hasSize(4);
    }

    @Test
    void testPathStyleCanonicalValueWinsOverDottedAlias() {
        Options raw = new Options();
        raw.set("s3.path-style-access", "false");
        raw.set("s3.path.style.access", "true");
        raw.set("s3a.path-style-access", "true");
        raw.set("fs.s3a.path.style.access", "true");

        assertThat(S3ConfigTranslator.translate(raw).getBoolean("s3.path-style-access", true))
                .isFalse();
    }

    @Test
    void testWarningsOnlyNameUnsupportedFsAliases() {
        Options raw = new Options();
        raw.set("fs.s3a.access.key", "preferred-access");
        raw.set("fs.s3a.access-key", "other-access");
        raw.set("fs.s3a.unsupported", "ignored");
        List<String> warnings = new ArrayList<>();
        AbstractAppender appender =
                new AbstractAppender("s3-alias-warnings", null, null, false, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        warnings.add(event.getMessage().getFormattedMessage());
                    }
                };
        // Initialize the SLF4J binding before configuring the test appender.
        org.slf4j.LoggerFactory.getLogger(S3ConfigTranslator.class);
        Logger logger = (Logger) LogManager.getLogger(S3ConfigTranslator.class);
        Level previousLevel = logger.getLevel();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.WARN);
        try {
            assertThat(S3ConfigTranslator.translate(raw).get("s3.access-key"))
                    .isEqualTo("preferred-access");
            assertThat(warnings).hasSize(1);
            assertThat(warnings.get(0)).contains("fs.s3a.unsupported");
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(previousLevel);
            appender.stop();
        }
    }
}
