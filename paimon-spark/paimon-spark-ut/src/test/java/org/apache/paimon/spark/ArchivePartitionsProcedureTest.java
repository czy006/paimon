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

package org.apache.paimon.spark;

import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the real SQL CALL, including writes before and after archiving. */
public class ArchivePartitionsProcedureTest extends LocalPartitionRelocationTest {

    @Override
    protected String commitUserPrefix() {
        return "archive_partitions-";
    }

    @Override
    protected void relocateColdPartition(FileStoreTable table, List<ManifestEntry> original) {
        assertThat(SparkProcedures.names())
                .as(
                        "Procedure registry loaded from %s",
                        SparkProcedures.class.getProtectionDomain().getCodeSource().getLocation())
                .contains("archive_partitions");
        String call =
                "CALL paimon.sys.archive_partitions(table => 'db.T', "
                        + "partitions => 'pt=cold;pt=cold', target_path => '"
                        + targetRoot()
                        + "')";
        List<Row> archived = sparkSession().sql(call).collectAsList();
        assertThat(archived).hasSize(1);
        assertThat(archived.get(0).getString(0)).contains("state=ARCHIVED");
        List<Row> repeated = sparkSession().sql(call).collectAsList();
        assertThat(repeated).hasSize(1);
        assertThat(repeated.get(0).getString(0)).contains("state=NOOP");
    }

    @Test
    void archivesMultiplePartitionsAndContinuesPastMissingPartition() throws Exception {
        createTable("id INT, pt STRING", "pt", "");
        sparkSession().sql("INSERT INTO paimon.db.T VALUES (1,'cold'),(2,'hot')");
        long before = freshTable().snapshotManager().latestSnapshot().id();
        List<Row> rows = call("pt=missing;pt=cold;pt=hot;pt=cold");
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getString(0)).contains("state=FAILED", "PARTITION_NOT_FOUND");
        assertThat(rows.get(1).getString(0)).contains("state=ARCHIVED");
        assertThat(rows.get(2).getString(0)).contains("state=ARCHIVED");
        assertThat(freshTable().snapshotManager().latestSnapshot().id()).isEqualTo(before + 2);
        assertThat(freshTable().tagManager().allTagNames()).isEmpty();
        assertThat(sparkSession().sql("SELECT * FROM paimon.db.T").collectAsList())
                .extracting(Row::toString)
                .containsExactlyInAnyOrder("[1,cold]", "[2,hot]");
    }

    @Test
    void deduplicatesTypedCompositePartitions() throws Exception {
        createTable("id INT, dt INT, region STRING", "dt,region", "");
        sparkSession().sql("INSERT INTO paimon.db.T VALUES (7,1,'cn'),(8,1,'us')");
        long before = freshTable().snapshotManager().latestSnapshot().id();
        List<Row> rows = call("dt=01,region=cn;region=cn,dt=1");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(0)).contains("state=ARCHIVED");
        assertThat(freshTable().snapshotManager().latestSnapshot().id()).isEqualTo(before + 1);
        sparkSession().sql("INSERT INTO paimon.db.T VALUES (9,1,'cn')");
        assertThat(
                        sparkSession()
                                .sql("SELECT id FROM paimon.db.T WHERE dt=1 AND region='cn'")
                                .collectAsList())
                .extracting(r -> r.getInt(0))
                .containsExactlyInAnyOrder(7, 9);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {"", "pt=cold;", "pt=cold,pt=hot", "unknown=cold", "pt=*", "pt=cold,id=1"})
    void rejectsInvalidPartitionsWithoutPublishing(String partitions) throws Exception {
        createTable("id INT, pt STRING", "pt", "");
        sparkSession().sql("INSERT INTO paimon.db.T VALUES (1,'cold')");
        long before = freshTable().snapshotManager().latestSnapshot().id();
        if ("pt=*".equals(partitions)) {
            // Literal asterisk is not a wildcard; it names a nonexistent partition.
            assertThat(call(partitions).get(0).getString(0)).contains("PARTITION_NOT_FOUND");
        } else {
            assertThatThrownBy(() -> call(partitions)).isInstanceOf(Exception.class);
        }
        assertThat(freshTable().snapshotManager().latestSnapshot().id()).isEqualTo(before);
        assertThat(freshTable().tagManager().allTagNames()).isEmpty();
        assertThat(freshTable().fileIO().exists(targetRoot())).isFalse();
    }

    @Test
    void rejectsUnregisteredAndSharedRootsBeforeCopying() throws Exception {
        createTable("id INT, pt STRING", "pt", "");
        sparkSession().sql("INSERT INTO paimon.db.T VALUES (1,'cold')");
        assertThatThrownBy(
                        () ->
                                sparkSession()
                                        .sql(
                                                "CALL paimon.sys.archive_partitions('db.T','pt=cold','"
                                                        + targetRoot()
                                                        + "-other')")
                                        .collectAsList())
                .hasMessageContaining("persistently registered");
        sparkSession()
                .sql(
                        "CREATE TABLE paimon.db.other (id INT, pt STRING) USING paimon "
                                + "PARTITIONED BY (pt) TBLPROPERTIES ('data-file.external-paths'='"
                                + targetRoot()
                                + "')");
        assertThatThrownBy(() -> call("pt=cold")).hasMessageContaining("shared");
        assertThat(freshTable().fileIO().exists(targetRoot())).isFalse();
        assertThat(freshTable().tagManager().allTagNames()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"1B", "1MB"})
    void preservesFileIndexPredicateQueries(String threshold) throws Exception {
        createTable(
                "id INT, pt STRING",
                "pt",
                ",'file-index.bitmap.columns'='id',"
                        + "'file-index.in-manifest-threshold'='"
                        + threshold
                        + "'");
        sparkSession().sql("INSERT INTO paimon.db.T VALUES (1,'cold'),(2,'cold'),(3,'hot')");
        FileStoreTable table = freshTable();
        List<ManifestEntry> before = table.store().newScan().keepStats().plan().files();
        if ("1B".equals(threshold)) {
            assertThat(before).anyMatch(e -> !e.file().extraFiles().isEmpty());
        } else {
            assertThat(before).anyMatch(e -> e.file().embeddedIndex() != null);
        }
        assertThat(call("pt=cold").get(0).getString(0)).contains("state=ARCHIVED");
        assertThat(
                        sparkSession()
                                .newSession()
                                .sql("SELECT id FROM paimon.db.T WHERE id=2")
                                .collectAsList())
                .extracting(r -> r.getInt(0))
                .containsExactly(2);
        assertThat(call("pt=cold").get(0).getString(0)).contains("state=NOOP");
    }

    private void createTable(String columns, String partitions, String extraOptions) {
        sparkSession()
                .sql(
                        "CREATE TABLE paimon.db.T ("
                                + columns
                                + ") USING paimon "
                                + "PARTITIONED BY ("
                                + partitions
                                + ") TBLPROPERTIES ('bucket'='-1',"
                                + "'write-only'='true','data-file.external-paths'='"
                                + targetRoot()
                                + "',"
                                + "'data-file.external-paths.strategy'='none'"
                                + extraOptions
                                + ")");
    }

    private List<Row> call(String partitions) {
        return sparkSession()
                .sql(
                        "CALL paimon.sys.archive_partitions(table => 'db.T', "
                                + "partitions => '"
                                + partitions
                                + "', target_path => '"
                                + targetRoot()
                                + "')")
                .collectAsList();
    }
}
