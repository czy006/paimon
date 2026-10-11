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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ScanMode;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.options.CatalogOptions.RESOLVING_FILE_IO_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Storage compatibility probe for the proposed byte-copy/COMPACT relocation.
 *
 * <p>This test-only harness is not the production archiver: it operates on an isolated, quiescent,
 * data-only table and does not implement admission, recovery, cleanup, or the Spark procedure.
 *
 * <p>Sources: https://paimon.apache.org/docs/1.3/maintenance/configurations/ (resolving FileIO) and
 * https://paimon.apache.org/docs/1.3/spark/sql-query/#batch-time-travel (snapshot/tag queries).
 */
public abstract class PartitionRelocationTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionRelocationTestBase.class);

    @TempDir java.nio.file.Path tempDir;

    private SparkSession spark;
    private Path tablePath;
    private Path coldRoot;
    private Options ioOptions;

    protected abstract Path coldRoot();

    protected Map<String, String> storageOptions() {
        return Collections.emptyMap();
    }

    @BeforeEach
    public void startSpark() {
        Path warehouse = new Path(tempDir.resolve("warehouse").toUri());
        tablePath = new Path(warehouse, "db.db/T");
        coldRoot = coldRoot();
        ioOptions = new Options();
        storageOptions().forEach(ioOptions::setString);
        ioOptions.set(RESOLVING_FILE_IO_ENABLED, true);

        SparkSession.Builder builder =
                SparkSession.builder()
                        .master("local[2]")
                        .appName("partition-relocation-storage-probe")
                        .config("spark.ui.enabled", "false")
                        .config("spark.sql.shuffle.partitions", "2")
                        .config("spark.sql.catalog.paimon", SparkCatalog.class.getName())
                        .config("spark.sql.catalog.paimon.warehouse", warehouse.toString())
                        .config(
                                "spark.sql.extensions",
                                "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions");
        ioOptions.toMap().forEach((k, v) -> builder.config("spark.sql.catalog.paimon." + k, v));
        spark = builder.getOrCreate();
        spark.sql("CREATE DATABASE paimon.db");
    }

    @AfterEach
    public void stopSpark() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();
    }

    @ParameterizedTest(name = "{0}, bucket={1}, primaryKey={2}")
    @CsvSource({
        "parquet,2,false", "orc,2,false", "avro,2,false",
        "parquet,-1,false", "orc,-1,false", "avro,-1,false",
        "parquet,2,true", "orc,2,true", "avro,2,true"
    })
    public void preservesSnapshotsAndQueries(String format, int buckets, boolean primaryKey)
            throws Exception {
        spark.sql(
                "CREATE TABLE paimon.db.T (id INT, v INT, pt STRING) USING paimon "
                        + "PARTITIONED BY (pt) TBLPROPERTIES ("
                        + "'bucket'='"
                        + buckets
                        + "',"
                        + (buckets > 0 ? "'bucket-key'='id','bucket-function.type'='mod'," : "")
                        + (primaryKey ? "'primary-key'='pt,id'," : "")
                        + "'file.format'='"
                        + format
                        + "',"
                        + "'write-only'='true',"
                        + "'data-file.external-paths'='"
                        + coldRoot
                        + "',"
                        + "'data-file.external-paths.strategy'='none')");
        spark.sql(
                "INSERT INTO paimon.db.T VALUES "
                        + "(1,10,'cold'),(2,20,'cold'),(3,30,'hot'),(4,40,'cold'),(5,50,'cold')");
        if (primaryKey) {
            spark.sql("INSERT INTO paimon.db.T VALUES (1,11,'cold')");
            spark.sql("DELETE FROM paimon.db.T WHERE id=4");
        } else {
            spark.sql("INSERT INTO paimon.db.T VALUES (2,20,'cold')");
        }

        List<String> expected = new ArrayList<>();
        Collections.addAll(expected, "[2,20,cold]", "[3,30,hot]", "[5,50,cold]");
        if (primaryKey) {
            expected.add("[1,11,cold]");
        } else {
            Collections.addAll(expected, "[1,10,cold]", "[2,20,cold]", "[4,40,cold]");
        }
        assertRows(spark, "paimon.db.T", expected);

        FileStoreTable table = freshTable();
        Snapshot before = table.snapshotManager().latestSnapshot();
        List<ManifestEntry> original = liveFiles(table, before.id());
        assertThat(original)
                .isNotEmpty()
                .allSatisfy(e -> assertThat(e.file().externalPath()).isEmpty());
        table.tagManager()
                .createTag(before, "before_relocation", null, Collections.emptyList(), false);

        relocateColdPartition(table, original);

        FileStoreTable reopened = freshTable();
        Snapshot after = reopened.snapshotManager().latestSnapshot();
        assertThat(after.id()).isEqualTo(before.id() + 1);
        assertThat(after.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(after.totalRecordCount()).isEqualTo(before.totalRecordCount());
        assertThat(after.deltaRecordCount()).isZero();
        assertThat(after.commitUser()).startsWith(commitUserPrefix());
        assertThat(after.commitIdentifier()).isZero();
        assertThat(reopened.snapshotManager().snapshot(before.id())).isEqualTo(before);
        assertThat(liveFiles(reopened, before.id())).containsExactlyInAnyOrderElementsOf(original);
        assertThat(reopened.tagManager().getOrThrow("before_relocation").id())
                .isEqualTo(before.id());

        List<ManifestEntry> relocated = liveFiles(reopened, after.id());
        assertThat(relocated).hasSameSizeAs(original);
        assertThat(relocated.stream().filter(PartitionRelocationTestBase::isCold))
                .isNotEmpty()
                .allSatisfy(
                        e ->
                                assertThat(e.file().externalPath().get())
                                        .startsWith(coldRoot.toString() + "/"));
        assertThat(relocated.stream().filter(e -> !isCold(e)).collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(
                        original.stream().filter(e -> !isCold(e)).collect(Collectors.toList()));
        List<ManifestEntry> coldBefore =
                original.stream()
                        .filter(PartitionRelocationTestBase::isCold)
                        .collect(Collectors.toList());
        assertThat(coldBefore.stream().map(ManifestEntry::bucket).distinct())
                .hasSize(buckets > 0 ? buckets : 1);
        for (ManifestEntry old : coldBefore) {
            List<ManifestEntry> matches =
                    relocated.stream()
                            .filter(e -> e.file().fileName().endsWith("-" + old.file().fileName()))
                            .collect(Collectors.toList());
            assertThat(matches).hasSize(1);
            ManifestEntry actual = matches.get(0);
            assertThat(
                            ManifestEntry.create(
                                    actual.kind(),
                                    actual.partition(),
                                    actual.bucket(),
                                    actual.totalBuckets(),
                                    actual.file()
                                            .rename(old.file().fileName())
                                            .newExternalPath(
                                                    old.file().externalPath().orElse(null))))
                    .isEqualTo(old);
        }
        List<ManifestEntry> delta =
                reopened.store()
                        .newScan()
                        .withSnapshot(after.id())
                        .withKind(ScanMode.DELTA)
                        .keepStats()
                        .plan()
                        .files();
        assertThat(delta).hasSize(coldBefore.size() * 2);
        assertThat(delta.stream().filter(e -> e.kind() == FileKind.DELETE))
                .hasSize(coldBefore.size());
        assertThat(delta.stream().filter(e -> e.kind() == FileKind.ADD)).hasSize(coldBefore.size());
        assertThat(delta).allMatch(PartitionRelocationTestBase::isCold);
        assertThat(
                        spark.sql(
                                        "SELECT snapshot_id,commit_kind,delta_record_count FROM paimon.db.`T$snapshots` WHERE snapshot_id="
                                                + after.id())
                                .collectAsList()
                                .toString())
                .isEqualTo("[[" + after.id() + ",COMPACT,0]]");
        assertRows(spark.newSession(), "paimon.db.T", expected);
        assertRows(spark.newSession(), "paimon.db.T VERSION AS OF " + before.id(), expected);
        assertRows(spark.newSession(), "paimon.db.T VERSION AS OF 'before_relocation'", expected);
        assertThat(
                        spark.sql("SELECT id,v FROM paimon.db.T WHERE pt='hot'")
                                .collectAsList()
                                .toString())
                .isEqualTo("[[3,30]]");
        assertThat(
                        spark.sql("SELECT id,v FROM paimon.db.T WHERE pt='cold' AND id=5")
                                .collectAsList()
                                .toString())
                .isEqualTo("[[5,50]]");

        // The strategy remains NONE: new writes are hot, including inside an archived partition.
        spark.sql("INSERT INTO paimon.db.T VALUES (6,60,'cold')");
        List<String> withAppend = new ArrayList<>(expected);
        withAppend.add("[6,60,cold]");
        assertRows(spark.newSession(), "paimon.db.T", withAppend);
        assertRows(spark.newSession(), "paimon.db.T VERSION AS OF " + after.id(), expected);
        assertRows(spark.newSession(), "paimon.db.T VERSION AS OF " + before.id(), expected);
        assertRows(spark.newSession(), "paimon.db.T VERSION AS OF 'before_relocation'", expected);
        List<ManifestEntry> mixedPartition =
                liveFiles(freshTable(), after.id() + 1).stream()
                        .filter(PartitionRelocationTestBase::isCold)
                        .collect(Collectors.toList());
        assertThat(mixedPartition).anySatisfy(e -> assertThat(e.file().externalPath()).isEmpty());
        assertThat(mixedPartition).anySatisfy(e -> assertThat(e.file().externalPath()).isPresent());
        List<String> latestRows = new ArrayList<>(withAppend);
        if (primaryKey) {
            // Resolve competing versions across storage tiers, including a hot tombstone.
            spark.sql("INSERT INTO paimon.db.T VALUES (1,111,'cold')");
            spark.sql("DELETE FROM paimon.db.T WHERE id=5");
            latestRows.remove("[1,11,cold]");
            latestRows.remove("[5,50,cold]");
            latestRows.add("[1,111,cold]");
            assertRows(spark.newSession(), "paimon.db.T", latestRows);
            assertRows(
                    spark.newSession(),
                    "paimon.db.T VERSION AS OF " + (after.id() + 1),
                    withAppend);
            assertRows(spark.newSession(), "paimon.db.T VERSION AS OF " + after.id(), expected);
            assertRows(spark.newSession(), "paimon.db.T VERSION AS OF " + before.id(), expected);
            assertRows(
                    spark.newSession(), "paimon.db.T VERSION AS OF 'before_relocation'", expected);
        }
        assertMissingColdFileFails(original, relocated, expected, latestRows);
        LOG.info(
                "Relocation probe: scheme={}, format={}, buckets={}, pk={}, snapshots={}->{}, "
                        + "coldFiles={}, originalRows={}, finalRows={}; history, mixed reads and missing-object check passed",
                coldRoot.toUri().getScheme(),
                format,
                buckets,
                primaryKey,
                before.id(),
                after.id(),
                coldBefore.size(),
                expected.size(),
                latestRows.size());
    }

    private void assertMissingColdFileFails(
            List<ManifestEntry> original,
            List<ManifestEntry> relocated,
            List<String> beforeRows,
            List<String> latestRows)
            throws Exception {
        ManifestEntry old =
                original.stream().filter(PartitionRelocationTestBase::isCold).findFirst().get();
        ManifestEntry replacement =
                relocated.stream()
                        .filter(e -> e.file().fileName().endsWith("-" + old.file().fileName()))
                        .findFirst()
                        .get();
        FileStoreTable table = freshTable();
        FileIO io = table.fileIO();
        Path source =
                table.store()
                        .pathFactory()
                        .createDataFilePathFactory(old.partition(), old.bucket())
                        .toPath(old.file());
        Path target = new Path(replacement.file().externalPath().get());
        assertThat(io.delete(target, false)).isTrue();
        try {
            assertThatThrownBy(
                            () ->
                                    spark.newSession()
                                            .sql("SELECT id,v,pt FROM paimon.db.T")
                                            .collectAsList())
                    .as(
                            "Missing cold data must fail, not silently fall back to the original hot file")
                    .isInstanceOf(Exception.class)
                    .hasRootCauseInstanceOf(FileNotFoundException.class)
                    .hasStackTraceContaining(target.getName());
            assertRows(
                    spark.newSession(),
                    "paimon.db.T VERSION AS OF 'before_relocation'",
                    beforeRows);
        } finally {
            // Restore only this test's deliberately removed object from its still-retained source.
            io.copyFile(source, target, false);
        }
        assertThat(digest(io, target)).containsExactly(digest(io, source));
        assertRows(spark.newSession(), "paimon.db.T", latestRows);
    }

    protected FileStoreTable freshTable() throws Exception {
        return FileStoreTableFactory.create(
                FileIO.get(tablePath, CatalogContext.create(ioOptions)), tablePath);
    }

    private static List<ManifestEntry> liveFiles(FileStoreTable table, long snapshot) {
        return table.store()
                .newScan()
                .withSnapshot(snapshot)
                .withKind(ScanMode.ALL)
                .keepStats()
                .plan()
                .files();
    }

    private static boolean isCold(ManifestEntry entry) {
        return "cold".equals(entry.partition().getString(0).toString());
    }

    private static void assertRows(SparkSession session, String relation, List<String> expected) {
        List<String> actual =
                session.sql("SELECT id,v,pt FROM " + relation).collectAsList().stream()
                        .map(Row::toString)
                        .collect(Collectors.toList());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    protected String commitUserPrefix() {
        return "storage-probe-";
    }

    protected Path targetRoot() {
        return coldRoot;
    }

    protected SparkSession sparkSession() {
        return spark;
    }

    protected void relocateColdPartition(FileStoreTable table, List<ManifestEntry> original)
            throws Exception {
        // Release-1.3.2 FileStoreCommit/CompactIncrement and DataFileMeta.newExternalPath are
        // internal APIs. This probe tests the proposed composition; no built-in archive API exists.
        Map<Integer, List<ManifestEntry>> buckets =
                original.stream()
                        .filter(PartitionRelocationTestBase::isCold)
                        .collect(Collectors.groupingBy(ManifestEntry::bucket));
        assertThat(buckets).isNotEmpty();
        ManifestCommittable committable = new ManifestCommittable(0);
        FileIO io = table.fileIO();
        for (List<ManifestEntry> entries : buckets.values()) {
            List<DataFileMeta> before = new ArrayList<>();
            List<DataFileMeta> after = new ArrayList<>();
            for (ManifestEntry entry : entries) {
                DataFileMeta source = entry.file();
                assertThat(source.extraFiles()).as("This probe is data-only").isEmpty();
                Path sourcePath =
                        table.store()
                                .pathFactory()
                                .createDataFilePathFactory(entry.partition(), entry.bucket())
                                .toPath(source);
                String newName = "data-" + UUID.randomUUID() + "-" + source.fileName();
                Path destination =
                        new Path(
                                new Path(
                                        coldRoot,
                                        table.store()
                                                .pathFactory()
                                                .relativeBucketPath(
                                                        entry.partition(), entry.bucket())),
                                newName);
                io.mkdirs(destination.getParent());
                io.copyFile(sourcePath, destination, false);
                assertThat(io.getFileStatus(destination).getLen()).isEqualTo(source.fileSize());
                assertThat(digest(io, destination)).containsExactly(digest(io, sourcePath));
                assertThat(io.exists(sourcePath)).isTrue();
                DataFileMeta replacement =
                        source.rename(newName).newExternalPath(destination.toString());
                assertThat(
                                replacement
                                        .rename(source.fileName())
                                        .newExternalPath(source.externalPath().orElse(null)))
                        .isEqualTo(source);
                before.add(source);
                after.add(replacement);
            }
            ManifestEntry first = entries.get(0);
            committable.addFileCommittable(
                    new CommitMessageImpl(
                            first.partition(),
                            first.bucket(),
                            first.totalBuckets(),
                            DataIncrement.emptyIncrement(),
                            new CompactIncrement(before, after, Collections.emptyList())));
        }
        try (FileStoreCommit commit =
                table.store().newCommit("storage-probe-" + UUID.randomUUID(), null)) {
            commit.ignoreEmptyCommit(true).commit(committable, true);
        }
    }

    private static byte[] digest(FileIO io, Path path) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream input = io.newInputStream(path)) {
            byte[] buffer = new byte[8192];
            int length;
            while ((length = input.read(buffer)) != -1) {
                digest.update(buffer, 0, length);
            }
        }
        return digest.digest();
    }
}
