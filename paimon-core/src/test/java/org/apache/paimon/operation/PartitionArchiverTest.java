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

package org.apache.paimon.operation;

import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.ResolvingFileIO;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real local files with deterministic failures at copy and publication boundaries. */
class PartitionArchiverTest {

    @TempDir java.nio.file.Path temp;
    private Path cold;
    private FaultIO io;
    private FileSystemCatalog catalog;
    private FileStoreTable table;

    @BeforeEach
    void setup() throws Exception {
        cold = new Path(temp.resolve("cold").toUri());
        Path warehouse = new Path(temp.resolve("warehouse").toUri());
        io = new FaultIO();
        io.configure(CatalogContext.create(new Options()));
        catalog = new FileSystemCatalog(io, warehouse);
        catalog.createDatabase("db", false);
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "-1");
        options.put("write-only", "true");
        options.put("data-file.external-paths", cold.toString());
        options.put("data-file.external-paths.strategy", "none");
        catalog.createTable(
                Identifier.create("db", "T"),
                new Schema(
                        Arrays.asList(
                                new DataField(0, "id", new IntType()),
                                new DataField(1, "pt", new VarCharType(20))),
                        Collections.singletonList("pt"),
                        Collections.emptyList(),
                        options,
                        ""),
                false);
        table = (FileStoreTable) catalog.getTable(Identifier.create("db", "T"));
        write(1);
    }

    @Test
    void copyCloseFailureNeverPublishesOrDeletesSource() throws Exception {
        List<ManifestEntry> before = table.store().newScan().keepStats().plan().files();
        long snapshot = table.snapshotManager().latestSnapshot().id();
        io.failColdClose = true;
        assertThat(archive().get(0)).contains("state=FAILED");
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(snapshot);
        assertThat(table.store().newScan().keepStats().plan().files())
                .containsExactlyElementsOf(before);
        assertThat(table.tagManager().allTagNames()).isEmpty();
        io.failColdClose = false;
        assertThat(archive().get(0)).contains("state=ARCHIVED");
    }

    @Test
    void detectsAppendDuringCopyAndStopsFurtherPartitions() throws Exception {
        io.afterColdClose =
                () -> {
                    try {
                        write(2);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };
        List<String> results =
                new PartitionArchiver(table, cold.toString(), catalog, Duration.ZERO)
                        .archive("pt=cold;pt=missing");
        assertThat(results.get(0)).contains("MAINTENANCE_VIOLATION", "recovery_required=true");
        assertThat(results.get(1)).contains("NOT_ATTEMPTED");
        assertThat(table.tagManager().allTagNames()).hasSize(1);
        assertThat(table.store().newScan().keepStats().plan().files())
                .allMatch(e -> !e.file().externalPath().isPresent());
    }

    @Test
    void retainsPinWhenBranchAppearsDuringCopy() throws Exception {
        io.afterColdClose = () -> table.createBranch("concurrent");
        List<String> results =
                new PartitionArchiver(table, cold.toString(), catalog, Duration.ZERO)
                        .archive("pt=cold;pt=missing");
        assertThat(results.get(0)).contains("MAINTENANCE_VIOLATION", "recovery_required=true");
        assertThat(results.get(1)).contains("NOT_ATTEMPTED");
        assertThat(table.tagManager().allTagNames()).hasSize(1);
    }

    @Test
    void reportsUnknownAndRetainsObjectsWhenConfirmationCannotRead() throws Exception {
        io.hidePublishedReads = true;
        List<String> results =
                new PartitionArchiver(table, cold.toString(), catalog, Duration.ZERO)
                        .archive("pt=cold;pt=missing");
        assertThat(results.get(0)).contains("state=UNKNOWN", "recovery_required=true");
        assertThat(results.get(1)).contains("NOT_ATTEMPTED");
        io.hidePublishedReads = false;
        io.blockSnapshotReads = false;
        assertThat(table.tagManager().allTagNames()).hasSize(1);
        assertThat(table.store().newScan().keepStats().plan().files())
                .allSatisfy(
                        e ->
                                assertThat(io.exists(new Path(e.file().externalPath().get())))
                                        .isTrue());
    }

    @Test
    void rejectsRootOwnedByAnotherTablesBranch() throws Exception {
        Identifier otherId = Identifier.create("db", "other");
        Map<String, String> options = new HashMap<>(table.options());
        options.remove("path");
        options.remove("data-file.external-paths");
        catalog.createTable(
                otherId,
                new Schema(
                        table.schema().fields(),
                        table.partitionKeys(),
                        table.primaryKeys(),
                        options,
                        ""),
                false);
        FileStoreTable other = (FileStoreTable) catalog.getTable(otherId);
        other.createBranch("cold_owner");
        other.schemaManager()
                .copyWithBranch("cold_owner")
                .commitChanges(SchemaChange.setOption("data-file.external-paths", cold.toString()));
        assertThatThrownBy(this::archive).hasMessageContaining("shared");
        assertThat(io.exists(cold)).isFalse();
        assertThat(table.tagManager().allTagNames()).isEmpty();
    }

    @Test
    void rejectsCaseInsensitiveFileMonitor() throws Exception {
        catalog.alterTable(
                Identifier.create("db", "T"),
                Collections.singletonList(
                        SchemaChange.setOption("stream-scan-mode", "FILE-MONITOR")),
                false);
        assertThatThrownBy(this::archive).hasMessageContaining("FILE_MONITOR");
        assertThat(io.exists(cold)).isFalse();
    }

    @Test
    void archivesLiteralEscapedPartitionDirectories() throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(2, BinaryString.fromString("a/b: c")));
            commit.commit(write.prepareCommit());
        }
        assertThat(
                        new PartitionArchiver(table, cold.toString(), catalog, Duration.ZERO)
                                .archive("pt=a/b: c")
                                .get(0))
                .contains("state=ARCHIVED");
    }

    @Test
    void rejectsSidecarWithUnsupportedEscapedRootBeforePublication() throws Exception {
        Path escaped = new Path(temp.resolve("cold space").toUri());
        catalog.alterTable(
                Identifier.create("db", "T"),
                Arrays.asList(
                        SchemaChange.setOption("data-file.external-paths", escaped.toString()),
                        SchemaChange.setOption("file-index.bitmap.columns", "id"),
                        SchemaChange.setOption("file-index.in-manifest-threshold", "1B")),
                false);
        table = (FileStoreTable) catalog.getTable(Identifier.create("db", "T"));
        write(2);
        assertThat(table.store().newScan().keepStats().plan().files())
                .anyMatch(e -> !e.file().extraFiles().isEmpty());
        long before = table.snapshotManager().latestSnapshot().id();
        assertThat(
                        new PartitionArchiver(
                                        table, escaped.toUri().toString(), catalog, Duration.ZERO)
                                .archive("pt=cold")
                                .get(0))
                .contains("state=FAILED");
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(before);
        assertThat(io.exists(escaped)).isFalse();
        assertThat(table.tagManager().allTagNames()).isEmpty();
    }

    @Test
    void reportsIncompleteWhenAppendWinsPublicationRace() throws Exception {
        io.beforeArchivePublication =
                () -> {
                    try {
                        write(2);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };
        List<String> results =
                new PartitionArchiver(table, cold.toString(), catalog, Duration.ZERO)
                        .archive("pt=cold;pt=missing");
        assertThat(results.get(0)).contains("state=INCOMPLETE", "committed_snapshot=");
        assertThat(results.get(1)).contains("NOT_ATTEMPTED");
        assertThat(table.tagManager().allTagNames()).hasSize(1);
        assertThat(table.store().newScan().keepStats().plan().files())
                .anyMatch(e -> e.file().externalPath().isPresent())
                .anyMatch(e -> !e.file().externalPath().isPresent());
    }

    @Test
    void reloadsCachedCatalogSchemaBeforeCommit() throws Exception {
        CachingCatalog cached = new CachingCatalog(catalog, new Options());
        FileStoreTable stale = (FileStoreTable) cached.getTable(Identifier.create("db", "T"));
        io.afterColdClose =
                () -> {
                    try {
                        catalog.alterTable(
                                Identifier.create("db", "T"),
                                SchemaChange.setOption("archive.test.marker", "changed"),
                                false);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };
        List<String> results =
                new PartitionArchiver(stale, cold.toString(), cached, Duration.ZERO)
                        .archive("pt=cold;pt=missing");
        assertThat(results.get(0)).contains("MAINTENANCE_VIOLATION");
        assertThat(results.get(1)).contains("NOT_ATTEMPTED");
        assertThat(table.tagManager().allTagNames()).hasSize(1);
    }

    @Test
    void plansFromFreshSchemaRatherThanStaleConstructor() throws Exception {
        CachingCatalog cached = new CachingCatalog(catalog, new Options());
        FileStoreTable stale = (FileStoreTable) cached.getTable(Identifier.create("db", "T"));
        catalog.alterTable(
                Identifier.create("db", "T"),
                SchemaChange.setOption("archive.test.marker", "before-planning"),
                false);
        assertThat(
                        new PartitionArchiver(stale, cold.toString(), cached, Duration.ZERO)
                                .archive("pt=cold")
                                .get(0))
                .contains("state=ARCHIVED");
        assertThat(table.tagManager().allTagNames()).isEmpty();
    }

    @Test
    void refusesInstanceReuseAfterUnknownWithoutRemovingPin() throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(2, BinaryString.fromString("hot")));
            commit.commit(write.prepareCommit());
        }
        PartitionArchiver archiver =
                new PartitionArchiver(table, cold.toString(), catalog, Duration.ZERO);
        io.hidePublishedReads = true;
        assertThat(archiver.archive("pt=cold").get(0)).contains("state=UNKNOWN");
        io.hidePublishedReads = false;
        io.blockSnapshotReads = false;
        List<String> pins = table.tagManager().allTagNames();
        assertThat(pins).hasSize(1);
        assertThatThrownBy(() -> archiver.archive("pt=hot"))
                .hasMessageContaining("single invocation");
        assertThat(table.tagManager().allTagNames()).containsExactlyElementsOf(pins);
    }

    @Test
    void confirmsPublishedSnapshotAfterLostCommitResponse() throws Exception {
        long before = table.snapshotManager().latestSnapshot().id();
        io.failPublishedResponse = true;
        assertThat(archive().get(0)).contains("state=ARCHIVED");
        io.failPublishedResponse = false;
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(before + 1);
        assertThat(table.tagManager().allTagNames()).isEmpty();
        assertThat(archive().get(0)).contains("state=NOOP");
    }

    @Test
    void rejectsSymlinkAliasOfTableBeforeCreatingPin() throws Exception {
        java.nio.file.Path alias = temp.resolve("alias");
        java.nio.file.Files.createSymbolicLink(
                alias, java.nio.file.Paths.get(table.location().toUri()));
        Path root = new Path(alias.resolve("nested").toUri());
        Map<String, String> modified = new HashMap<>(table.options());
        modified.put("data-file.external-paths", root.toString());
        FileStoreTable candidate = table.copy(modified);
        assertThatThrownBy(
                        () ->
                                ArchiveSafety.validate(
                                        candidate, ArchivePath.parseRoot(root.toString())))
                .hasMessageContaining("overlap");
        assertThat(table.tagManager().allTagNames()).isEmpty();
    }

    @Test
    void rejectsUnreadableProtectedHistory() throws Exception {
        table.createTag("protected", table.snapshotManager().latestSnapshot().id(), null);
        io.failTagReads = true;
        assertThatThrownBy(this::archive).isInstanceOf(Exception.class);
        io.failTagReads = false;
        assertThat(table.tagManager().allTagNames()).containsExactly("protected");
        assertThat(io.exists(cold)).isFalse();
    }

    @Test
    void rejectsCatalogManagedVersionAuthorityBeforeCopy() throws Exception {
        CatalogEnvironment managed =
                new CatalogEnvironment(
                        table.catalogEnvironment().identifier(),
                        table.uuid(),
                        null,
                        null,
                        null,
                        true);
        FileStoreTable candidate =
                FileStoreTableFactory.create(io, table.location(), table.schema(), managed);
        assertThatThrownBy(
                        () ->
                                ArchiveSafety.validate(
                                        candidate, ArchivePath.parseRoot(cold.toString())))
                .hasMessageContaining("filesystem-managed snapshots");
        assertThat(io.exists(cold)).isFalse();
        assertThat(table.tagManager().allTagNames()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"delete", "retarget", "ttl"})
    void stopsWhenSourcePinChangesDuringCopy(String action) throws Exception {
        write(2);
        io.afterColdClose =
                () -> {
                    String pin = table.tagManager().allTagNames().get(0);
                    if ("delete".equals(action)) {
                        table.deleteTag(pin);
                    } else if ("retarget".equals(action)) {
                        table.replaceTag(pin, 1L, null);
                    } else {
                        table.replaceTag(pin, 2L, Duration.ofMinutes(1));
                    }
                };
        List<String> results =
                new PartitionArchiver(table, cold.toString(), catalog, Duration.ZERO)
                        .archive("pt=cold;pt=missing");
        assertThat(results.get(0)).contains("MAINTENANCE_VIOLATION");
        assertThat(results.get(1)).contains("NOT_ATTEMPTED");
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(2);
    }

    @Test
    void stopsWhenProtectedSnapshotExpiresDuringCopy() throws Exception {
        write(2);
        io.afterColdClose = () -> table.snapshotManager().deleteSnapshot(1L);
        List<String> results =
                new PartitionArchiver(table, cold.toString(), catalog, Duration.ZERO)
                        .archive("pt=cold;pt=missing");
        assertThat(results.get(0)).contains("MAINTENANCE_VIOLATION");
        assertThat(results.get(1)).contains("NOT_ATTEMPTED");
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(2);
        assertThat(table.tagManager().allTagNames()).hasSize(1);
    }

    @Test
    void retainsProtectionWhenPinDisappearsAfterPublication() throws Exception {
        io.afterArchivePublication = () -> table.deleteTag(table.tagManager().allTagNames().get(0));
        List<String> results =
                new PartitionArchiver(table, cold.toString(), catalog, Duration.ZERO)
                        .archive("pt=cold;pt=missing");
        assertThat(results.get(0)).contains("state=INCOMPLETE", "committed_snapshot=2");
        assertThat(results.get(1)).contains("NOT_ATTEMPTED");
    }

    @Test
    void classifiesNativeConflictBeforePublicationAsMaintenanceViolation() throws Exception {
        ManifestEntry before = table.store().newScan().keepStats().plan().files().get(0);
        io.beforeArchivePublication = () -> rewrite(before);
        List<String> results =
                new PartitionArchiver(table, cold.toString(), catalog, Duration.ZERO)
                        .archive("pt=cold;pt=missing");
        assertThat(results.get(0)).contains("state=FAILED", "MAINTENANCE_VIOLATION");
        assertThat(results.get(1)).contains("NOT_ATTEMPTED");
        assertThat(table.tagManager().allTagNames()).hasSize(1);
    }

    @Test
    void confirmsCopiedPublicationWhenSkippedFileIsReplaced() throws Exception {
        assertThat(archive().get(0)).contains("state=ARCHIVED");
        ManifestEntry skipped = table.store().newScan().keepStats().plan().files().get(0);
        write(2);
        io.beforeArchivePublication = () -> rewrite(skipped);
        List<String> results =
                new PartitionArchiver(table, cold.toString(), catalog, Duration.ZERO)
                        .archive("pt=cold;pt=missing");
        assertThat(results.get(0)).contains("state=INCOMPLETE", "committed_snapshot=5");
        assertThat(results.get(1)).contains("NOT_ATTEMPTED");
        assertThat(table.tagManager().allTagNames()).hasSize(1);
    }

    private void rewrite(ManifestEntry entry) {
        try {
            Path source =
                    table.store()
                            .pathFactory()
                            .createDataFilePathFactory(entry.partition(), entry.bucket())
                            .toPath(entry.file());
            String name = "data-" + UUID.randomUUID() + "-" + entry.file().fileName();
            Path destination = new Path(source.getParent(), name);
            io.copyFile(source, destination, false);
            DataFileMeta after =
                    entry.file()
                            .rename(name)
                            .newExternalPath(
                                    entry.file().externalPath().isPresent()
                                            ? destination.toString()
                                            : null);
            ManifestCommittable committable = new ManifestCommittable(0);
            committable.addFileCommittable(
                    new CommitMessageImpl(
                            entry.partition(),
                            entry.bucket(),
                            entry.totalBuckets(),
                            DataIncrement.emptyIncrement(),
                            new CompactIncrement(
                                    Collections.singletonList(entry.file()),
                                    Collections.singletonList(after),
                                    Collections.emptyList())));
            try (FileStoreCommit commit =
                    table.store().newCommit("review-rewrite-" + UUID.randomUUID(), table)) {
                commit.ignoreEmptyCommit(true).commit(committable, false);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"non-partitioned", "pk-dynamic", "pk-key-dynamic", "first-row"})
    void rejectsUnsupportedTableModes(String mode) throws Exception {
        Map<String, String> options = new HashMap<>(table.options());
        options.remove("path");
        boolean pk = !"non-partitioned".equals(mode);
        options.put("bucket", "first-row".equals(mode) ? "2" : "-1");
        if ("first-row".equals(mode)) {
            options.put("merge-engine", "first-row");
        }
        List<String> partitions = pk ? table.partitionKeys() : Collections.emptyList();
        List<String> keys =
                !pk
                        ? Collections.emptyList()
                        : "pk-key-dynamic".equals(mode)
                                ? Collections.singletonList("id")
                                : Arrays.asList("pt", "id");
        Identifier id = Identifier.create("db", "unsupported");
        catalog.createTable(
                id, new Schema(table.schema().fields(), partitions, keys, options, ""), false);
        FileStoreTable candidate = (FileStoreTable) catalog.getTable(id);
        assertThatThrownBy(
                        () ->
                                ArchiveSafety.validate(
                                        candidate, ArchivePath.parseRoot(cold.toString())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(io.exists(cold)).isFalse();
    }

    @Test
    void rejectsPinMissingFromCapturedBaseline() throws Exception {
        io.removePinAtTagListing = true;
        List<String> results =
                new PartitionArchiver(table, cold.toString(), catalog, Duration.ZERO)
                        .archive("pt=cold;pt=missing");
        assertThat(results.get(0)).contains("SOURCE_PROTECTION_UNCONFIRMED");
        assertThat(results.get(1)).contains("NOT_ATTEMPTED");
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(1);
        assertThat(io.exists(cold)).isFalse();
    }

    @Test
    void doesNotTreatFailedReleaseVerificationAsSuccessfulPinDeletion() throws Exception {
        io.afterArchivePublication = () -> io.removePinAtRead = 4;
        List<String> results =
                new PartitionArchiver(table, cold.toString(), catalog, Duration.ZERO)
                        .archive("pt=cold;pt=missing");
        assertThat(results.get(0)).contains("state=INCOMPLETE", "committed_snapshot=2");
        assertThat(results.get(1)).contains("NOT_ATTEMPTED");
    }

    @Test
    void stopsWhenUserTagChangesEvenIfOwnedPinIsIntact() throws Exception {
        table.createTag("customer", 1L, null);
        write(2);
        io.afterColdClose = () -> table.deleteTag("customer");
        List<String> results =
                new PartitionArchiver(table, cold.toString(), catalog, Duration.ZERO)
                        .archive("pt=cold;pt=missing");
        assertThat(results.get(0)).contains("MAINTENANCE_VIOLATION");
        assertThat(results.get(1)).contains("NOT_ATTEMPTED");
        assertThat(table.tagManager().allTagNames()).hasSize(1);
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(2);
    }

    @Test
    void defaultIdleDaySkipsFreshPartitionWithoutPinOrSnapshot() throws Exception {
        List<String> result =
                new PartitionArchiver(table, cold.toString(), catalog).archive("pt=cold");
        assertThat(result.get(0)).contains("state=NOOP", "PARTITION_ACTIVE");
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(1);
        assertThat(table.tagManager().allTagNames()).isEmpty();
        assertThat(io.exists(cold)).isFalse();
    }

    @Test
    void idleThresholdIncludesExactBoundaryWithoutRefreshingActivity() throws Exception {
        long activity =
                table.store().newScan().readPartitionEntries().get(0).lastFileCreationTime();
        Duration threshold = Duration.ofHours(2);
        Clock before =
                Clock.fixed(
                        Instant.ofEpochMilli(activity + threshold.toMillis() - 1), ZoneOffset.UTC);
        assertThat(
                        new PartitionArchiver(table, cold.toString(), catalog, threshold, before)
                                .archive("pt=cold")
                                .get(0))
                .contains("PARTITION_ACTIVE");
        assertThat(io.exists(cold)).isFalse();
        Clock exact =
                Clock.fixed(Instant.ofEpochMilli(activity + threshold.toMillis()), ZoneOffset.UTC);
        assertThat(
                        new PartitionArchiver(table, cold.toString(), catalog, threshold, exact)
                                .archive("pt=cold")
                                .get(0))
                .contains("state=ARCHIVED");
        assertThat(table.store().newScan().readPartitionEntries().get(0).lastFileCreationTime())
                .isEqualTo(activity);
        assertThat(
                        new PartitionArchiver(table, cold.toString(), catalog, threshold, exact)
                                .archive("pt=cold")
                                .get(0))
                .contains("state=NOOP");
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(2);
    }

    @ParameterizedTest
    @CsvSource({"2,false", "-1,false", "2,true"})
    void idleEligibilityUsesLatestActivityAcrossAllBuckets(int buckets, boolean primaryKey)
            throws Exception {
        Identifier id = Identifier.create("db", "idle_table");
        Path root = new Path(temp.resolve("idle-root").toUri());
        Map<String, String> options = new HashMap<>(table.options());
        options.remove("path");
        options.put("bucket", String.valueOf(buckets));
        options.put("data-file.external-paths", root.toString());
        if (buckets > 0) {
            options.put("bucket-key", "id");
            options.put("bucket-function.type", "mod");
        }
        catalog.createTable(
                id,
                new Schema(
                        table.schema().fields(),
                        table.partitionKeys(),
                        primaryKey ? Arrays.asList("pt", "id") : Collections.emptyList(),
                        options,
                        ""),
                false);
        FileStoreTable candidate = (FileStoreTable) catalog.getTable(id);
        BatchWriteBuilder builder = candidate.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(1, BinaryString.fromString("cold")));
            write.write(GenericRow.of(2, BinaryString.fromString("cold")));
            commit.commit(write.prepareCommit());
        }
        List<ManifestEntry> live = candidate.store().newScan().keepStats().plan().files();
        assertThat(live.stream().map(ManifestEntry::bucket).distinct())
                .hasSize(buckets > 0 ? buckets : 1);
        long activity =
                candidate.store().newScan().readPartitionEntries().get(0).lastFileCreationTime();
        assertThat(activity)
                .isEqualTo(
                        live.stream()
                                .mapToLong(e -> e.file().creationTimeEpochMillis())
                                .max()
                                .getAsLong());
        Clock eligible =
                Clock.fixed(
                        Instant.ofEpochMilli(activity + Duration.ofDays(1).toMillis()),
                        ZoneOffset.UTC);
        assertThat(
                        new PartitionArchiver(
                                        candidate,
                                        root.toString(),
                                        catalog,
                                        PartitionArchiver.DEFAULT_PARTITION_IDLE_TIME,
                                        eligible)
                                .archive("pt=cold")
                                .get(0))
                .contains("state=ARCHIVED");
    }

    @ParameterizedTest
    @ValueSource(longs = {-1L, Long.MAX_VALUE})
    void rejectsNegativeOrOverflowingIdleTime(long seconds) throws Exception {
        assertThatThrownBy(
                        () ->
                                new PartitionArchiver(
                                        table,
                                        cold.toString(),
                                        catalog,
                                        Duration.ofSeconds(seconds)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(io.exists(cold)).isFalse();
        assertThat(table.tagManager().allTagNames()).isEmpty();
    }

    @Test
    void laterWriteMakesArchivedPartitionActiveAgain() throws Exception {
        assertThat(archive().get(0)).contains("state=ARCHIVED");
        write(2);
        long before = table.snapshotManager().latestSnapshot().id();
        assertThat(new PartitionArchiver(table, cold.toString(), catalog).archive("pt=cold").get(0))
                .contains("PARTITION_ACTIVE");
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(before);
        assertThat(table.tagManager().allTagNames()).isEmpty();
    }

    private List<String> archive() {
        return new PartitionArchiver(table, cold.toString(), catalog, Duration.ZERO)
                .archive("pt=cold");
    }

    private void write(int id) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(id, BinaryString.fromString("cold")));
            commit.commit(write.prepareCommit());
        }
    }

    private class FaultIO extends ResolvingFileIO {
        boolean failColdClose;
        boolean failPublishedResponse;
        boolean failTagReads;
        boolean hidePublishedReads;
        boolean blockSnapshotReads;
        Runnable afterColdClose;
        Runnable beforeArchivePublication;
        Runnable afterArchivePublication;
        boolean removePinAtTagListing;
        int removePinAtRead;

        @Override
        public org.apache.paimon.fs.FileStatus[] listStatus(Path path) throws IOException {
            org.apache.paimon.fs.FileStatus[] files = super.listStatus(path);
            if (removePinAtTagListing && path.getName().equals("tag")) {
                for (org.apache.paimon.fs.FileStatus file : files) {
                    if (file.getPath().getName().startsWith("tag-archive-partitions-")) {
                        removePinAtTagListing = false;
                        super.delete(file.getPath(), false);
                        return super.listStatus(path);
                    }
                }
            }
            return files;
        }

        @Override
        public org.apache.paimon.fs.SeekableInputStream newInputStream(Path path)
                throws IOException {
            if (removePinAtRead > 0 && path.getName().startsWith("tag-archive-partitions-")) {
                if (--removePinAtRead == 0) {
                    super.delete(path, false);
                }
            }
            if (failTagReads && path.toString().contains("/tag/")) {
                throw new IOException("Injected tag read failure");
            }
            if (blockSnapshotReads && path.toString().contains("/snapshot/")) {
                throw new IOException("Injected confirmation read failure");
            }
            return super.newInputStream(path);
        }

        @Override
        public boolean tryToWriteAtomic(Path path, String content) throws IOException {
            if (beforeArchivePublication != null && content.contains("archive_partitions-")) {
                Runnable action = beforeArchivePublication;
                beforeArchivePublication = null;
                action.run();
            }
            boolean written = super.tryToWriteAtomic(path, content);
            if (written
                    && afterArchivePublication != null
                    && content.contains("archive_partitions-")) {
                Runnable action = afterArchivePublication;
                afterArchivePublication = null;
                action.run();
            }
            if (written && hidePublishedReads && path.toString().contains("/snapshot/snapshot-")) {
                blockSnapshotReads = true;
                throw new IOException("Injected uncertain publication");
            }
            if (written
                    && failPublishedResponse
                    && path.toString().contains("/snapshot/snapshot-")) {
                failPublishedResponse = false;
                throw new IOException("Injected lost publication response");
            }
            return written;
        }

        @Override
        public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                throws IOException {
            PositionOutputStream delegate = super.newOutputStream(path, overwrite);
            if (!path.toString().startsWith(cold.toString() + "/")) {
                return delegate;
            }
            return new PositionOutputStream() {
                @Override
                public long getPos() throws IOException {
                    return delegate.getPos();
                }

                @Override
                public void write(int value) throws IOException {
                    delegate.write(value);
                }

                @Override
                public void write(byte[] bytes) throws IOException {
                    delegate.write(bytes);
                }

                @Override
                public void write(byte[] bytes, int offset, int length) throws IOException {
                    delegate.write(bytes, offset, length);
                }

                @Override
                public void flush() throws IOException {
                    delegate.flush();
                }

                @Override
                public void close() throws IOException {
                    delegate.close();
                    if (failColdClose) {
                        throw new IOException("Injected cold close failure");
                    }
                    if (afterColdClose != null) {
                        Runnable action = afterColdClose;
                        afterColdClose = null;
                        action.run();
                    }
                }
            };
        }
    }
}
