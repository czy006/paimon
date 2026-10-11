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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.ResolvingFileIO;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ScanMode;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Fail-closed admission and retained-reference checks for the archive operation. */
final class ArchiveSafety {

    static final int MAX_HISTORY = 10000;
    static final int MAX_FILES = 100000;
    static final long MAX_BYTES = 1L << 40;

    private ArchiveSafety() {}

    static void validate(FileStoreTable table, Path target) throws IOException {
        validate(table, target, new HashMap<>());
    }

    static void validate(FileStoreTable table, Path target, Map<Snapshot, Long> validated)
            throws IOException {
        checkArgument(
                table.getClass() == AppendOnlyFileStoreTable.class
                        || table.getClass() == PrimaryKeyFileStoreTable.class,
                "archive_partitions requires an ordinary FileStoreTable");
        checkArgument(
                !table.partitionKeys().isEmpty(),
                "archive_partitions requires a partitioned table");
        checkArgument(
                !table.catalogEnvironment().supportsVersionManagement(),
                "archive_partitions currently requires filesystem-managed snapshots");
        checkArgument(
                table.bucketMode() == BucketMode.HASH_FIXED
                        || (table.primaryKeys().isEmpty()
                                && table.bucketMode() == BucketMode.BUCKET_UNAWARE),
                "archive_partitions does not support this bucket mode");
        checkArgument(
                "main".equals(table.snapshotManager().branch())
                        && table.branchManager().branches().isEmpty(),
                "archive_partitions requires main without other branches");
        checkArgument(
                table.fileIO() instanceof ResolvingFileIO,
                "archive_partitions requires resolving-file-io.enabled=true");
        validateOptions(table.schema());
        List<Path> roots = registeredRoots(table.schema());
        checkArgument(
                roots.stream().anyMatch(p -> p.equals(target)),
                "archive target must be persistently registered in data-file.external-paths");
        for (Path root : roots) {
            checkArgument(
                    !overlaps(root, table.location()),
                    "archive roots must not overlap the table directory");
            Path database = table.location().getParent();
            checkArgument(
                    database == null || !contains(root, database),
                    "archive root must not contain the database or warehouse");
        }
        if (!table.location().toUri().getScheme().equalsIgnoreCase(target.toUri().getScheme())) {
            // ResolvingFileIO is required above; retain the complete configured table IO.
            table.fileIO().exists(target);
        }
        validateHistory(table, validated);
    }

    static void validateOwnership(Catalog catalog, FileStoreTable owner, Path target)
            throws IOException {
        int inspected = 0;
        for (String database : catalog.listDatabases()) {
            final List<String> names;
            try {
                names = catalog.listTables(database);
            } catch (Catalog.DatabaseNotExistException e) {
                throw new IOException("Catalog changed while checking archive ownership", e);
            }
            for (String name : names) {
                checkArgument(
                        ++inspected <= MAX_HISTORY, "Archive catalog ownership budget exceeded");
                final Table candidate;
                try {
                    catalog.invalidateTable(Identifier.create(database, name));
                    candidate = catalog.getTable(Identifier.create(database, name));
                } catch (Catalog.TableNotExistException e) {
                    throw new IOException("Catalog changed while checking archive ownership", e);
                }
                if (!(candidate instanceof FileStoreTable)) {
                    continue;
                }
                FileStoreTable other = (FileStoreTable) candidate;
                if (other.location().equals(owner.location())) {
                    continue;
                }
                checkArgument(
                        !overlaps(target, other.location()),
                        "Archive target overlaps another table");
                List<String> branches = new ArrayList<>(other.branchManager().branches());
                branches.add("main");
                checkArgument(
                        branches.size() <= MAX_HISTORY, "Archive branch ownership budget exceeded");
                int schemaCount = 0;
                for (String branch : branches) {
                    SchemaManager manager = other.schemaManager().copyWithBranch(branch);
                    List<Long> schemas = manager.listAllIds();
                    schemaCount = Math.addExact(schemaCount, schemas.size());
                    checkArgument(
                            schemaCount <= MAX_HISTORY, "Archive schema ownership budget exceeded");
                    for (Long id : schemas) {
                        for (Path root : registeredRoots(manager.schema(id))) {
                            checkArgument(
                                    !overlaps(target, root),
                                    "Archive target is shared with another table");
                        }
                    }
                }
            }
        }
    }

    static List<Path> registeredRoots(TableSchema schema) {
        List<Path> roots = new ArrayList<>();
        String configured = CoreOptions.fromMap(schema.options()).dataFileExternalPaths();
        if (configured != null) {
            for (String entry : configured.split(",", -1)) {
                // Registered schema strings use Path(String)'s unescaped-text semantics.
                roots.add(ArchivePath.parseRoot(new Path(entry.trim()).toUri().toString()));
            }
        }
        return roots;
    }

    private static void validateOptions(TableSchema schema) {
        CoreOptions options = CoreOptions.fromMap(schema.options());
        checkArgument(
                schema.primaryKeys().isEmpty()
                        || options.mergeEngine() == CoreOptions.MergeEngine.DEDUPLICATE
                        || options.mergeEngine() == CoreOptions.MergeEngine.PARTIAL_UPDATE
                        || options.mergeEngine() == CoreOptions.MergeEngine.AGGREGATE,
                "archive_partitions does not support this primary-key merge engine");
        checkArgument(
                !options.deletionVectorsEnabled()
                        && !options.rowTrackingEnabled()
                        && !options.dataEvolutionEnabled()
                        && options.changelogProducer() == CoreOptions.ChangelogProducer.NONE,
                "Archive rejects DV, row tracking, data evolution and changelog in retained schemas");
        checkArgument(
                options.dataFilePathDirectory() == null
                        && !schema.options().containsKey("scan.fallback-branch"),
                "Archive requires the standard data layout without a fallback branch");
        checkArgument(
                options.externalPathStrategy() == CoreOptions.ExternalPathStrategy.NONE,
                "Archive requires data-file.external-paths.strategy=none");
        checkArgument(
                options.commitCallbacks().isEmpty()
                        && options.tagCallbacks().isEmpty()
                        && !options.partitionedTableInMetastore()
                        && options.tagToPartitionField() == null
                        && !options.tagCreateSuccessFile()
                        && Options.fromMap(schema.options())
                                        .get(IcebergOptions.METADATA_ICEBERG_STORAGE)
                                == IcebergOptions.StorageType.DISABLED,
                "Archive rejects external metadata mirrors and commit/tag side effects");
        checkArgument(
                Options.fromMap(schema.options()).get(CoreOptions.STREAM_SCAN_MODE)
                        != CoreOptions.StreamScanMode.FILE_MONITOR,
                "Archive does not support FILE_MONITOR");
    }

    private static void validateHistory(FileStoreTable table, Map<Snapshot, Long> validated)
            throws IOException {
        checkArgument(
                table.snapshotManager().snapshotCount() <= MAX_HISTORY,
                "Archive snapshot history budget exceeded");
        checkArgument(
                table.tagManager().tagCount() <= MAX_HISTORY,
                "Archive tag history budget exceeded");
        checkArgument(
                !table.changelogManager().changelogs().hasNext(),
                "Archive rejects retained independent changelog");
        Set<Long> schemas = new HashSet<>();
        schemas.add(table.schema().id());
        Iterator<Snapshot> snapshots = table.snapshotManager().snapshots();
        while (snapshots.hasNext()) {
            inspectSnapshot(table, snapshots.next(), schemas, validated);
        }
        for (Snapshot snapshot : table.tagManager().taggedSnapshots()) {
            inspectSnapshot(table, snapshot, schemas, validated);
        }
    }

    private static void inspectSnapshot(
            FileStoreTable table,
            Snapshot snapshot,
            Set<Long> schemas,
            Map<Snapshot, Long> validated)
            throws IOException {
        checkArgument(
                snapshot.indexManifest() == null && snapshot.changelogManifestList() == null,
                "Archive rejects retained index/changelog manifests");
        if (schemas.add(snapshot.schemaId())) {
            validateOptions(table.schemaManager().schema(snapshot.schemaId()));
        }
        if (validated.containsKey(snapshot)) {
            return;
        }
        long entries = validateScanBudget(table, snapshot);
        long work = entries;
        for (Long previous : validated.values()) {
            work = Math.addExact(work, previous);
        }
        checkArgument(work <= 10L * MAX_FILES, "Archive cumulative history budget exceeded");
        Iterator<ManifestEntry> files =
                table.store()
                        .newScan()
                        .withSnapshot(snapshot)
                        .withKind(ScanMode.ALL)
                        .keepStats()
                        .readFileIterator();
        int count = 0;
        while (files.hasNext()) {
            checkArgument(++count <= MAX_FILES, "Archive retained-file budget exceeded");
            ManifestEntry entry = files.next();
            if (schemas.add(entry.file().schemaId())) {
                validateOptions(table.schemaManager().schema(entry.file().schemaId()));
            }
            checkArgument(
                    entry.file().firstRowId() == null && entry.file().writeCols() == null,
                    "Archive rejects row-tracking/evolution file metadata");
        }
        validated.put(snapshot, entries);
    }

    static long validateScanBudget(FileStoreTable table, Snapshot snapshot) throws IOException {
        for (String list :
                new String[] {snapshot.baseManifestList(), snapshot.deltaManifestList()}) {
            checkArgument(
                    table.fileIO()
                                    .getFileStatus(
                                            table.store().pathFactory().toManifestListPath(list))
                                    .getLen()
                            <= (16L << 20),
                    "Archive manifest-list byte budget exceeded");
        }
        long entries = 0;
        for (ManifestFileMeta manifest :
                table.store().manifestListFactory().create().readDataManifests(snapshot)) {
            entries =
                    Math.addExact(
                            entries,
                            Math.addExact(manifest.numAddedFiles(), manifest.numDeletedFiles()));
            checkArgument(
                    entries <= MAX_FILES && manifest.fileSize() <= (64L << 20),
                    "Archive manifest entry/byte budget exceeded");
        }
        return entries;
    }

    static void validateFile(DataFileMeta file) {
        checkArgument(
                file.fileName().equals(new Path(file.fileName()).getName())
                        && !file.fileName().equals(".")
                        && !file.fileName().equals("..")
                        && !file.fileName().contains("\\")
                        && !file.fileName().contains(":"),
                "Archive requires a relative data basename");
        checkArgument(
                file.extraFiles().size() <= 1, "Archive supports at most one file-index sidecar");
        for (String extra : file.extraFiles()) {
            checkArgument(
                    extra.endsWith(".index")
                            && extra.equals(new Path(extra).getName())
                            && !extra.contains("\\")
                            && !extra.contains(":"),
                    "Archive rejects unknown or non-relative sidecars");
        }
    }

    static boolean overlaps(Path left, Path right) throws IOException {
        return contains(left, right) || contains(right, left);
    }

    static boolean contains(Path parent, Path child) throws IOException {
        return ArchivePath.contains(canonical(parent), canonical(child));
    }

    private static Path canonical(Path path) throws IOException {
        // Paimon-generated file/partition paths may contain literal percent-escaped directory
        // names.
        // Their Path URI already describes the physical path; do not apply the root-input decoder.
        Path normalized = new Path(path.toUri());
        if (!"file".equals(normalized.toUri().getScheme())) {
            return normalized;
        }
        java.nio.file.Path absolute = java.nio.file.Paths.get(normalized.toUri());
        java.nio.file.Path existing = absolute;
        while (existing != null && !Files.exists(existing)) {
            existing = existing.getParent();
        }
        checkArgument(existing != null, "Unable to resolve local archive ownership");
        java.nio.file.Path resolved = existing.toRealPath().resolve(existing.relativize(absolute));
        return new Path(resolved.toUri());
    }
}
