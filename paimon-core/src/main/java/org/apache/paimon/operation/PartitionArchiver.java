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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Copies full partitions and atomically replaces file references without running a data compactor.
 *
 * <p>Callers must freeze writers and pause cleanup throughout the operation. Tags preserve source
 * references, not unpublished objects. Sources:
 * https://paimon.apache.org/docs/1.3/maintenance/manage-tags/ and
 * https://paimon.apache.org/docs/1.3/maintenance/manage-snapshots/. The COMPACT composition uses
 * release-1.3.2 FileStoreCommit/CompactIncrement internals; it is a new procedure, not a documented
 * built-in archive API.
 */
public final class PartitionArchiver {

    public static final Duration DEFAULT_PARTITION_IDLE_TIME = Duration.ofDays(1);

    private final FileStoreTable table;
    private final Catalog catalog;
    private final Path target;
    private final Identifier owner;
    private final String tableUuid;
    private final Duration partitionIdleTime;
    private final Clock clock;
    private final Map<Snapshot, Long> validatedHistory = new LinkedHashMap<>();
    private final String operation = UUID.randomUUID().toString();
    private final String commitUser = "archive_partitions-" + operation;
    private boolean invoked;

    public PartitionArchiver(FileStoreTable table, String targetPath, Catalog catalog) {
        this(table, targetPath, catalog, DEFAULT_PARTITION_IDLE_TIME);
    }

    public PartitionArchiver(
            FileStoreTable table, String targetPath, Catalog catalog, Duration partitionIdleTime) {
        this(table, targetPath, catalog, partitionIdleTime, Clock.systemUTC());
    }

    PartitionArchiver(
            FileStoreTable table,
            String targetPath,
            Catalog catalog,
            Duration partitionIdleTime,
            Clock clock) {
        checkArgument(
                partitionIdleTime != null && !partitionIdleTime.isNegative(),
                "partition_idle_time must be non-negative");
        try {
            checkArgument(
                    partitionIdleTime.isZero() || partitionIdleTime.toMillis() > 0,
                    "partition_idle_time must be zero or at least one millisecond");
        } catch (ArithmeticException tooLarge) {
            throw new IllegalArgumentException("partition_idle_time is too large", tooLarge);
        }
        this.partitionIdleTime = partitionIdleTime;
        this.clock = java.util.Objects.requireNonNull(clock, "clock");
        checkArgument(
                !catalog.supportsVersionManagement(),
                "archive_partitions currently requires filesystem-managed snapshots");
        this.table = table;
        this.target = ArchivePath.parseRoot(targetPath);
        this.catalog = catalog;
        this.owner = checkOwner(table);
        this.tableUuid = table.uuid();
    }

    /** One result per typed, deduplicated full partition, preserving first input order. */
    public synchronized List<String> archive(String partitionSpecs) {
        checkArgument(!invoked, "PartitionArchiver supports a single invocation");
        invoked = true;
        FileStoreTable current = freshTable();
        LinkedHashMap<BinaryRow, String> partitions = parsePartitions(current, partitionSpecs);
        try {
            ArchiveSafety.validate(current, target, validatedHistory);
            ArchiveSafety.validateOwnership(catalog, current, target);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to validate archive deployment", e);
        }
        List<String> results = new ArrayList<>();
        boolean stopped = false;
        long identifier = 0;
        for (Map.Entry<BinaryRow, String> partition : partitions.entrySet()) {
            if (stopped) {
                results.add(partition.getValue() + ": state=NOT_ATTEMPTED");
            } else {
                Outcome outcome = archivePartition(partition.getKey(), identifier);
                results.add(partition.getValue() + ": " + outcome.text);
                stopped = outcome.stop;
            }
            identifier++;
        }
        return results;
    }

    private Outcome archivePartition(BinaryRow partition, long identifier) {
        String pin = "archive-partitions-" + operation + "-" + identifier;
        boolean pinAttempted = false;
        boolean commitAttempted = false;
        boolean revalidating = false;
        Snapshot source = null;
        ArchiveProtection protection = null;
        List<Mapping> mappings = Collections.emptyList();
        List<ManifestEntry> original = Collections.emptyList();
        FileStoreTable current = freshTable();
        try {
            ArchiveSafety.validate(current, target, validatedHistory);
            source = current.snapshotManager().latestSnapshot();
            if (source == null) {
                return new Outcome("state=FAILED code=PARTITION_NOT_FOUND", false);
            }
            original = scan(current, source, partition);
            if (original.isEmpty()) {
                return new Outcome("state=FAILED code=PARTITION_NOT_FOUND", false);
            }
            // Match compact's manifest-derived activity, not directory/object modification times.
            // Creation times survive byte-copy reference replacements.
            if (!partitionIdleTime.isZero()) {
                List<PartitionEntry> entries =
                        current.store()
                                .newScan()
                                .withSnapshot(source)
                                .withKind(ScanMode.ALL)
                                .withPartitionFilter(Collections.singletonList(partition))
                                .readPartitionEntries();
                checkArgument(
                        entries.size() == 1, "Unable to determine archive partition activity");
                long lastActivity = entries.get(0).lastFileCreationTime();
                long cutoff = clock.instant().minus(partitionIdleTime).toEpochMilli();
                if (lastActivity > cutoff) {
                    return new Outcome(
                            "state=NOOP code=PARTITION_ACTIVE last_file_creation_time="
                                    + lastActivity
                                    + " idle_cutoff="
                                    + cutoff
                                    + " data_files=0 extra_files=0",
                            false);
                }
            }
            mappings = plan(current, original);
            if (mappings.isEmpty()) {
                return new Outcome(
                        "state=NOOP data_files=0 extra_files=0 skipped=" + original.size(), false);
            }
            pinAttempted = true;
            current.createTag(pin, source.id(), null);
            checkArgument(
                    current.tagManager().getOrThrow(pin).id() == source.id(),
                    "Unable to confirm archive source pin");
            protection = ArchiveProtection.capture(current, pin, source);
            for (Mapping mapping : mappings) {
                ArchiveFileCopier.copyAndVerify(
                        current.fileIO(),
                        mapping.source,
                        mapping.destination,
                        mapping.before.file().fileSize());
                for (Copy extra : mapping.extras) {
                    ArchiveFileCopier.copyAndVerify(
                            current.fileIO(), extra.source, extra.destination, extra.length);
                }
            }
            revalidating = true;
            FileStoreTable beforeCommit = freshTable();
            ArchiveSafety.validate(beforeCommit, target, validatedHistory);
            ArchiveSafety.validateOwnership(catalog, beforeCommit, target);
            Snapshot latest = beforeCommit.snapshotManager().latestSnapshot();
            if (!protection.unchanged(beforeCommit)
                    || !sameSchema(current, beforeCommit)
                    || latest == null
                    || !sameFiles(original, scan(beforeCommit, latest, partition))) {
                return new Outcome(
                        "state=FAILED code=MAINTENANCE_VIOLATION operation="
                                + operation
                                + " pin_name="
                                + pin
                                + " recovery_required=true",
                        true);
            }

            ManifestCommittable committable = committable(mappings, identifier);
            // Raw FileStoreCommit avoids TableCommitImpl's automatic partition expiration.
            // https://github.com/apache/paimon/blob/release-1.3.2/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java
            try (FileStoreCommit commit =
                    beforeCommit.store().newCommit(commitUser, beforeCommit)) {
                commit.ignoreEmptyCommit(true);
                commitAttempted = true;
                commit.commit(committable, false);
            }
            Outcome confirmed =
                    confirm(
                            current,
                            partition,
                            mappings,
                            original,
                            source,
                            identifier,
                            pin,
                            protection);
            return confirmed.stop
                    ? confirmed
                    : releasePin(current, pin, source, protection, confirmed);
        } catch (Exception failure) {
            if (commitAttempted) {
                try {
                    Outcome confirmed =
                            confirm(
                                    current,
                                    partition,
                                    mappings,
                                    original,
                                    source,
                                    identifier,
                                    pin,
                                    protection);
                    if (confirmed.committedSnapshot == null && isPrePublicationConflict(failure)) {
                        return maintenanceViolation(pin);
                    }
                    return confirmed.stop
                            ? confirmed
                            : releasePin(current, pin, source, protection, confirmed);
                } catch (Exception confirmationFailure) {
                    return unknown(source, pin);
                }
            }
            if (revalidating) {
                return new Outcome(
                        "state=FAILED code=MAINTENANCE_VIOLATION operation="
                                + operation
                                + " pin_name="
                                + pin
                                + " recovery_required=true",
                        true);
            }
            if (pinAttempted && protection == null) {
                return new Outcome(
                        "state=FAILED code=SOURCE_PROTECTION_UNCONFIRMED operation="
                                + operation
                                + " pin_name="
                                + pin
                                + " recovery_required=true",
                        true);
            }
            Outcome failed =
                    new Outcome(
                            "state=FAILED code=COPY_OR_VALIDATION_FAILED operation=" + operation,
                            false);
            // Keep failed fresh objects for coordinated orphan cleanup; never delete source data.
            return pinAttempted ? releasePin(current, pin, source, protection, failed) : failed;
        }
    }

    private Outcome confirm(
            FileStoreTable plannedTable,
            BinaryRow partition,
            List<Mapping> mappings,
            List<ManifestEntry> original,
            Snapshot source,
            long identifier,
            String pin,
            ArchiveProtection protection)
            throws IOException {
        FileStoreTable current = freshTable();
        Snapshot latest = current.snapshotManager().latestSnapshot();
        if (latest == null) {
            return unknown(source, pin);
        }
        // Only snapshots at/below the catalog's authoritative latest are candidates.
        Snapshot published = null;
        java.util.Iterator<Snapshot> snapshots = current.snapshotManager().snapshots();
        while (snapshots.hasNext()) {
            Snapshot candidate = snapshots.next();
            if (candidate.id() <= latest.id()
                    && candidate.commitUser().equals(commitUser)
                    && candidate.commitIdentifier() == identifier
                    && candidate.commitKind() == Snapshot.CommitKind.COMPACT) {
                published = candidate;
            }
        }
        if (published == null) {
            return unknown(source, pin);
        }
        List<ManifestEntry> expected = new ArrayList<>(original);
        for (Mapping mapping : mappings) {
            expected.remove(mapping.before);
            expected.add(mapping.afterEntry());
        }
        List<ManifestEntry> publishedFiles = scan(current, published, partition);
        List<ManifestEntry> copied = new ArrayList<>();
        for (Mapping mapping : mappings) {
            copied.add(mapping.afterEntry());
        }
        if (!publishedFiles.containsAll(copied)) {
            return unknown(source, pin);
        }
        List<ManifestEntry> live = scan(current, latest, partition);
        if (!sameFiles(expected, publishedFiles)
                || !sameFiles(expected, live)
                || !sameSchema(plannedTable, current)
                || protection == null
                || !protectionUnchanged(protection, current)) {
            return incomplete(published.id(), pin);
        }
        for (ManifestEntry entry : live) {
            checkReference(current, entry);
        }
        long dataBytes = 0;
        long extraBytes = 0;
        int extras = 0;
        for (Mapping mapping : mappings) {
            dataBytes = Math.addExact(dataBytes, mapping.before.file().fileSize());
            for (Copy extra : mapping.extras) {
                extras++;
                extraBytes = Math.addExact(extraBytes, extra.length);
            }
        }
        return new Outcome(
                "state=ARCHIVED operation="
                        + operation
                        + " source_snapshot="
                        + source.id()
                        + " committed_snapshot="
                        + published.id()
                        + " data_files="
                        + mappings.size()
                        + " extra_files="
                        + extras
                        + " data_bytes="
                        + dataBytes
                        + " extra_bytes="
                        + extraBytes
                        + " skipped="
                        + (original.size() - mappings.size()),
                false,
                published.id());
    }

    private Outcome unknown(Snapshot source, String pin) {
        return new Outcome(
                "state=UNKNOWN operation="
                        + operation
                        + " source_snapshot="
                        + (source == null ? "unknown" : source.id())
                        + " pin_name="
                        + pin
                        + " recovery_required=true",
                true);
    }

    private Outcome releasePin(
            FileStoreTable current,
            String pin,
            Snapshot source,
            ArchiveProtection protection,
            Outcome outcome) {
        if (protection != null && !protectionUnchanged(protection, current)) {
            return outcome.committedSnapshot == null
                    ? maintenanceViolation(pin)
                    : incomplete(outcome.committedSnapshot, pin);
        }
        boolean deletionAttempted = false;
        try {
            if (!current.tagManager().tagExists(pin)
                    || !ArchiveProtection.pinMatches(current, pin, source)) {
                return outcome.committedSnapshot == null
                        ? maintenanceViolation(pin)
                        : incomplete(outcome.committedSnapshot, pin);
            }
            deletionAttempted = true;
            current.deleteTag(pin);
            if (!current.tagManager().tagExists(pin)) {
                return outcome;
            }
        } catch (Exception ignored) {
            if (!deletionAttempted) {
                return outcome.committedSnapshot == null
                        ? maintenanceViolation(pin)
                        : incomplete(outcome.committedSnapshot, pin);
            }
            // A failed response may follow a successful delete. Do not overwrite publication state.
            try {
                if (!current.tagManager().tagExists(pin)) {
                    return outcome;
                }
            } catch (Exception confirmationFailure) {
                // Retain an explicit cleanup requirement.
            }
        }
        return new Outcome(
                outcome.text + " pin_name=" + pin + " cleanup_required=true",
                outcome.stop,
                outcome.committedSnapshot);
    }

    private static boolean protectionUnchanged(ArchiveProtection protection, FileStoreTable table) {
        try {
            return protection != null && protection.unchanged(table);
        } catch (Exception unavailable) {
            // Publication may be proven even when protection cannot be verified. Preserve its
            // identity and stop recovery; a failed protection read is not a successful pin delete.
            return false;
        }
    }

    private Outcome maintenanceViolation(String pin) {
        return new Outcome(
                "state=FAILED code=MAINTENANCE_VIOLATION operation="
                        + operation
                        + " pin_name="
                        + pin
                        + " recovery_required=true",
                true);
    }

    private Outcome incomplete(long snapshot, String pin) {
        return new Outcome(
                "state=INCOMPLETE operation="
                        + operation
                        + " committed_snapshot="
                        + snapshot
                        + " pin_name="
                        + pin
                        + " maintenance_violation=true recovery_required=true",
                true,
                snapshot);
    }

    private static boolean isPrePublicationConflict(Exception failure) {
        // Release 1.3.2 has no typed file-conflict exception. Require both the native origin and
        // its fixed conflict prefix; never classify arbitrary FileIO exception text as evidence.
        StackTraceElement[] trace = failure.getStackTrace();
        String message = failure.getMessage();
        return failure.getClass() == RuntimeException.class
                && trace.length > 0
                && trace[0].getClassName().equals(FileStoreCommitImpl.class.getName())
                && trace[0].getMethodName().equals("createConflictException")
                && message != null
                && (message.startsWith("File deletion conflicts detected!")
                        || message.startsWith("LSM conflicts detected!"));
    }

    private List<Mapping> plan(FileStoreTable current, List<ManifestEntry> files)
            throws IOException {
        checkArgument(files.size() <= ArchiveSafety.MAX_FILES, "Archive file budget exceeded");
        long bytes = 0;
        List<Mapping> mappings = new ArrayList<>();
        for (ManifestEntry entry : files) {
            ArchiveSafety.validateFile(entry.file());
            checkReference(current, entry);
            DataFilePathFactory paths =
                    current.store()
                            .pathFactory()
                            .createDataFilePathFactory(entry.partition(), entry.bucket());
            Path source = paths.toPath(entry.file());
            if (ArchiveSafety.contains(target, source)) {
                continue;
            }
            String name =
                    current.store()
                                    .pathFactory()
                                    .createDataFilePathFactory(entry.partition(), entry.bucket())
                                    .dataFilePrefix()
                            + UUID.randomUUID()
                            + "-"
                            + entry.file().fileName();
            Path destination =
                    new Path(
                            new Path(
                                    target,
                                    current.store()
                                            .pathFactory()
                                            .relativeBucketPath(entry.partition(), entry.bucket())),
                            name);
            checkArgument(
                    ArchiveSafety.contains(target, destination),
                    "Archive destination escapes registered target");
            List<Copy> extras = new ArrayList<>();
            List<String> extraNames = new ArrayList<>();
            for (String extra : entry.file().extraFiles()) {
                Path extraSource = paths.toAlignedPath(extra, entry.file());
                // Recognize the referenced file-index header before copying.
                try (SeekableInputStream input = current.fileIO().newInputStream(extraSource)) {
                    input.seek(12);
                    int headerLength = new DataInputStream(input).readInt();
                    checkArgument(
                            headerLength >= 16
                                    && headerLength <= (1 << 20)
                                    && headerLength
                                            <= current.fileIO().getFileStatus(extraSource).getLen(),
                            "Archive index header exceeds bounds");
                    input.seek(0);
                    try (FileIndexFormat.Reader reader =
                            FileIndexFormat.createReader(
                                    input,
                                    current.schemaManager()
                                            .schema(entry.file().schemaId())
                                            .logicalRowType())) {
                        // Metadata is the authoritative name; the bounded reader validates its
                        // header.
                    }
                }
                String extraName = name + "-" + UUID.randomUUID() + ".index";
                long length = current.fileIO().getFileStatus(extraSource).getLen();
                extras.add(
                        new Copy(
                                extraSource, new Path(destination.getParent(), extraName), length));
                extraNames.add(extraName);
                bytes = Math.addExact(bytes, length);
            }
            bytes = Math.addExact(bytes, entry.file().fileSize());
            checkArgument(bytes <= ArchiveSafety.MAX_BYTES, "Archive byte budget exceeded");
            DataFileMeta after =
                    entry.file()
                            .rename(name)
                            .copy(extraNames)
                            .newExternalPath(destination.toString());
            checkArgument(
                    paths.toPath(after).equals(destination),
                    "Archive data path cannot round-trip through reader metadata");
            for (int i = 0; i < extras.size(); i++) {
                checkArgument(
                        paths.toAlignedPath(extraNames.get(i), after)
                                .equals(extras.get(i).destination),
                        "Archive index path cannot round-trip through reader metadata");
            }
            mappings.add(new Mapping(entry, source, destination, after, extras));
        }
        return mappings;
    }

    private static void checkReference(FileStoreTable table, ManifestEntry entry)
            throws IOException {
        DataFilePathFactory paths =
                table.store()
                        .pathFactory()
                        .createDataFilePathFactory(entry.partition(), entry.bucket());
        checkArgument(
                table.fileIO().getFileStatus(paths.toPath(entry.file())).getLen()
                        == entry.file().fileSize(),
                "Archive referenced data length mismatch");
        for (String extra : entry.file().extraFiles()) {
            checkArgument(
                    !table.fileIO().getFileStatus(paths.toAlignedPath(extra, entry.file())).isDir(),
                    "Archive referenced index must be a file");
        }
    }

    private static ManifestCommittable committable(List<Mapping> mappings, long identifier) {
        Map<Integer, List<Mapping>> buckets = new LinkedHashMap<>();
        for (Mapping mapping : mappings) {
            buckets.computeIfAbsent(mapping.before.bucket(), b -> new ArrayList<>()).add(mapping);
        }
        ManifestCommittable committable = new ManifestCommittable(identifier);
        for (List<Mapping> bucket : buckets.values()) {
            ManifestEntry first = bucket.get(0).before;
            List<DataFileMeta> before = new ArrayList<>();
            List<DataFileMeta> after = new ArrayList<>();
            for (Mapping mapping : bucket) {
                checkArgument(
                        mapping.before.totalBuckets() == first.totalBuckets(),
                        "Archive bucket totalBuckets mismatch");
                before.add(mapping.before.file());
                after.add(mapping.after);
            }
            committable.addFileCommittable(
                    new CommitMessageImpl(
                            first.partition(),
                            first.bucket(),
                            first.totalBuckets(),
                            DataIncrement.emptyIncrement(),
                            new CompactIncrement(before, after, Collections.emptyList())));
        }
        return committable;
    }

    private FileStoreTable freshTable() {
        final org.apache.paimon.table.Table loaded;
        try {
            catalog.invalidateTable(owner);
            loaded = catalog.getTable(owner);
        } catch (Catalog.TableNotExistException e) {
            throw new IllegalArgumentException("Archive table no longer exists", e);
        }
        checkArgument(loaded instanceof FileStoreTable, "Archive requires a FileStoreTable");
        FileStoreTable actual = (FileStoreTable) loaded;
        checkArgument(
                tableUuid.equals(actual.uuid()) && table.location().equals(actual.location()),
                "Archive table identity changed");
        checkOwner(actual);
        FileStoreTable fresh = actual.copy(actual.schema());
        fresh.setSnapshotCache(null);
        fresh.setManifestCache(null);
        return fresh;
    }

    private static List<ManifestEntry> scan(
            FileStoreTable table, Snapshot snapshot, BinaryRow partition) throws IOException {
        ArchiveSafety.validateScanBudget(table, snapshot);
        java.util.Iterator<ManifestEntry> iterator =
                table.store()
                        .newScan()
                        .withSnapshot(snapshot)
                        .withKind(ScanMode.ALL)
                        .keepStats()
                        .withPartitionFilter(Collections.singletonList(partition))
                        .readFileIterator();
        List<ManifestEntry> files = new ArrayList<>();
        while (iterator.hasNext()) {
            checkArgument(files.size() < ArchiveSafety.MAX_FILES, "Archive file budget exceeded");
            files.add(iterator.next());
        }
        return files;
    }

    private static Identifier checkOwner(FileStoreTable table) {
        checkArgument(
                "main".equals(table.snapshotManager().branch()), "Archive requires main branch");
        checkArgument(
                table.getClass() == org.apache.paimon.table.AppendOnlyFileStoreTable.class
                        || table.getClass()
                                == org.apache.paimon.table.PrimaryKeyFileStoreTable.class,
                "Archive requires an ordinary FileStoreTable");
        Identifier id = table.catalogEnvironment().identifier();
        checkArgument(id != null, "Archive requires catalog table identity");
        return id;
    }

    private static boolean sameSchema(FileStoreTable left, FileStoreTable right) {
        return left.schema().id() == right.schema().id()
                && left.uuid().equals(right.uuid())
                && left.schema().fields().equals(right.schema().fields())
                && left.primaryKeys().equals(right.primaryKeys())
                && left.partitionKeys().equals(right.partitionKeys())
                && left.options().equals(right.options());
    }

    private static boolean sameFiles(List<ManifestEntry> left, List<ManifestEntry> right) {
        return left.size() == right.size() && new HashSet<>(left).equals(new HashSet<>(right));
    }

    private static LinkedHashMap<BinaryRow, String> parsePartitions(
            FileStoreTable table, String text) {
        checkArgument(
                text != null && !text.trim().isEmpty(), "Archive partitions must not be empty");
        String[] specifications = text.split(";", -1);
        checkArgument(
                specifications.length <= ArchiveSafety.MAX_HISTORY,
                "Archive partition budget exceeded");
        LinkedHashMap<BinaryRow, String> partitions = new LinkedHashMap<>();
        InternalRowSerializer serializer =
                new InternalRowSerializer(table.schema().logicalPartitionType());
        Set<String> keys = new HashSet<>(table.partitionKeys());
        for (String specification : specifications) {
            Map<String, String> spec = new LinkedHashMap<>();
            for (String field : specification.split(",", -1)) {
                String[] pair = field.split("=", 2);
                checkArgument(
                        pair.length == 2 && !pair[1].trim().isEmpty(),
                        "Archive requires key=value full partitions");
                String key = pair[0].trim();
                String value = pair[1].trim();
                checkArgument(!spec.containsKey(key), "Archive duplicate partition key");
                checkArgument(
                        value.chars().noneMatch(Character::isISOControl),
                        "Archive partition values must not contain control characters");
                spec.put(key, value);
            }
            checkArgument(
                    spec.keySet().equals(keys),
                    "Archive requires all and only the table partition keys");
            BinaryRow partition =
                    serializer
                            .toBinaryRow(
                                    InternalRowPartitionComputer.convertSpecToInternalRow(
                                            spec,
                                            table.schema().logicalPartitionType(),
                                            CoreOptions.fromMap(table.options())
                                                    .partitionDefaultName()))
                            .copy();
            partitions.putIfAbsent(partition, specification.trim());
        }
        return partitions;
    }

    private static class Outcome {
        final String text;
        final boolean stop;
        final Long committedSnapshot;

        Outcome(String text, boolean stop) {
            this(text, stop, null);
        }

        Outcome(String text, boolean stop, Long committedSnapshot) {
            this.text = text;
            this.stop = stop;
            this.committedSnapshot = committedSnapshot;
        }
    }

    private static class Copy {
        final Path source;
        final Path destination;
        final long length;

        Copy(Path source, Path destination, long length) {
            this.source = source;
            this.destination = destination;
            this.length = length;
        }
    }

    private static class Mapping {
        final ManifestEntry before;
        final Path source;
        final Path destination;
        final DataFileMeta after;
        final List<Copy> extras;

        Mapping(
                ManifestEntry before,
                Path source,
                Path destination,
                DataFileMeta after,
                List<Copy> extras) {
            this.before = before;
            this.source = source;
            this.destination = destination;
            this.after = after;
            this.extras = extras;
        }

        ManifestEntry afterEntry() {
            return ManifestEntry.create(
                    before.kind(),
                    before.partition(),
                    before.bucket(),
                    before.totalBuckets(),
                    after);
        }
    }
}
