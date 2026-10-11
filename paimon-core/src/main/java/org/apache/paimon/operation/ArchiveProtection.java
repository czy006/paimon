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

import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.tag.Tag;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Captures the protected history, including the operation's exact no-TTL source pin. */
final class ArchiveProtection {

    private final Map<Long, Snapshot> snapshots;
    private final Map<String, Tag> tags;
    private final String pin;
    private final Snapshot source;

    private ArchiveProtection(
            Map<Long, Snapshot> snapshots, Map<String, Tag> tags, String pin, Snapshot source) {
        this.snapshots = snapshots;
        this.tags = tags;
        this.pin = pin;
        this.source = source;
    }

    static ArchiveProtection capture(FileStoreTable table, String pin, Snapshot source)
            throws IOException {
        checkArgument(
                table.snapshotManager().snapshotCount() <= ArchiveSafety.MAX_HISTORY,
                "Archive protected snapshot budget exceeded");
        Map<Long, Snapshot> snapshots = new HashMap<>();
        Iterator<Snapshot> iterator = table.snapshotManager().snapshots();
        while (iterator.hasNext()) {
            checkArgument(
                    snapshots.size() < ArchiveSafety.MAX_HISTORY,
                    "Archive protected snapshot budget exceeded");
            Snapshot snapshot = iterator.next();
            snapshots.put(snapshot.id(), snapshot);
        }
        checkArgument(
                source.equals(snapshots.get(source.id())),
                "Archive source snapshot disappeared before copying");
        Map<String, Tag> captured = tags(table);
        checkArgument(
                pinMatches(captured.get(pin), source),
                "Archive source pin is missing or changed in the captured protection");
        return new ArchiveProtection(snapshots, captured, pin, source);
    }

    boolean unchanged(FileStoreTable table) throws IOException {
        for (Snapshot snapshot : snapshots.values()) {
            if (!table.snapshotManager().snapshotExists(snapshot.id())
                    || !snapshot.equals(table.snapshotManager().snapshot(snapshot.id()))) {
                return false;
            }
        }
        // New ordinary snapshots are allowed; tag additions/removals/retargeting violate the
        // window.
        Map<String, Tag> current = tags(table);
        return pinMatches(current.get(pin), source) && tags.equals(current);
    }

    static boolean pinMatches(FileStoreTable table, String pin, Snapshot source) {
        Optional<Tag> actual = table.tagManager().get(pin);
        return actual.isPresent() && pinMatches(actual.get(), source);
    }

    private static boolean pinMatches(Tag tag, Snapshot source) {
        return tag != null
                && tag.getTagTimeRetained() == null
                && source.equals(tag.trimToSnapshot());
    }

    private static Map<String, Tag> tags(FileStoreTable table) {
        checkArgument(
                table.tagManager().tagCount() <= ArchiveSafety.MAX_HISTORY,
                "Archive protected tag budget exceeded");
        Map<String, Tag> tags = new HashMap<>();
        for (String name : table.tagManager().allTagNames()) {
            checkArgument(
                    tags.size() < ArchiveSafety.MAX_HISTORY,
                    "Archive protected tag budget exceeded");
            tags.put(name, table.tagManager().getOrThrow(name));
        }
        return tags;
    }
}
