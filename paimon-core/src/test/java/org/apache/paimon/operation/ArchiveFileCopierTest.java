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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Byte copy, content verification and destination ownership tests. */
class ArchiveFileCopierTest {

    @TempDir java.nio.file.Path temp;

    @ParameterizedTest
    @ValueSource(ints = {0, 65535, 65536, 65537, 131089})
    void copiesExactBytesWithoutRemovingSource(int size) throws Exception {
        FileIO io = LocalFileIO.create();
        Path source = new Path(temp.resolve("source").toUri());
        Path target = new Path(temp.resolve("target").toUri());
        byte[] payload = new byte[size];
        new Random(73).nextBytes(payload);
        try (OutputStream out = io.newOutputStream(source, false)) {
            out.write(payload);
        }
        ArchiveFileCopier.copyAndVerify(io, source, target, size);
        assertThat(io.exists(source)).isTrue();
        assertThat(io.newInputStream(target))
                .satisfies(
                        input -> {
                            try (java.io.InputStream in = input) {
                                assertThat(in.readAllBytes()).isEqualTo(payload);
                            } catch (java.io.IOException e) {
                                throw new java.io.UncheckedIOException(e);
                            }
                        });
    }

    @Test
    void rejectsSameLengthContentCorruption() throws Exception {
        Path source = new Path(temp.resolve("source").toUri());
        Path target = new Path(temp.resolve("target").toUri());
        LocalFileIO io =
                new LocalFileIO() {
                    @Override
                    public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                            throws IOException {
                        PositionOutputStream out = super.newOutputStream(path, overwrite);
                        if (!path.equals(target)) {
                            return out;
                        }
                        return new PositionOutputStream() {
                            @Override
                            public long getPos() throws IOException {
                                return out.getPos();
                            }

                            @Override
                            public void write(int b) throws IOException {
                                out.write(b ^ 1);
                            }

                            @Override
                            public void write(byte[] b) throws IOException {
                                write(b, 0, b.length);
                            }

                            @Override
                            public void write(byte[] b, int off, int len) throws IOException {
                                byte[] changed = java.util.Arrays.copyOfRange(b, off, off + len);
                                changed[0] ^= 1;
                                out.write(changed);
                            }

                            @Override
                            public void flush() throws IOException {
                                out.flush();
                            }

                            @Override
                            public void close() throws IOException {
                                out.close();
                            }
                        };
                    }
                };
        try (OutputStream out = io.newOutputStream(source, false)) {
            out.write(new byte[] {1, 2, 3});
        }
        assertThatThrownBy(() -> ArchiveFileCopier.copyAndVerify(io, source, target, 3))
                .hasMessageContaining("SHA-256");
        assertThat(io.getFileStatus(target).getLen()).isEqualTo(3);
        assertThat(io.exists(source)).isTrue();
    }

    @Test
    void refusesCollisionAndWrongSourceLength() throws Exception {
        FileIO io = LocalFileIO.create();
        Path source = new Path(temp.resolve("source").toUri());
        Path target = new Path(temp.resolve("target").toUri());
        try (OutputStream out = io.newOutputStream(source, false)) {
            out.write(new byte[] {1, 2, 3});
        }
        assertThatThrownBy(() -> ArchiveFileCopier.copyAndVerify(io, source, target, 4))
                .hasMessageContaining("length");
        assertThat(io.exists(target)).isFalse();
        try (OutputStream out = io.newOutputStream(target, false)) {
            out.write(7);
        }
        assertThatThrownBy(() -> ArchiveFileCopier.copyAndVerify(io, source, target, 3))
                .hasMessageContaining("exists");
        try (java.io.InputStream in = io.newInputStream(target)) {
            assertThat(in.readAllBytes()).containsExactly((byte) 7);
        }
    }
}
