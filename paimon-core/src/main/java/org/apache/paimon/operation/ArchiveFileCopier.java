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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Copy and verify an immutable archive object. */
final class ArchiveFileCopier {

    private static final int BUFFER_SIZE = 64 * 1024;

    private ArchiveFileCopier() {}

    static void copyAndVerify(FileIO io, Path source, Path destination, long expectedLength)
            throws IOException {
        if (expectedLength < 0 || io.getFileStatus(source).getLen() != expectedLength) {
            throw new IOException("Archive source length does not match metadata");
        }
        if (io.exists(destination)) {
            throw new IOException("Archive destination already exists");
        }
        io.mkdirs(destination.getParent());
        MessageDigest sourceDigest = digest();
        try (InputStream in = io.newInputStream(source);
                OutputStream out = io.newOutputStream(destination, false)) {
            transfer(in, out, sourceDigest, expectedLength);
        }
        if (io.getFileStatus(destination).getLen() != expectedLength) {
            throw new IOException("Archive destination length mismatch after close");
        }
        MessageDigest targetDigest = digest();
        try (InputStream in = io.newInputStream(destination)) {
            transfer(in, null, targetDigest, expectedLength);
        }
        if (!Arrays.equals(sourceDigest.digest(), targetDigest.digest())) {
            throw new IOException("Archive destination SHA-256 mismatch");
        }
    }

    private static void transfer(
            InputStream in, OutputStream out, MessageDigest digest, long expectedLength)
            throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        long count = 0;
        int read;
        while ((read = in.read(buffer)) != -1) {
            count = Math.addExact(count, read);
            if (count > expectedLength) {
                throw new IOException("Archive stream exceeds expected length");
            }
            digest.update(buffer, 0, read);
            if (out != null) {
                out.write(buffer, 0, read);
            }
        }
        if (count != expectedLength) {
            throw new IOException("Archive stream length mismatch");
        }
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", e);
        }
    }
}
