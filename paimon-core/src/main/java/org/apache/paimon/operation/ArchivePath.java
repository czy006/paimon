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

import org.apache.paimon.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Lexical URI policy for archive roots, not a proof of filesystem ownership or symlink identity.
 *
 * <p>Inspect components before Paimon's Path normalizes them. Use Path(URI) to preserve escaped
 * characters; Path(String) interprets its input as unescaped text. URI component/escaping
 * semantics: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/net/URI.html
 */
final class ArchivePath {

    private ArchivePath() {}

    static Path parseRoot(String location) {
        if (location == null) {
            throw invalidRoot();
        }
        try {
            URI uri = new URI(location);
            if (!uri.isAbsolute() || !isSafePath(uri) || uri.getPath().equals("/")) {
                throw invalidRoot();
            }
            String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
            if ("file".equals(scheme)) {
                // Local authority aliases cannot establish an exclusive filesystem root.
                if (uri.getRawAuthority() != null) {
                    throw invalidRoot();
                }
            } else if (uri.getHost() == null
                    || uri.getRawAuthority().contains("%")
                    || uri.getRawAuthority().contains("@")) {
                throw invalidRoot();
            }
            String path = uri.getPath();
            if (path.contains("%")) {
                // Do not let a later decoder reinterpret nested escapes in the registered root.
                throw invalidRoot();
            }
            if (path.endsWith("/")) {
                path = path.substring(0, path.length() - 1);
            }
            String host = uri.getHost();
            return new Path(
                    new URI(
                            scheme,
                            null,
                            host == null ? null : host.toLowerCase(Locale.ROOT),
                            uri.getPort(),
                            path,
                            null,
                            null));
        } catch (URISyntaxException ignored) {
            // The input and the syntax exception can contain credentials.
            throw invalidRoot();
        }
    }

    /** Roots must have been validated with parseRoot; candidate paths come from file metadata. */
    static boolean contains(Path root, Path candidate) {
        URI parent = root.toUri();
        URI child = candidate.toUri();
        if (child.getScheme() == null
                || !parent.getScheme().equalsIgnoreCase(child.getScheme())
                || !sameAuthority(parent, child)
                || !isSafePath(child)) {
            return false;
        }
        String[] parentSegments = parent.getPath().split("/");
        String[] childSegments = child.getPath().split("/");
        if (parentSegments.length > childSegments.length) {
            return false;
        }
        for (int i = 0; i < parentSegments.length; i++) {
            if (!parentSegments[i].equals(childSegments[i])) {
                return false;
            }
        }
        return true;
    }

    static boolean overlaps(Path first, Path second) {
        return contains(first, second) || contains(second, first);
    }

    private static boolean sameAuthority(URI first, URI second) {
        if (first.getRawAuthority() == null || second.getRawAuthority() == null) {
            return first.getRawAuthority() == null && second.getRawAuthority() == null;
        }
        return first.getHost() != null
                && second.getHost() != null
                && first.getHost().equalsIgnoreCase(second.getHost())
                && first.getPort() == second.getPort();
    }

    private static boolean isSafePath(URI uri) {
        String path = uri.getPath();
        if (uri.isOpaque()
                || uri.getRawQuery() != null
                || uri.getRawFragment() != null
                || uri.getRawUserInfo() != null
                || !hasValidUtf8Escapes(uri.getRawPath())
                || path == null
                || !path.startsWith("/")
                || path.contains("//")
                || path.contains("\\")) {
            return false;
        }
        String rawPath = uri.getRawPath().toLowerCase(Locale.ROOT);
        if (rawPath.contains("%2f") || rawPath.contains("%5c")) {
            return false;
        }
        for (String segment : path.split("/")) {
            if (".".equals(segment) || "..".equals(segment)) {
                return false;
            }
        }
        for (int i = 0; i < path.length(); i++) {
            char character = path.charAt(i);
            if (Character.isISOControl(character) || Character.isLowSurrogate(character)) {
                return false;
            }
            if (Character.isHighSurrogate(character)) {
                if (i + 1 == path.length() || !Character.isLowSurrogate(path.charAt(i + 1))) {
                    return false;
                }
                i++;
            }
        }
        return true;
    }

    private static boolean hasValidUtf8Escapes(String rawPath) {
        if (rawPath == null) {
            return false;
        }
        for (int start = 0; start < rawPath.length(); ) {
            if (rawPath.charAt(start) != '%') {
                start++;
                continue;
            }
            int end = start;
            // URI has already checked the syntax of every %XX escape.
            while (end < rawPath.length() && rawPath.charAt(end) == '%') {
                end += 3;
            }
            byte[] bytes = new byte[(end - start) / 3];
            for (int i = 0; i < bytes.length; i++) {
                int offset = start + i * 3;
                bytes[i] =
                        (byte)
                                ((Character.digit(rawPath.charAt(offset + 1), 16) << 4)
                                        | Character.digit(rawPath.charAt(offset + 2), 16));
            }
            try {
                // URI.getPath replaces invalid UTF-8. A replacement would change the destination.
                StandardCharsets.UTF_8
                        .newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(bytes));
            } catch (CharacterCodingException ignored) {
                return false;
            }
            start = end;
        }
        return true;
    }

    private static IllegalArgumentException invalidRoot() {
        return new IllegalArgumentException("Invalid archive storage root URI");
    }
}
