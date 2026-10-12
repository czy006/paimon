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

import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyPartResult;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectAclRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.TaggingDirective;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;
import software.amazon.awssdk.utils.DateUtils;

import java.io.IOException;
import java.net.URLDecoder;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Copy-service model verifies destination attributes, including S3's metadata COPY defaults. */
class S3NativeCopyAttributesTest {

    private static final Path SOURCE = S3NativeFileIORenameTest.SOURCE;
    private static final Path DESTINATION = S3NativeFileIORenameTest.DESTINATION;
    private static final Instant EXPIRES = Instant.parse("2027-01-01T00:00:00Z");

    private S3Client client;
    private final Map<String, HeadObjectResponse> sources = new HashMap<>();
    private final Map<String, HeadObjectResponse> destinations = new HashMap<>();
    private final Map<String, Map<String, String>> destinationTags = new HashMap<>();
    private final Map<String, ObjectCannedACL> destinationAcls = new HashMap<>();
    private final AtomicReference<CreateMultipartUploadRequest> create = new AtomicReference<>();

    @BeforeEach
    void setup() {
        client = mock(S3Client.class);
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenReturn(DeleteObjectsResponse.builder().build());
        when(client.headObject(any(HeadObjectRequest.class)))
                .thenAnswer(
                        invocation -> {
                            String key = ((HeadObjectRequest) invocation.getArgument(0)).key();
                            HeadObjectResponse head = sources.get(key);
                            if (head == null) {
                                head = destinations.get(key);
                            }
                            if (head == null) {
                                throw NoSuchKeyException.builder().statusCode(404).build();
                            }
                            return head;
                        });
        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(ListObjectsV2Response.builder().build());
        when(client.getObjectTagging(any(GetObjectTaggingRequest.class)))
                .thenReturn(
                        GetObjectTaggingResponse.builder()
                                .tagSet(
                                        Tag.builder().key("source-only").value("a b+c/值").build(),
                                        Tag.builder().key("shared").value("source").build())
                                .build());
        when(client.copyObject(any(CopyObjectRequest.class)))
                .thenAnswer(
                        invocation -> {
                            CopyObjectRequest request = invocation.getArgument(0);
                            HeadObjectResponse source = sources.get(request.sourceKey());
                            // S3 copies metadata by default, but never copies the redirect header.
                            HeadObjectResponse.Builder head =
                                    request.metadataDirective() == MetadataDirective.REPLACE
                                            ? HeadObjectResponse.builder()
                                                    .metadata(request.metadata())
                                                    .contentType(request.contentType())
                                                    .cacheControl(request.cacheControl())
                                                    .contentEncoding(request.contentEncoding())
                                                    .contentLanguage(request.contentLanguage())
                                                    .contentDisposition(
                                                            request.contentDisposition())
                                                    .expiresString(
                                                            request.expires() == null
                                                                    ? null
                                                                    : DateUtils.formatRfc822Date(
                                                                            request.expires()))
                                            : source.toBuilder();
                            destinations.put(
                                    request.destinationKey(),
                                    head.websiteRedirectLocation(request.websiteRedirectLocation())
                                            .storageClass(storageClass(request.storageClass()))
                                            .build());
                            destinationTags.put(
                                    request.destinationKey(),
                                    request.taggingDirective() == TaggingDirective.REPLACE
                                            ? decodeTags(request.tagging())
                                            : sourceTags());
                            destinationAcls.put(request.destinationKey(), acl(request.acl()));
                            return null;
                        });
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenAnswer(
                        invocation -> {
                            create.set(invocation.getArgument(0));
                            return CreateMultipartUploadResponse.builder().uploadId("copy").build();
                        });
        when(client.uploadPartCopy(any(UploadPartCopyRequest.class)))
                .thenReturn(
                        UploadPartCopyResponse.builder()
                                .copyPartResult(CopyPartResult.builder().eTag("part").build())
                                .build());
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenAnswer(
                        invocation -> {
                            recordMultipartDestination();
                            return null;
                        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRenamePreservesSourceAttributes(boolean multipart) throws Exception {
        sources.put("source", sourceHead(size(multipart)));
        assertThat(
                        S3NativeFileIORenameTest.fileIO(client, new Options())
                                .rename(SOURCE, DESTINATION))
                .isTrue();
        assertPreservedAttributes(destinations.get("destination"));
        assertThat(destinationTags.get("destination")).isEqualTo(sourceTags());
        assertThat(destinationAcls.get("destination")).isEqualTo(ObjectCannedACL.PRIVATE);
        verify(client, never()).getObjectAcl(any(GetObjectAclRequest.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConfiguredAttributesOverlaySource(boolean multipart) throws Exception {
        sources.put("source", sourceHead(size(multipart)));
        Options options = configuredAttributes();
        assertThat(S3NativeFileIORenameTest.fileIO(client, options).rename(SOURCE, DESTINATION))
                .isTrue();
        assertThat(destinations.get("destination").metadata()).containsEntry("custom", "value");
        assertThat(destinations.get("destination").storageClass())
                .isEqualTo(StorageClass.ONEZONE_IA);
        assertThat(destinationTags.get("destination"))
                .containsEntry("source-only", "a b+c/值")
                .containsEntry("shared", "configured")
                .containsEntry("added", "new");
        assertThat(destinationAcls.get("destination"))
                .isEqualTo(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL);
        verify(client, never()).getObjectAcl(any(GetObjectAclRequest.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTaggingPermissionFailurePreservesSource(boolean multipart) throws Exception {
        sources.put("source", sourceHead(size(multipart)));
        when(client.getObjectTagging(any(GetObjectTaggingRequest.class)))
                .thenThrow(S3Exception.builder().statusCode(403).message("AccessDenied").build());
        assertThatThrownBy(
                        () ->
                                S3NativeFileIORenameTest.fileIO(client, new Options())
                                        .rename(SOURCE, DESTINATION))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("tags");
        assertNoCopyOrDelete();
    }

    @Test
    void testMultipartRecoveryUsesFreshTokenWhilePreservingMetadata() throws Exception {
        sources.put("source", sourceHead(size(true)));
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenAnswer(
                        invocation -> {
                            recordMultipartDestination();
                            throw NoSuchUploadException.builder().statusCode(404).build();
                        });
        assertThat(
                        S3NativeFileIORenameTest.fileIO(client, new Options())
                                .rename(SOURCE, DESTINATION))
                .isTrue();
        assertThat(create.get().metadata())
                .containsEntry("custom", "value")
                .containsKey("paimon-upload-id");
        assertThat(create.get().metadata().get("paimon-upload-id"))
                .isNotEmpty()
                .isNotEqualTo("previous-upload-token");
        verify(client).deleteObject(any(DeleteObjectRequest.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "0"})
    void testMultipartCopyPreservesRawExpiresHeader(String expires) throws Exception {
        sources.put("source", sourceHead(size(true)).toBuilder().expiresString(expires).build());
        assertThat(
                        S3NativeFileIORenameTest.fileIO(client, new Options())
                                .rename(SOURCE, DESTINATION))
                .isTrue();
        assertThat(create.get().overrideConfiguration().get().headers().get("Expires"))
                .containsExactly(expires);
        assertThat(destinations.get("destination").expiresString()).isEqualTo(expires);
    }

    @Test
    void testDirectoryCopyUsesHeadForEachObjectIncludingMarker() throws Exception {
        directoryListing();
        sources.put(
                "source/",
                sourceHead(0)
                        .toBuilder()
                        .metadata(Collections.singletonMap("kind", "marker"))
                        .build());
        sources.put("source/file", sourceHead(size(true)));
        assertThat(
                        S3NativeFileIORenameTest.fileIO(client, new Options())
                                .rename(SOURCE, DESTINATION))
                .isTrue();
        assertThat(destinations.get("destination/").metadata()).containsEntry("kind", "marker");
        assertThat(destinations.get("destination/").storageClass())
                .isEqualTo(StorageClass.STANDARD_IA);
        assertPreservedAttributes(destinations.get("destination/file"));
        assertThat(create.get().key()).isEqualTo("destination/file");
    }

    @Test
    void testDirectoryPreflightsAllTagPermissionsBeforeAnyCopy() throws Exception {
        directoryListing();
        sources.put("source/", sourceHead(0));
        sources.put("source/file", sourceHead(4));
        when(client.getObjectTagging(any(GetObjectTaggingRequest.class)))
                .thenAnswer(
                        invocation -> {
                            GetObjectTaggingRequest request = invocation.getArgument(0);
                            if (request.key().equals("source/file")) {
                                throw S3Exception.builder().statusCode(403).build();
                            }
                            return GetObjectTaggingResponse.builder().build();
                        });
        assertThatThrownBy(
                        () ->
                                S3NativeFileIORenameTest.fileIO(client, new Options())
                                        .rename(SOURCE, DESTINATION))
                .isInstanceOf(IOException.class);
        assertNoCopyOrDelete();
    }

    @Test
    void testDirectoryRejectsTooManyMergedTagsBeforeAnyCopy() throws Exception {
        directoryListing();
        sources.put("source/", sourceHead(0));
        sources.put("source/file", sourceHead(4));
        when(client.getObjectTagging(any(GetObjectTaggingRequest.class)))
                .thenAnswer(
                        invocation -> {
                            GetObjectTaggingRequest request = invocation.getArgument(0);
                            java.util.List<Tag> tags = new java.util.ArrayList<>();
                            int count = request.key().equals("source/file") ? 10 : 1;
                            for (int i = 0; i < count; i++) {
                                tags.add(Tag.builder().key("source-" + i).value("value").build());
                            }
                            return GetObjectTaggingResponse.builder().tagSet(tags).build();
                        });
        Options options = new Options();
        options.set("s3.write.tags", "added:value");
        assertThatThrownBy(
                        () ->
                                S3NativeFileIORenameTest.fileIO(client, options)
                                        .rename(SOURCE, DESTINATION))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("10");
        assertNoCopyOrDelete();
    }

    @Test
    void testMissingDirectoryObjectFailsBeforeAnyCopy() throws Exception {
        directoryListing();
        sources.put("source/", sourceHead(0));
        assertThatThrownBy(
                        () ->
                                S3NativeFileIORenameTest.fileIO(client, new Options())
                                        .rename(SOURCE, DESTINATION))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("source/file");
        assertNoCopyOrDelete();
    }

    @Test
    void testNewMarkerUsesConfiguredAttributes() throws Exception {
        assertThat(S3NativeFileIORenameTest.fileIO(client, configuredAttributes()).mkdirs(SOURCE))
                .isTrue();
        org.mockito.ArgumentCaptor<PutObjectRequest> request =
                org.mockito.ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(client).putObject(request.capture(), any(RequestBody.class));
        assertThat(request.getValue().storageClass()).isEqualTo(StorageClass.ONEZONE_IA);
        assertThat(request.getValue().acl()).isEqualTo(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL);
        assertThat(decodeTags(request.getValue().tagging()))
                .containsEntry("shared", "configured")
                .containsEntry("added", "new");
    }

    private void directoryListing() {
        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenAnswer(
                        invocation -> {
                            ListObjectsV2Request request = invocation.getArgument(0);
                            return request.prefix().equals("source/")
                                    ? ListObjectsV2Response.builder()
                                            .contents(
                                                    S3Object.builder()
                                                            .key("source/")
                                                            .size(0L)
                                                            .build(),
                                                    // Deliberately stale size: HEAD decides copy
                                                    // mode.
                                                    S3Object.builder()
                                                            .key("source/file")
                                                            .size(1L)
                                                            .build())
                                            .build()
                                    : ListObjectsV2Response.builder().build();
                        });
    }

    private void recordMultipartDestination() throws Exception {
        CreateMultipartUploadRequest request = create.get();
        String expires =
                request.overrideConfiguration()
                        .map(config -> config.headers().get("Expires"))
                        .filter(values -> !values.isEmpty())
                        .map(values -> values.get(0))
                        .orElse(
                                request.expires() == null
                                        ? null
                                        : DateUtils.formatRfc822Date(request.expires()));
        destinations.put(
                request.key(),
                HeadObjectResponse.builder()
                        .contentLength(size(true))
                        .metadata(request.metadata())
                        .contentType(request.contentType())
                        .cacheControl(request.cacheControl())
                        .contentEncoding(request.contentEncoding())
                        .contentLanguage(request.contentLanguage())
                        .contentDisposition(request.contentDisposition())
                        .expiresString(expires)
                        .websiteRedirectLocation(request.websiteRedirectLocation())
                        .storageClass(storageClass(request.storageClass()))
                        .build());
        destinationTags.put(request.key(), decodeTags(request.tagging()));
        destinationAcls.put(request.key(), acl(request.acl()));
    }

    private void assertNoCopyOrDelete() {
        verify(client, never()).copyObject(any(CopyObjectRequest.class));
        verify(client, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(client, never()).deleteObject(any(DeleteObjectRequest.class));
        verify(client, never()).deleteObjects(any(DeleteObjectsRequest.class));
    }

    private static HeadObjectResponse sourceHead(long size) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("custom", "value");
        metadata.put("paimon-upload-id", "previous-upload-token");
        return HeadObjectResponse.builder()
                .contentLength(size)
                .metadata(metadata)
                .contentType("application/parquet")
                .cacheControl("max-age=60")
                .contentEncoding("gzip")
                .contentLanguage("zh-CN")
                .contentDisposition("attachment; filename=table.parquet")
                .expiresString(DateUtils.formatRfc822Date(EXPIRES))
                .websiteRedirectLocation("/table")
                .storageClass(StorageClass.STANDARD_IA)
                .build();
    }

    private static void assertPreservedAttributes(HeadObjectResponse head) {
        assertThat(head.metadata()).containsEntry("custom", "value");
        assertThat(head.contentType()).isEqualTo("application/parquet");
        assertThat(head.cacheControl()).isEqualTo("max-age=60");
        assertThat(head.contentEncoding()).isEqualTo("gzip");
        assertThat(head.contentLanguage()).isEqualTo("zh-CN");
        assertThat(head.contentDisposition()).isEqualTo("attachment; filename=table.parquet");
        assertThat(head.expiresString()).isEqualTo(DateUtils.formatRfc822Date(EXPIRES));
        assertThat(head.websiteRedirectLocation()).isEqualTo("/table");
        assertThat(head.storageClass()).isEqualTo(StorageClass.STANDARD_IA);
    }

    private static Options configuredAttributes() {
        Options options = new Options();
        options.set("s3.write.tags", "shared:configured,added:new");
        options.set("s3.write.storage-class", "ONEZONE_IA");
        options.set("s3.acl", "bucket-owner-full-control");
        return options;
    }

    private static long size(boolean multipart) {
        return multipart ? S3NativeFileIORenameTest.LARGE_OBJECT_SIZE : 4L;
    }

    private static StorageClass storageClass(StorageClass configured) {
        return configured == null ? StorageClass.STANDARD : configured;
    }

    private static ObjectCannedACL acl(ObjectCannedACL configured) {
        return configured == null ? ObjectCannedACL.PRIVATE : configured;
    }

    private static Map<String, String> sourceTags() {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("source-only", "a b+c/值");
        tags.put("shared", "source");
        return tags;
    }

    private static Map<String, String> decodeTags(String query) throws Exception {
        Map<String, String> tags = new HashMap<>();
        if (query != null && !query.isEmpty()) {
            for (String entry : query.split("&")) {
                String[] parts = entry.split("=", 2);
                tags.put(
                        URLDecoder.decode(parts[0], "UTF-8"), URLDecoder.decode(parts[1], "UTF-8"));
            }
        }
        return tags;
    }
}
