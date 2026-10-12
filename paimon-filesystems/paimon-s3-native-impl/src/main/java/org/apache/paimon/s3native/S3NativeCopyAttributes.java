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

import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.TaggingDirective;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Source attributes and explicit destination overrides for both S3 copy paths.
 *
 * <p>Only an explicitly configured ACL is sent. Source ACLs are not queried or copied; S3 defaults
 * copies to private when ACLs are enabled. Omitting the ACL also supports bucket-owner-enforced
 * ownership. See https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html.
 */
final class S3NativeCopyAttributes {

    private final HeadObjectResponse head;
    private final Tagging tags;
    private final String storageClass;
    private final ObjectCannedACL acl;

    private S3NativeCopyAttributes(
            HeadObjectResponse head, Map<String, String> sourceTags, S3NativeOptions options)
            throws IOException {
        this.head = head;
        Map<String, String> mergedTags = new LinkedHashMap<>(sourceTags);
        mergedTags.putAll(options.writeTags);
        // Both tag sets may be valid separately while their union exceeds the S3 object limit.
        // https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html
        if (mergedTags.size() > 10) {
            throw new IOException("Merged S3 object tags exceed the limit of 10");
        }
        this.tags = toTagging(mergedTags);
        this.storageClass =
                options.writeStorageClass == null
                        ? head.storageClassAsString()
                        : options.writeStorageClass.toString();
        this.acl = options.acl;
    }

    static S3NativeCopyAttributes load(
            S3NativeObjectOperations operations, String sourceKey, S3NativeOptions options)
            throws IOException {
        HeadObjectResponse head;
        try {
            head = operations.headObjectOrNull(sourceKey);
        } catch (RuntimeException e) {
            throw new IOException("Failed to read source object metadata: " + sourceKey, e);
        }
        if (head == null) {
            throw new IOException("Source object disappeared before copy: " + sourceKey);
        }
        if (head.contentLength() == null) {
            throw new IOException("Missing source object length before copy: " + sourceKey);
        }
        try {
            // Source tags are read before any destination write. A permission error must not
            // silently remove tags. Requires s3:GetObjectTagging; see:
            // https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html
            GetObjectTaggingResponse response =
                    operations
                            .client()
                            .getObjectTagging(
                                    GetObjectTaggingRequest.builder()
                                            .bucket(operations.bucket())
                                            .key(sourceKey)
                                            .build());
            if (response == null) {
                throw new IOException("Missing source object tags response: " + sourceKey);
            }
            Map<String, String> sourceTags = new LinkedHashMap<>();
            for (Tag tag : response.tagSet()) {
                sourceTags.put(tag.key(), tag.value());
            }
            return new S3NativeCopyAttributes(head, sourceTags, options);
        } catch (RuntimeException e) {
            throw new IOException("Failed to read source object tags: " + sourceKey, e);
        }
    }

    static List<S3NativeCopyAttributes> loadAll(
            S3NativeObjectOperations operations,
            List<S3Object> sourceObjects,
            S3NativeOptions options)
            throws IOException {
        List<S3NativeCopyAttributes> attributes = new ArrayList<>(sourceObjects.size());
        for (S3Object object : sourceObjects) {
            attributes.add(load(operations, object.key(), options));
        }
        return attributes;
    }

    long contentLength() {
        return head.contentLength();
    }

    void apply(CopyObjectRequest.Builder builder) {
        // COPY preserves source user metadata and content headers. The redirect header is the
        // documented exception and must be sent explicitly. Source:
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html
        builder.metadataDirective(MetadataDirective.COPY)
                .websiteRedirectLocation(head.websiteRedirectLocation())
                .taggingDirective(TaggingDirective.REPLACE)
                .tagging(tags)
                .storageClass(storageClass)
                .acl(acl);
    }

    void apply(CreateMultipartUploadRequest.Builder builder, String uploadToken) {
        // Multipart copy does not automatically inherit source metadata. Start the upload with
        // all source attributes, replacing any previous internal token with this upload's token.
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
        builder.metadata(S3NativeUploadMetadata.withToken(head.metadata(), uploadToken))
                .contentType(head.contentType())
                .cacheControl(head.cacheControl())
                .contentEncoding(head.contentEncoding())
                .contentLanguage(head.contentLanguage())
                .contentDisposition(head.contentDisposition())
                .websiteRedirectLocation(head.websiteRedirectLocation())
                .tagging(tags)
                .storageClass(storageClass)
                .acl(acl);
        if (head.expiresString() != null) {
            // The SDK retains raw Expires because S3 can return non-date values such as "0".
            // Preserve that header without converting it through the request's Instant field.
            builder.overrideConfiguration(
                    config -> config.putHeader("Expires", head.expiresString()));
        }
    }

    static void applyWriteAttributes(PutObjectRequest.Builder builder, S3NativeOptions options) {
        if (!options.writeTags.isEmpty()) {
            builder.tagging(toTagging(options.writeTags));
        }
        if (options.writeStorageClass != null) {
            builder.storageClass(options.writeStorageClass);
        }
        if (options.acl != null) {
            builder.acl(options.acl);
        }
    }

    private static Tagging toTagging(Map<String, String> values) {
        List<Tag> tags = new ArrayList<>(values.size());
        for (Map.Entry<String, String> entry : values.entrySet()) {
            tags.add(Tag.builder().key(entry.getKey()).value(entry.getValue()).build());
        }
        // The SDK's Tagging overload performs the required URL encoding; do not encode manually.
        return Tagging.builder().tagSet(tags).build();
    }
}
