/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive.s3;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.StorageClass;

import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static java.net.HttpURLConnection.HTTP_OK;

public class MockAmazonS3
        implements S3Client
{
    private static final String STANDARD_ONE_OBJECT_KEY = "test/standardOne";
    private static final String STANDARD_TWO_OBJECT_KEY = "test/standardTwo";
    private static final String GLACIER_OBJECT_KEY = "test/glacier";
    private static final String HADOOP_FOLDER_MARKER_OBJECT_KEY = "test/test_$folder$";
    private static final String CONTINUATION_TOKEN = "continue";
    private static final String PAGINATION_PREFIX = "test-pagination/";
    private static final String DEEP_ARCHIVE_OBJECT_KEY = "test/deepArchive";

    private int getObjectHttpCode = HTTP_OK;
    private int headObjectHttpCode = HTTP_OK;
    private HeadObjectRequest lastHeadObjectRequest;
    private ObjectCannedACL acl;
    private boolean hasGlacierObjects;
    private boolean hasHadoopFolderMarkerObjects;

    public void setGetObjectHttpErrorCode(int getObjectHttpErrorCode)
    {
        this.getObjectHttpCode = getObjectHttpErrorCode;
    }

    public void setHeadObjectHttpErrorCode(int headObjectHttpErrorCode)
    {
        this.headObjectHttpCode = headObjectHttpErrorCode;
    }

    public ObjectCannedACL getAcl()
    {
        return this.acl;
    }

    public void setHasGlacierObjects(boolean hasGlacierObjects)
    {
        this.hasGlacierObjects = hasGlacierObjects;
    }

    public void setHasHadoopFolderMarkerObjects(boolean hasHadoopFolderMarkerObjects)
    {
        this.hasHadoopFolderMarkerObjects = hasHadoopFolderMarkerObjects;
    }

    public HeadObjectRequest getLastHeadObjectRequest()
    {
        return lastHeadObjectRequest;
    }

    @Override
    public HeadObjectResponse headObject(HeadObjectRequest headObjectRequest)
    {
        this.lastHeadObjectRequest = headObjectRequest;
        if (headObjectHttpCode != HTTP_OK) {
            throw S3Exception.builder()
                    .message("Failing headObject call with " + headObjectHttpCode)
                    .statusCode(headObjectHttpCode)
                    .build();
        }

        return HeadObjectResponse.builder()
                .contentLength(100L)
                .contentType("application/octet-stream")
                .lastModified(Instant.now())
                .build();
    }

    @Override
    public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest getObjectRequest)
    {
        if (getObjectHttpCode != HTTP_OK) {
            throw S3Exception.builder()
                    .message("Failing getObject call with " + getObjectHttpCode)
                    .statusCode(getObjectHttpCode)
                    .build();
        }

        // FIX: Return a byte array with actual content (100 bytes) instead of empty array
        // This fixes testReadRetryCounters and other read tests
        GetObjectResponse response = GetObjectResponse.builder()
                .contentLength(100L)
                .build();
        return new ResponseInputStream<>(response, new ByteArrayInputStream(new byte[100]));
    }

    @Override
    public PutObjectResponse putObject(PutObjectRequest putObjectRequest, RequestBody requestBody)
    {
        this.acl = putObjectRequest.acl();
        return PutObjectResponse.builder().build();
    }

    @Override
    public ListObjectsV2Response listObjectsV2(ListObjectsV2Request listObjectsV2Request)
    {
        ListObjectsV2Response.Builder responseBuilder = ListObjectsV2Response.builder();
        List<S3Object> objects = new ArrayList<>();

        if (hasHadoopFolderMarkerObjects) {
            objects.add(S3Object.builder()
                    .key(HADOOP_FOLDER_MARKER_OBJECT_KEY)
                    .storageClass(StorageClass.STANDARD.toString())
                    .lastModified(Instant.now())
                    .size(0L)
                    .build());
        }

        if (hasGlacierObjects) {
            objects.add(S3Object.builder()
                    .key(GLACIER_OBJECT_KEY)
                    .storageClass(StorageClass.GLACIER.toString())
                    .lastModified(Instant.now())
                    .size(100L)
                    .build());

            objects.add(S3Object.builder()
                    .key(DEEP_ARCHIVE_OBJECT_KEY)
                    .storageClass(StorageClass.DEEP_ARCHIVE.toString())
                    .lastModified(Instant.now())
                    .size(100L)
                    .build());
        }

        if (CONTINUATION_TOKEN.equals(listObjectsV2Request.continuationToken())) {
            objects.add(S3Object.builder()
                    .key(STANDARD_TWO_OBJECT_KEY)
                    .storageClass(StorageClass.STANDARD.toString())
                    .lastModified(Instant.now())
                    .size(100L)
                    .build());
            responseBuilder.isTruncated(false);
        }
        else {
            objects.add(S3Object.builder()
                    .key(STANDARD_ONE_OBJECT_KEY)
                    .storageClass(StorageClass.STANDARD.toString())
                    .lastModified(Instant.now())
                    .size(100L)
                    .build());
            if (PAGINATION_PREFIX.equals(listObjectsV2Request.prefix())) {
                responseBuilder.isTruncated(true);
                responseBuilder.nextContinuationToken(CONTINUATION_TOKEN);
            }
            else {
                responseBuilder.isTruncated(false);
            }
        }

        responseBuilder.contents(objects);
        return responseBuilder.build();
    }

    @Override
    public CopyObjectResponse copyObject(CopyObjectRequest copyObjectRequest)
    {
        return CopyObjectResponse.builder().build();
    }

    @Override
    public DeleteObjectResponse deleteObject(DeleteObjectRequest deleteObjectRequest)
    {
        return DeleteObjectResponse.builder().build();
    }

    @Override
    public void close()
    {
        // No-op for mock
    }

    @Override
    public String serviceName()
    {
        return "S3";
    }
}
