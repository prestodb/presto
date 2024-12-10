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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.net.HttpURLConnection.HTTP_OK;

public class MockAmazonS3
        extends AbstractAmazonS3
{
    private static final String STANDARD_OBJECT_KEY = "test/standard";
    private static final String GLACIER_OBJECT_KEY = "test/glacier";

    private int getObjectHttpCode = HTTP_OK;
    private int getObjectMetadataHttpCode = HTTP_OK;
    private GetObjectMetadataRequest getObjectMetadataRequest;
    private CannedAccessControlList acl;
    private boolean hasGlacierObjects;
    private boolean hasHadoopFolderMarkerObjects;
    int initiateMultipartRequests;
    int uploadPartRequests;
    int completePartUploadRequests;
    int abortPartUploadRequests;
    int putObjectRequests;
    boolean throwOnUpload;

    Map<String, ByteBuffer> uploads = new HashMap<>();
    Map<String, ByteArrayOutputStream> inProgressUploads = new HashMap<>();

    public void setGetObjectHttpErrorCode(int getObjectHttpErrorCode)
    {
        this.getObjectHttpCode = getObjectHttpErrorCode;
    }

    public void setGetObjectMetadataHttpCode(int getObjectMetadataHttpCode)
    {
        this.getObjectMetadataHttpCode = getObjectMetadataHttpCode;
    }

    public CannedAccessControlList getAcl()
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

    public GetObjectMetadataRequest getGetObjectMetadataRequest()
    {
        return getObjectMetadataRequest;
    }

    public void setThrowOnNextUpload()
    {
        throwOnUpload = true;
    }

    private void throwIfNecessary()
    {
        if (throwOnUpload) {
            throwOnUpload = false;
            throw new RuntimeException("Mocked error");
        }
    }

    @Override
    public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest getObjectMetadataRequest)
    {
        this.getObjectMetadataRequest = getObjectMetadataRequest;
        if (getObjectMetadataHttpCode != HTTP_OK) {
            AmazonS3Exception exception = new AmazonS3Exception("Failing getObjectMetadata call with " + getObjectMetadataHttpCode);
            exception.setStatusCode(getObjectMetadataHttpCode);
            throw exception;
        }
        return null;
    }

    @Override
    public S3Object getObject(GetObjectRequest getObjectRequest)
    {
        if (getObjectHttpCode != HTTP_OK) {
            AmazonS3Exception exception = new AmazonS3Exception("Failing getObject call with " + getObjectHttpCode);
            exception.setStatusCode(getObjectHttpCode);
            throw exception;
        }

        return Optional.ofNullable(uploads.get(getObjectRequest.getBucketName() + getObjectRequest.getKey()))
                .map(file -> {
                    S3Object object = new S3Object();
                    object.setObjectContent(new ByteArrayInputStream(file.array()));
                    object.setBucketName(getObjectRequest.getBucketName());
                    object.setKey(getObjectRequest.getKey());
                    return object;
                }).orElseGet(() -> {
                    AmazonS3Exception exception = new AmazonS3Exception("Failing getObject call with not found");
                    exception.setStatusCode(404);
                    throw exception;
                });
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest putObjectRequest)
    {
        throwIfNecessary();
        this.acl = putObjectRequest.getCannedAcl();
        putObjectRequests++;
        try {
            uploads.put(putObjectRequest.getBucketName() + putObjectRequest.getKey(), ByteBuffer.wrap(Files.readAllBytes(putObjectRequest.getFile().toPath())));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new PutObjectResult();
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, String content)
    {
        return new PutObjectResult();
    }

    @Override
    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
    {
        ObjectListing listing = new ObjectListing();

        S3ObjectSummary standard = new S3ObjectSummary();
        standard.setStorageClass(StorageClass.Standard.toString());
        standard.setKey(STANDARD_OBJECT_KEY);
        standard.setLastModified(new Date());
        listing.getObjectSummaries().add(standard);

        if (hasHadoopFolderMarkerObjects) {
            S3ObjectSummary hadoopFolderMarker = new S3ObjectSummary();
            hadoopFolderMarker.setStorageClass(StorageClass.Standard.toString());
            hadoopFolderMarker.setKey("test/test_$folder$");
            hadoopFolderMarker.setLastModified(new Date());
            listing.getObjectSummaries().add(hadoopFolderMarker);
        }

        if (hasGlacierObjects) {
            S3ObjectSummary glacier = new S3ObjectSummary();
            glacier.setStorageClass(StorageClass.Glacier.toString());
            glacier.setKey(GLACIER_OBJECT_KEY);
            glacier.setLastModified(new Date());
            listing.getObjectSummaries().add(glacier);

            S3ObjectSummary deepArchive = new S3ObjectSummary();
            deepArchive.setStorageClass(StorageClass.DeepArchive.toString());
            deepArchive.setKey("test/deepArchive");
            deepArchive.setLastModified(new Date());
            listing.getObjectSummaries().add(deepArchive);
        }

        return listing;
    }

    @Override
    public void shutdown()
    {
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request)
            throws SdkClientException, AmazonServiceException
    {
        throwIfNecessary();
        InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
        this.initiateMultipartRequests++;
        String uploadId = "uploadId-" + initiateMultipartRequests;
        result.setUploadId(uploadId);
        inProgressUploads.computeIfAbsent(uploadId, (key) -> new ByteArrayOutputStream());
        this.acl = request.getCannedACL();
        return result;
    }

    @Override
    public UploadPartResult uploadPart(UploadPartRequest request)
            throws SdkClientException, AmazonServiceException
    {
        throwIfNecessary();
        if (!inProgressUploads.containsKey(request.getUploadId())) {
            throw new SdkClientException("bad upload ID");
        }
        inProgressUploads.computeIfPresent(request.getUploadId(), (key, value) -> {
            try {
                value.write(Files.readAllBytes(request.getFile().toPath()));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            return value;
        });
        uploadPartRequests++;
        return new UploadPartResult();
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request)
            throws SdkClientException, AmazonServiceException
    {
        throwIfNecessary();
        completePartUploadRequests++;
        ByteBuffer file = ByteBuffer.wrap(inProgressUploads.get(request.getUploadId()).toByteArray());
        inProgressUploads.remove(request.getUploadId());
        uploads.put(request.getBucketName() + request.getKey(), file);
        CompleteMultipartUploadResult result = new CompleteMultipartUploadResult();
        try {
            result.setETag(Base64.getEncoder().encodeToString(MessageDigest.getInstance("MD5").digest(file.array())));
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public void abortMultipartUpload(AbortMultipartUploadRequest request)
            throws SdkClientException, AmazonServiceException
    {
        throwIfNecessary();
        abortPartUploadRequests++;
        uploads.remove(request.getBucketName() + request.getKey());
        inProgressUploads.remove(request.getUploadId());
    }

    @Override
    public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request)
            throws SdkClientException, AmazonServiceException
    {
        MultipartUploadListing listing = new MultipartUploadListing();
        listing.setMultipartUploads(inProgressUploads.keySet().stream().map(id -> {
            MultipartUpload upload = new MultipartUpload();
            upload.setUploadId(id);
            return upload;
        }).collect(Collectors.toList()));
        return listing;
    }
}
