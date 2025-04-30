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

import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;

import java.util.Date;

import static java.net.HttpURLConnection.HTTP_OK;

public class MockAmazonS3
        extends AbstractAmazonS3
{
    private static final String STANDARD_ONE_OBJECT_KEY = "test/standardOne";
    private static final String STANDARD_TWO_OBJECT_KEY = "test/standardTwo";
    private static final String GLACIER_OBJECT_KEY = "test/glacier";
    private static final String HADOOP_FOLDER_MARKER_OBJECT_KEY = "test/test_$folder$";
    private static final String CONTINUATION_TOKEN = "continue";
    private static final String PAGINATION_PREFIX = "test-pagination/";
    private static final String DEEP_ARCHIVE_OBJECT_KEY = "test/deepArchive";

    private int getObjectHttpCode = HTTP_OK;
    private int getObjectMetadataHttpCode = HTTP_OK;
    private GetObjectMetadataRequest getObjectMetadataRequest;
    private CannedAccessControlList acl;
    private boolean hasGlacierObjects;
    private boolean hasHadoopFolderMarkerObjects;

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
        return null;
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest putObjectRequest)
    {
        this.acl = putObjectRequest.getCannedAcl();
        return new PutObjectResult();
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, String content)
    {
        return new PutObjectResult();
    }

    @Override
    public ListObjectsV2Result listObjectsV2(ListObjectsV2Request listObjectsV2Request)
    {
        ListObjectsV2Result listing = new ListObjectsV2Result();
        if (hasHadoopFolderMarkerObjects) {
            S3ObjectSummary hadoopFolderMarker = new S3ObjectSummary();
            hadoopFolderMarker.setStorageClass(StorageClass.Standard.toString());
            hadoopFolderMarker.setKey(HADOOP_FOLDER_MARKER_OBJECT_KEY);
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
            deepArchive.setKey(DEEP_ARCHIVE_OBJECT_KEY);
            deepArchive.setLastModified(new Date());
            listing.getObjectSummaries().add(deepArchive);
        }
        if (CONTINUATION_TOKEN.equals(listObjectsV2Request.getContinuationToken())) {
            S3ObjectSummary standardTwo = new S3ObjectSummary();
            standardTwo.setStorageClass(StorageClass.Standard.toString());
            standardTwo.setKey(STANDARD_TWO_OBJECT_KEY);
            standardTwo.setLastModified(new Date());
            listing.getObjectSummaries().add(standardTwo);
            listing.setTruncated(false);
        }
        else {
            S3ObjectSummary standardOne = new S3ObjectSummary();
            standardOne.setStorageClass(StorageClass.Standard.toString());
            standardOne.setKey(STANDARD_ONE_OBJECT_KEY);
            standardOne.setLastModified(new Date());
            listing.getObjectSummaries().add(standardOne);
            if (listObjectsV2Request.getPrefix().equals(PAGINATION_PREFIX)) {
                listing.setTruncated(true);
                listing.setNextContinuationToken(CONTINUATION_TOKEN);
            }
        }
        return listing;
    }

    @Override
    public void shutdown()
    {
    }
}
