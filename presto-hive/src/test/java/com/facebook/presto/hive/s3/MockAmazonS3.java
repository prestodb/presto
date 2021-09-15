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
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
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
    private static final String STANDARD_OBJECT_KEY = "test/standard";
    private static final String GLACIER_OBJECT_KEY = "test/glacier";

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
        }

        return listing;
    }

    @Override
    public void shutdown()
    {
    }
}
