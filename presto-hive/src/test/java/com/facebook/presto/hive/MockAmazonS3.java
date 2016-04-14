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
package com.facebook.presto.hive;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.HttpMethod;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLoggingConfiguration;
import com.amazonaws.services.s3.model.BucketNotificationConfiguration;
import com.amazonaws.services.s3.model.BucketPolicy;
import com.amazonaws.services.s3.model.BucketReplicationConfiguration;
import com.amazonaws.services.s3.model.BucketTaggingConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.BucketWebsiteConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketPolicyRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetBucketAclRequest;
import com.amazonaws.services.s3.model.GetBucketLocationRequest;
import com.amazonaws.services.s3.model.GetBucketPolicyRequest;
import com.amazonaws.services.s3.model.GetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListBucketsRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SetBucketAclRequest;
import com.amazonaws.services.s3.model.SetBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketLoggingConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketNotificationConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketPolicyRequest;
import com.amazonaws.services.s3.model.SetBucketReplicationConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.SetObjectAclRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.VersionListing;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;

import static org.apache.http.HttpStatus.SC_OK;

public class MockAmazonS3
        implements AmazonS3
{
    private int getObjectHttpCode = SC_OK;
    private int getObjectMetadataHttpCode = SC_OK;

    public void setGetObjectHttpErrorCode(int getObjectHttpErrorCode)
    {
        this.getObjectHttpCode = getObjectHttpErrorCode;
    }

    public void setGetObjectMetadataHttpCode(int getObjectMetadataHttpCode)
    {
        this.getObjectMetadataHttpCode = getObjectMetadataHttpCode;
    }

    @Override
    public void setEndpoint(String endpoint)
    {
    }

    @Override
    public void setRegion(Region region)
            throws IllegalArgumentException
    {
    }

    @Override
    public void setS3ClientOptions(S3ClientOptions clientOptions)
    {
    }

    @Override
    public void changeObjectStorageClass(String bucketName, String key, StorageClass newStorageClass)
            throws AmazonClientException
    {
    }

    @Override
    public void setObjectRedirectLocation(String bucketName, String key, String newRedirectLocation)
            throws AmazonClientException
    {
    }

    @Override
    public ObjectListing listObjects(String bucketName)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public ObjectListing listObjects(String bucketName, String prefix)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public VersionListing listVersions(String bucketName, String prefix)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public VersionListing listNextBatchOfVersions(VersionListing previousVersionListing)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public VersionListing listVersions(String bucketName, String prefix, String keyMarker, String versionIdMarker, String delimiter, Integer maxResults)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public VersionListing listVersions(ListVersionsRequest listVersionsRequest)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public Owner getS3AccountOwner()
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public boolean doesBucketExist(String bucketName)
            throws AmazonClientException
    {
        return false;
    }

    @Override
    public List<Bucket> listBuckets()
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public List<Bucket> listBuckets(ListBucketsRequest listBucketsRequest)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public String getBucketLocation(String bucketName)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public String getBucketLocation(GetBucketLocationRequest getBucketLocationRequest)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public Bucket createBucket(CreateBucketRequest createBucketRequest)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public Bucket createBucket(String bucketName)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public Bucket createBucket(String bucketName, com.amazonaws.services.s3.model.Region region)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public Bucket createBucket(String bucketName, String region)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public AccessControlList getObjectAcl(String bucketName, String key)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public AccessControlList getObjectAcl(String bucketName, String key, String versionId)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public void setObjectAcl(String bucketName, String key, AccessControlList acl)
            throws AmazonClientException
    {
    }

    @Override
    public void setObjectAcl(String bucketName, String key, CannedAccessControlList acl)
            throws AmazonClientException
    {
    }

    @Override
    public void setObjectAcl(String bucketName, String key, String versionId, AccessControlList acl)
            throws AmazonClientException
    {
    }

    @Override
    public void setObjectAcl(String bucketName, String key, String versionId, CannedAccessControlList acl)
            throws AmazonClientException
    {
    }

    @Override
    public void setObjectAcl(SetObjectAclRequest setObjectAclRequest)
            throws AmazonClientException
    {
    }

    @Override
    public AccessControlList getBucketAcl(String bucketName)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public void setBucketAcl(SetBucketAclRequest setBucketAclRequest)
            throws AmazonClientException
    {
    }

    @Override
    public AccessControlList getBucketAcl(GetBucketAclRequest getBucketAclRequest)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public void setBucketAcl(String bucketName, AccessControlList acl)
            throws AmazonClientException
    {
    }

    @Override
    public void setBucketAcl(String bucketName, CannedAccessControlList acl)
            throws AmazonClientException
    {
    }

    @Override
    public ObjectMetadata getObjectMetadata(String bucketName, String key)
            throws AmazonClientException
    {
        if (getObjectMetadataHttpCode != SC_OK) {
            AmazonS3Exception exception = new AmazonS3Exception("Failing getObjectMetadata call with " + getObjectMetadataHttpCode);
            exception.setStatusCode(getObjectMetadataHttpCode);
            throw exception;
        }
        return null;
    }

    @Override
    public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest getObjectMetadataRequest)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public S3Object getObject(String bucketName, String key)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public S3Object getObject(GetObjectRequest getObjectRequest)
            throws AmazonClientException
    {
        if (getObjectHttpCode != SC_OK) {
            AmazonS3Exception exception = new AmazonS3Exception("Failing getObject call with " + getObjectHttpCode);
            exception.setStatusCode(getObjectHttpCode);
            throw exception;
        }
        return null;
    }

    @Override
    public ObjectMetadata getObject(GetObjectRequest getObjectRequest, File destinationFile)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public void deleteBucket(DeleteBucketRequest deleteBucketRequest)
            throws AmazonClientException
    {
    }

    @Override
    public void deleteBucket(String bucketName)
            throws AmazonClientException
    {
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest putObjectRequest)
            throws AmazonClientException
    {
        return new PutObjectResult();
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, File file)
            throws AmazonClientException
    {
        return new PutObjectResult();
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata)
            throws AmazonClientException
    {
        return new PutObjectResult();
    }

    @Override
    public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public CopyPartResult copyPart(CopyPartRequest copyPartRequest)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public void deleteObject(String bucketName, String key)
            throws AmazonClientException
    {
    }

    @Override
    public void deleteObject(DeleteObjectRequest deleteObjectRequest)
            throws AmazonClientException
    {
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public void deleteVersion(String bucketName, String key, String versionId)
            throws AmazonClientException
    {
    }

    @Override
    public void deleteVersion(DeleteVersionRequest deleteVersionRequest)
            throws AmazonClientException
    {
    }

    @Override
    public BucketLoggingConfiguration getBucketLoggingConfiguration(String bucketName)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public void setBucketLoggingConfiguration(SetBucketLoggingConfigurationRequest setBucketLoggingConfigurationRequest)
            throws AmazonClientException
    {
    }

    @Override
    public BucketVersioningConfiguration getBucketVersioningConfiguration(String bucketName)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public void setBucketVersioningConfiguration(SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest)
            throws AmazonClientException
    {
    }

    @Override
    public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String bucketName)
    {
        return null;
    }

    @Override
    public void setBucketLifecycleConfiguration(String bucketName, BucketLifecycleConfiguration bucketLifecycleConfiguration)
    {
    }

    @Override
    public void setBucketLifecycleConfiguration(SetBucketLifecycleConfigurationRequest setBucketLifecycleConfigurationRequest)
    {
    }

    @Override
    public void deleteBucketLifecycleConfiguration(String bucketName)
    {
    }

    @Override
    public void deleteBucketLifecycleConfiguration(DeleteBucketLifecycleConfigurationRequest deleteBucketLifecycleConfigurationRequest)
    {
    }

    @Override
    public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(String bucketName)
    {
        return null;
    }

    @Override
    public void setBucketCrossOriginConfiguration(String bucketName, BucketCrossOriginConfiguration bucketCrossOriginConfiguration)
    {
    }

    @Override
    public void setBucketCrossOriginConfiguration(SetBucketCrossOriginConfigurationRequest setBucketCrossOriginConfigurationRequest)
    {
    }

    @Override
    public void deleteBucketCrossOriginConfiguration(String bucketName)
    {
    }

    @Override
    public void deleteBucketCrossOriginConfiguration(DeleteBucketCrossOriginConfigurationRequest deleteBucketCrossOriginConfigurationRequest)
    {
    }

    @Override
    public BucketTaggingConfiguration getBucketTaggingConfiguration(String bucketName)
    {
        return null;
    }

    @Override
    public void setBucketTaggingConfiguration(String bucketName, BucketTaggingConfiguration bucketTaggingConfiguration)
    {
    }

    @Override
    public void setBucketTaggingConfiguration(SetBucketTaggingConfigurationRequest setBucketTaggingConfigurationRequest)
    {
    }

    @Override
    public void deleteBucketTaggingConfiguration(String bucketName)
    {
    }

    @Override
    public void deleteBucketTaggingConfiguration(DeleteBucketTaggingConfigurationRequest deleteBucketTaggingConfigurationRequest)
    {
    }

    @Override
    public BucketNotificationConfiguration getBucketNotificationConfiguration(String bucketName)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public void setBucketNotificationConfiguration(SetBucketNotificationConfigurationRequest setBucketNotificationConfigurationRequest)
            throws AmazonClientException
    {
    }

    @Override
    public void setBucketNotificationConfiguration(String bucketName, BucketNotificationConfiguration bucketNotificationConfiguration)
            throws AmazonClientException
    {
    }

    @Override
    public BucketWebsiteConfiguration getBucketWebsiteConfiguration(String bucketName)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public BucketWebsiteConfiguration getBucketWebsiteConfiguration(GetBucketWebsiteConfigurationRequest getBucketWebsiteConfigurationRequest)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public void setBucketWebsiteConfiguration(String bucketName, BucketWebsiteConfiguration configuration)
            throws AmazonClientException
    {
    }

    @Override
    public void setBucketWebsiteConfiguration(SetBucketWebsiteConfigurationRequest setBucketWebsiteConfigurationRequest)
            throws AmazonClientException
    {
    }

    @Override
    public void deleteBucketWebsiteConfiguration(String bucketName)
            throws AmazonClientException
    {
    }

    @Override
    public void deleteBucketWebsiteConfiguration(DeleteBucketWebsiteConfigurationRequest deleteBucketWebsiteConfigurationRequest)
            throws AmazonClientException
    {
    }

    @Override
    public BucketPolicy getBucketPolicy(String bucketName)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public BucketPolicy getBucketPolicy(GetBucketPolicyRequest getBucketPolicyRequest)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public void setBucketPolicy(String bucketName, String policyText)
            throws AmazonClientException
    {
    }

    @Override
    public void setBucketPolicy(SetBucketPolicyRequest setBucketPolicyRequest)
            throws AmazonClientException
    {
    }

    @Override
    public void deleteBucketPolicy(String bucketName)
            throws AmazonClientException
    {
    }

    @Override
    public void deleteBucketPolicy(DeleteBucketPolicyRequest deleteBucketPolicyRequest)
            throws AmazonClientException
    {
    }

    @Override
    public URL generatePresignedUrl(String bucketName, String key, Date expiration)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public URL generatePresignedUrl(String bucketName, String key, Date expiration, HttpMethod method)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public URL generatePresignedUrl(GeneratePresignedUrlRequest generatePresignedUrlRequest)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public UploadPartResult uploadPart(UploadPartRequest request)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public PartListing listParts(ListPartsRequest request)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public void abortMultipartUpload(AbortMultipartUploadRequest request)
            throws AmazonClientException
    {
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public S3ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request)
    {
        return null;
    }

    @Override
    public void restoreObject(RestoreObjectRequest request)
            throws AmazonServiceException
    {
    }

    @Override
    public void restoreObject(String bucketName, String key, int expirationInDays)
            throws AmazonServiceException
    {
    }

    @Override
    public void enableRequesterPays(String bucketName)
            throws AmazonClientException
    {
    }

    @Override
    public void disableRequesterPays(String bucketName)
            throws AmazonClientException
    {
    }

    @Override
    public boolean isRequesterPaysEnabled(String bucketName)
            throws AmazonClientException
    {
        return false;
    }

    @Override
    public void setBucketReplicationConfiguration(String bucketName, BucketReplicationConfiguration bucketReplicationConfiguration)
            throws AmazonClientException
    {
    }

    @Override
    public void setBucketReplicationConfiguration(SetBucketReplicationConfigurationRequest setBucketReplicationConfigurationRequest)
            throws AmazonClientException
    {
    }

    @Override
    public BucketReplicationConfiguration getBucketReplicationConfiguration(String bucketName)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public void deleteBucketReplicationConfiguration(String bucketName)
            throws AmazonClientException
    {
    }
}
