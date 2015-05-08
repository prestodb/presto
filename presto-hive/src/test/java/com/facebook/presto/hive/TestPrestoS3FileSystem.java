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
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
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
import com.amazonaws.services.s3.model.SetBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.VersionListing;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URL;
import java.util.Date;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.apache.http.HttpStatus.SC_FORBIDDEN;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE;
import static org.testng.Assert.assertEquals;

public class TestPrestoS3FileSystem
{
    @Test
    public void testStaticCredentials()
            throws Exception
    {
        Configuration config = new Configuration();
        config.set("fs.s3n.awsSecretAccessKey", "test_secret_access_key");
        config.set("fs.s3n.awsAccessKeyId", "test_access_key_id");
        // the static credentials should be preferred

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            assertInstanceOf(getAwsCredentialsProvider(fs), StaticCredentialsProvider.class);
        }
    }

    @Test
    public void testInstanceCredentialsEnabled()
            throws Exception
    {
        Configuration config = new Configuration();
        // instance credentials are enabled by default

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            assertInstanceOf(getAwsCredentialsProvider(fs), InstanceProfileCredentialsProvider.class);
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "S3 credentials not configured")
    public void testInstanceCredentialsDisabled()
            throws Exception
    {
        Configuration config = new Configuration();
        config.setBoolean(PrestoS3FileSystem.S3_USE_INSTANCE_CREDENTIALS, false);

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Failing getObject call with " + SC_NOT_FOUND + ".*")
    public void testReadNotFound()
            throws Exception
    {
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectHttpErrorCode(SC_NOT_FOUND);
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration());
            fs.setS3Client(s3);
            try (FSDataInputStream inputStream = fs.open(new Path("s3n://test-bucket/test"))) {
                inputStream.read();
            }
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Failing getObject call with " + SC_FORBIDDEN + ".*")
    public void testReadForbidden()
            throws Exception
    {
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectHttpErrorCode(SC_FORBIDDEN);
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration());
            fs.setS3Client(s3);
            try (FSDataInputStream inputStream = fs.open(new Path("s3n://test-bucket/test"))) {
                inputStream.read();
            }
        }
    }

    @Test
    public void testReadRequestRangeNotSatisfiable()
            throws Exception
    {
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectHttpErrorCode(SC_REQUESTED_RANGE_NOT_SATISFIABLE);
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration());
            fs.setS3Client(s3);
            try (FSDataInputStream inputStream = fs.open(new Path("s3n://test-bucket/test"))) {
                assertEquals(inputStream.read(), -1);
            }
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Failing getObjectMetadata call with " + SC_FORBIDDEN + ".*")
    public void testGetMetadataForbidden()
            throws Exception
    {
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectMetadataHttpCode(SC_FORBIDDEN);
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration());
            fs.setS3Client(s3);
            fs.getS3ObjectMetadata(new Path("s3n://test-bucket/test"));
        }
    }

    @Test
    public void testGetMetadataNotFound()
            throws Exception
    {
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectMetadataHttpCode(SC_NOT_FOUND);
            fs.initialize(new URI("s3n://test-bucket/"), new Configuration());
            fs.setS3Client(s3);
            assertEquals(fs.getS3ObjectMetadata(new Path("s3n://test-bucket/test")), null);
        }
    }

    private static AWSCredentialsProvider getAwsCredentialsProvider(PrestoS3FileSystem fs)
    {
        return getFieldValue(fs.getS3Client(), "awsCredentialsProvider", AWSCredentialsProvider.class);
    }

    @SuppressWarnings("unchecked")
    private static <T> T getFieldValue(Object instance, String name, Class<T> type)
    {
        try {
            Field field = instance.getClass().getDeclaredField(name);
            checkArgument(field.getType() == type, "expected %s but found %s", type, field.getType());
            field.setAccessible(true);
            return (T) field.get(instance);
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    class MockAmazonS3 implements AmazonS3
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
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public void setObjectRedirectLocation(String bucketName, String key, String newRedirectLocation)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public ObjectListing listObjects(String bucketName)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public ObjectListing listObjects(String bucketName, String prefix)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public VersionListing listVersions(String bucketName, String prefix)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public VersionListing listNextBatchOfVersions(VersionListing previousVersionListing)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public VersionListing listVersions(String bucketName, String prefix, String keyMarker, String versionIdMarker, String delimiter, Integer maxResults)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public VersionListing listVersions(ListVersionsRequest listVersionsRequest)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public Owner getS3AccountOwner()
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public boolean doesBucketExist(String bucketName)
                throws AmazonClientException, AmazonServiceException
        {
            return false;
        }

        @Override
        public List<Bucket> listBuckets()
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public List<Bucket> listBuckets(ListBucketsRequest listBucketsRequest)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public String getBucketLocation(String bucketName)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public String getBucketLocation(GetBucketLocationRequest getBucketLocationRequest)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public Bucket createBucket(CreateBucketRequest createBucketRequest)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public Bucket createBucket(String bucketName)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public Bucket createBucket(String bucketName, com.amazonaws.services.s3.model.Region region)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public Bucket createBucket(String bucketName, String region)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public AccessControlList getObjectAcl(String bucketName, String key)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public AccessControlList getObjectAcl(String bucketName, String key, String versionId)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public void setObjectAcl(String bucketName, String key, AccessControlList acl)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public void setObjectAcl(String bucketName, String key, CannedAccessControlList acl)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public void setObjectAcl(String bucketName, String key, String versionId, AccessControlList acl)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public void setObjectAcl(String bucketName, String key, String versionId, CannedAccessControlList acl)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public AccessControlList getBucketAcl(String bucketName)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public void setBucketAcl(SetBucketAclRequest setBucketAclRequest)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public AccessControlList getBucketAcl(GetBucketAclRequest getBucketAclRequest)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public void setBucketAcl(String bucketName, AccessControlList acl)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public void setBucketAcl(String bucketName, CannedAccessControlList acl)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public ObjectMetadata getObjectMetadata(String bucketName, String key)
                throws AmazonClientException, AmazonServiceException
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
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public S3Object getObject(String bucketName, String key)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public S3Object getObject(GetObjectRequest getObjectRequest)
                throws AmazonClientException, AmazonServiceException
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
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public void deleteBucket(DeleteBucketRequest deleteBucketRequest)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public void deleteBucket(String bucketName)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public PutObjectResult putObject(PutObjectRequest putObjectRequest)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public PutObjectResult putObject(String bucketName, String key, File file)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public CopyPartResult copyPart(CopyPartRequest copyPartRequest)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public void deleteObject(String bucketName, String key)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public void deleteObject(DeleteObjectRequest deleteObjectRequest)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public void deleteVersion(String bucketName, String key, String versionId)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public void deleteVersion(DeleteVersionRequest deleteVersionRequest)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public BucketLoggingConfiguration getBucketLoggingConfiguration(String bucketName)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public void setBucketLoggingConfiguration(SetBucketLoggingConfigurationRequest setBucketLoggingConfigurationRequest)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public BucketVersioningConfiguration getBucketVersioningConfiguration(String bucketName)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public void setBucketVersioningConfiguration(SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest)
                throws AmazonClientException, AmazonServiceException
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
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public void setBucketNotificationConfiguration(SetBucketNotificationConfigurationRequest setBucketNotificationConfigurationRequest)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public void setBucketNotificationConfiguration(String bucketName, BucketNotificationConfiguration bucketNotificationConfiguration)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public BucketWebsiteConfiguration getBucketWebsiteConfiguration(String bucketName)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public BucketWebsiteConfiguration getBucketWebsiteConfiguration(GetBucketWebsiteConfigurationRequest getBucketWebsiteConfigurationRequest)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public void setBucketWebsiteConfiguration(String bucketName, BucketWebsiteConfiguration configuration)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public void setBucketWebsiteConfiguration(SetBucketWebsiteConfigurationRequest setBucketWebsiteConfigurationRequest)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public void deleteBucketWebsiteConfiguration(String bucketName)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public void deleteBucketWebsiteConfiguration(DeleteBucketWebsiteConfigurationRequest deleteBucketWebsiteConfigurationRequest)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public BucketPolicy getBucketPolicy(String bucketName)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public BucketPolicy getBucketPolicy(GetBucketPolicyRequest getBucketPolicyRequest)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public void setBucketPolicy(String bucketName, String policyText)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public void setBucketPolicy(SetBucketPolicyRequest setBucketPolicyRequest)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public void deleteBucketPolicy(String bucketName)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public void deleteBucketPolicy(DeleteBucketPolicyRequest deleteBucketPolicyRequest)
                throws AmazonClientException, AmazonServiceException
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
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public UploadPartResult uploadPart(UploadPartRequest request)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public PartListing listParts(ListPartsRequest request)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public void abortMultipartUpload(AbortMultipartUploadRequest request)
                throws AmazonClientException, AmazonServiceException
        {
        }

        @Override
        public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request)
                throws AmazonClientException, AmazonServiceException
        {
            return null;
        }

        @Override
        public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request)
                throws AmazonClientException, AmazonServiceException
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
                throws AmazonServiceException, AmazonClientException
        {
        }

        @Override
        public void disableRequesterPays(String bucketName)
                throws AmazonServiceException, AmazonClientException
        {
        }

        @Override
        public boolean isRequesterPaysEnabled(String bucketName)
                throws AmazonServiceException, AmazonClientException
        {
            return false;
        }
    }
}
