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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import io.findify.s3mock.S3Mock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.UUID;

import static com.facebook.presto.testing.TestngUtils.findUnusedPort;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPrestoS3FileSystemWithS3Mock
{
    private S3Mock api;
    private AwsClientBuilder.EndpointConfiguration endpoint;

    @BeforeClass
    public void beforeClass()
            throws IOException
    {
        int port = findUnusedPort();
        api = new S3Mock.Builder().withPort(port).withInMemoryBackend().build();
        endpoint = new AwsClientBuilder.EndpointConfiguration("http://localhost:" + port, "us-west-2");
        api.start();
    }

    @AfterClass
    public void afterTest()
    {
        api.shutdown();
    }

    @Test
    public void testExistsBucket()
            throws Exception
    {
        final AmazonS3 client = amazonS3Client();
        final String emptyBucket = randomBucketName();
        final String bucketWithDir = randomBucketName();
        final String bucketWithFile = randomBucketName();
        client.createBucket(emptyBucket);
        client.createBucket(bucketWithDir);
        client.createBucket(bucketWithFile);
        createFolder(bucketWithDir, "dir", client);
        client.putObject(bucketWithFile, "file.txt", "content");

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI(format("s3n://%s/", emptyBucket)), new Configuration());
            fs.setS3Client(client);
            assertTrue(fs.exists(new Path(format("s3n://%s", emptyBucket))));
            assertTrue(fs.exists(new Path(format("s3n://%s/", emptyBucket))));
            assertTrue(fs.exists(new Path("/")));

            fs.initialize(new URI(format("s3n://%s/", bucketWithDir)), new Configuration());
            fs.setS3Client(client);
            assertTrue(fs.exists(new Path(format("s3n://%s", bucketWithDir))));
            assertTrue(fs.exists(new Path(format("s3n://%s/", bucketWithDir))));
            assertTrue(fs.exists(new Path("/")));

            fs.initialize(new URI(format("s3n://%s/", bucketWithFile)), new Configuration());
            fs.setS3Client(client);
            assertTrue(fs.exists(new Path(format("s3n://%s", bucketWithFile))));
            assertTrue(fs.exists(new Path(format("s3n://%s/", bucketWithFile))));
            assertTrue(fs.exists(new Path("/")));

            fs.initialize(new URI("s3n://unknown-bucket/"), new Configuration());
            fs.setS3Client(client);
            assertFalse(fs.exists(new Path("s3n://unknown-bucket")));
            assertFalse(fs.exists(new Path("s3n://unknown-bucket/")));
            assertFalse(fs.exists(new Path("/")));
        }
    }

    @Test
    public void testExistsDirectory()
            throws Exception
    {
        final AmazonS3 client = amazonS3Client();
        final String testBucket = randomBucketName();
        final String testBucketPath = format("s3n://%s/", testBucket);
        client.createBucket(testBucket);
        createFolder(testBucket, "empty-dir", client);
        client.putObject(testBucket, "dir-with-file/file.txt", "content");
        createFolder(testBucket, "dir-with-dir/dir", client);

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI(testBucketPath), new Configuration());
            fs.setS3Client(client);
            assertTrue(fs.exists(new Path(testBucketPath + "/empty-dir")));
            assertTrue(fs.exists(new Path(testBucketPath + "/empty-dir/")));

            assertTrue(fs.exists(new Path(testBucketPath + "/dir-with-file")));
            assertTrue(fs.exists(new Path(testBucketPath + "/dir-with-file/")));

            assertTrue(fs.exists(new Path(testBucketPath + "/dir-with-dir")));
            assertTrue(fs.exists(new Path(testBucketPath + "/dir-with-dir/")));

            assertFalse(fs.exists(new Path(testBucketPath + "/unknown-dir")));
            assertFalse(fs.exists(new Path(testBucketPath + "/unknown-dir/")));
        }
    }

    @Test
    public void testExistsFile()
            throws Exception
    {
        final AmazonS3 client = amazonS3Client();
        final String testBucket = randomBucketName();
        final String testBucketPath = format("s3n://%s/", testBucket);
        client.createBucket(testBucket);
        client.putObject(testBucket, "dir/file-inside-dir.txt", "content");
        client.putObject(testBucket, "file-inside-bucket.txt", "content");

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI(testBucketPath), new Configuration());
            fs.setS3Client(client);

            assertTrue(fs.exists(new Path(testBucketPath + "/dir/file-inside-dir.txt")));
            assertTrue(fs.exists(new Path(testBucketPath + "/file-inside-bucket.txt")));
            assertFalse(fs.exists(new Path(testBucketPath + "/unknown-file.txt")));
        }
    }

    @Test
    public void testFileStatusEmpty()
            throws Exception
    {
        final AmazonS3 client = amazonS3Client();
        final String testBucket = randomBucketName();
        final String testBucketPath = format("s3n://%s/", testBucket);
        client.createBucket(testBucket);
        createFolder(testBucket, "test", client);

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI(testBucketPath), new Configuration());
            fs.setS3Client(client);
            FileStatus fileStatus = fs.getFileStatus(new Path(testBucketPath + "/test"));
            assertTrue(fileStatus.isDirectory());
        }
    }

    @Test(expectedExceptions = FileNotFoundException.class)
    public void testFileStatusNotFound()
            throws Exception
    {
        final AmazonS3 client = amazonS3Client();
        final String testBucket = randomBucketName();
        final String testBucketPath = format("s3n://%s/", testBucket);
        client.createBucket(testBucket);

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI(testBucketPath), new Configuration());
            fs.setS3Client(client);
            fs.getFileStatus(new Path(testBucketPath + "/test"));
        }
    }

    @Test
    public void testFileStatusWithDir()
            throws Exception
    {
        final AmazonS3 client = amazonS3Client();
        final String testBucket = randomBucketName();
        final String testBucketPath = format("s3n://%s/", testBucket);
        client.createBucket(testBucket);
        createFolder(testBucket, "test/dir", client);

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI(testBucketPath), new Configuration());
            fs.setS3Client(client);
            FileStatus fileStatus = fs.getFileStatus(new Path(testBucketPath + "/test"));
            assertTrue(fileStatus.isDirectory());
        }
    }

    @Test
    public void testFileStatusWithFile()
            throws Exception
    {
        final AmazonS3 client = amazonS3Client();
        final String testBucket = randomBucketName();
        final String testBucketPath = format("s3n://%s/", testBucket);
        client.createBucket(testBucket);
        client.putObject(testBucket, "test/dir/file", "content");

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI(testBucketPath), new Configuration());
            fs.setS3Client(client);
            FileStatus fileStatus = fs.getFileStatus(new Path(testBucketPath + "/test"));
            assertTrue(fileStatus.isDirectory());
        }
    }

    @Test
    public void testListPrefix()
            throws Exception
    {
        final AmazonS3 client = amazonS3Client();
        final String testBucket = randomBucketName();
        final String testBucketPath = format("s3n://%s/", testBucket);
        client.createBucket(testBucket);
        client.putObject(testBucket, "test/dir/file1", "content");
        client.putObject(testBucket, "test/dir/file2", "content");
        client.putObject(testBucket, "test/dir/file3", "content");
        client.putObject(testBucket, "test/dir2/file4", "content");
        client.putObject(testBucket, "test/dir3/file5", "content");
        client.putObject(testBucket, "test/file6", "content");

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI(testBucketPath), new Configuration());
            fs.setS3Client(client);
            Iterator<LocatedFileStatus> fileStatuses = fs.listPrefix(new Path(testBucketPath + "/test"));
            int dirCount = 0;
            int fileCount = 0;
            while (fileStatuses.hasNext()) {
                if (fileStatuses.next().isDirectory()) {
                    dirCount++;
                }
                else {
                    fileCount++;
                }
            }
            assertEquals(dirCount, 3);
            assertEquals(fileCount, 1);
        }
    }

    @Test
    public void testListPrefixBucket()
            throws Exception
    {
        final AmazonS3 client = amazonS3Client();
        final String testBucket = randomBucketName();
        final String testBucketPath = format("s3n://%s/", testBucket);
        client.createBucket(testBucket);
        client.putObject(testBucket, "test/dir/file1", "content");
        client.putObject(testBucket, "test/dir/file2", "content");
        client.putObject(testBucket, "test/dir/file3", "content");
        client.putObject(testBucket, "test/dir2/file4", "content");
        client.putObject(testBucket, "test/dir3/file5", "content");
        client.putObject(testBucket, "test/file6", "content");
        client.putObject(testBucket, "file7", "content");

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI(testBucketPath), new Configuration());
            fs.setS3Client(client);
            Iterator<LocatedFileStatus> fileStatuses = fs.listPrefix(new Path(testBucketPath));
            int dirCount = 0;
            int fileCount = 0;
            while (fileStatuses.hasNext()) {
                if (fileStatuses.next().isDirectory()) {
                    dirCount++;
                }
                else {
                    fileCount++;
                }
            }
            assertEquals(dirCount, 1);
            assertEquals(fileCount, 1);
        }
    }

    @Test
    public void testListPrefixEmptyDir()
            throws Exception
    {
        final AmazonS3 client = amazonS3Client();
        final String testBucket = randomBucketName();
        final String testBucketPath = format("s3n://%s/", testBucket);
        client.createBucket(testBucket);
        createFolder(testBucket, "test", client);

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI(testBucketPath), new Configuration());
            fs.setS3Client(client);
            Iterator<LocatedFileStatus> fileStatuses = fs.listPrefix(new Path(testBucketPath + "/test"));
            assertFalse(fileStatuses.hasNext());
        }
    }

    @Test
    public void testListPrefixNotFound()
            throws Exception
    {
        final AmazonS3 client = amazonS3Client();
        final String testBucket = randomBucketName();
        final String testBucketPath = format("s3n://%s/", testBucket);
        client.createBucket(testBucket);

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI(testBucketPath), new Configuration());
            fs.setS3Client(client);
            Iterator<LocatedFileStatus> fileStatuses = fs.listPrefix(new Path(testBucketPath + "/test"));
            assertFalse(fileStatuses.hasNext());
        }
    }

    public AmazonS3 amazonS3Client()
    {
        return AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(endpoint)
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .build();
    }

    private String randomBucketName()
    {
        return "bucket" + UUID.randomUUID().toString();
    }

    private static void createFolder(String bucketName, String folderName, AmazonS3 client)
    {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, folderName + "/", emptyContent, metadata);
        client.putObject(putObjectRequest);
    }
}
