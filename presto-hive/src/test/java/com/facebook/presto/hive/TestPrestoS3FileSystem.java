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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.EncryptionMaterialsProvider;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.testng.SkipException;
import org.testng.annotations.Test;

import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static com.facebook.presto.hive.PrestoS3FileSystem.S3_ENCRYPTION_MATERIALS_PROVIDER;
import static com.facebook.presto.hive.PrestoS3FileSystem.S3_MAX_BACKOFF_TIME;
import static com.facebook.presto.hive.PrestoS3FileSystem.S3_MAX_CLIENT_RETRIES;
import static com.facebook.presto.hive.PrestoS3FileSystem.S3_MAX_RETRY_TIME;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static org.apache.http.HttpStatus.SC_FORBIDDEN;
import static org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPrestoS3FileSystem
{
    @Test
    public void testStaticCredentials()
            throws Exception
    {
        Configuration config = new Configuration();
        config.set(PrestoS3FileSystem.S3_ACCESS_KEY, "test_secret_access_key");
        config.set(PrestoS3FileSystem.S3_SECRET_KEY, "test_access_key_id");
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

    @SuppressWarnings({"ResultOfMethodCallIgnored", "OverlyStrongTypeCast", "ConstantConditions"})
    @Test
    public void testReadRetryCounters()
            throws Exception
    {
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            int maxRetries = 2;
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectHttpErrorCode(SC_INTERNAL_SERVER_ERROR);
            Configuration configuration = new Configuration();
            configuration.set(S3_MAX_BACKOFF_TIME, "1ms");
            configuration.set(S3_MAX_RETRY_TIME, "5s");
            configuration.setInt(S3_MAX_CLIENT_RETRIES, maxRetries);
            fs.initialize(new URI("s3n://test-bucket/"), configuration);
            fs.setS3Client(s3);
            try (FSDataInputStream inputStream = fs.open(new Path("s3n://test-bucket/test"))) {
                inputStream.read();
            }
            catch (Throwable expected) {
                assertInstanceOf(expected, AmazonS3Exception.class);
                assertEquals(((AmazonS3Exception) expected).getStatusCode(), SC_INTERNAL_SERVER_ERROR);
                assertEquals(PrestoS3FileSystem.getFileSystemStats().getReadRetries().getTotalCount(), maxRetries);
                assertEquals(PrestoS3FileSystem.getFileSystemStats().getGetObjectRetries().getTotalCount(), (maxRetries + 1L) * maxRetries);
            }
        }
    }

    @SuppressWarnings({"OverlyStrongTypeCast", "ConstantConditions"})
    @Test
    public void testGetMetadataRetryCounter()
    {
        int maxRetries = 2;
        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            s3.setGetObjectMetadataHttpCode(SC_INTERNAL_SERVER_ERROR);
            Configuration configuration = new Configuration();
            configuration.set(S3_MAX_BACKOFF_TIME, "1ms");
            configuration.set(S3_MAX_RETRY_TIME, "5s");
            configuration.setInt(S3_MAX_CLIENT_RETRIES, maxRetries);
            fs.initialize(new URI("s3n://test-bucket/"), configuration);
            fs.setS3Client(s3);
            fs.getS3ObjectMetadata(new Path("s3n://test-bucket/test"));
        }
        catch (Throwable expected) {
            assertInstanceOf(expected, AmazonS3Exception.class);
            assertEquals(((AmazonS3Exception) expected).getStatusCode(), SC_INTERNAL_SERVER_ERROR);
            assertEquals(PrestoS3FileSystem.getFileSystemStats().getGetMetadataRetries().getTotalCount(), maxRetries);
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
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

    @SuppressWarnings("ResultOfMethodCallIgnored")
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
    public void testCreateWithNonexistentStagingDirectory()
        throws Exception
    {
        java.nio.file.Path tmpdir = Paths.get(StandardSystemProperty.JAVA_IO_TMPDIR.value());
        java.nio.file.Path stagingParent = Files.createTempDirectory(tmpdir, "test");
        java.nio.file.Path staging = Paths.get(stagingParent.toString(), "staging");
        // stagingParent = /tmp/testXXX
        // staging = /tmp/testXXX/staging

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            Configuration conf = new Configuration();
            conf.set(PrestoS3FileSystem.S3_STAGING_DIRECTORY, staging.toString());
            fs.initialize(new URI("s3n://test-bucket/"), conf);
            fs.setS3Client(s3);
            FSDataOutputStream stream = fs.create(new Path("s3n://test-bucket/test"));
            stream.close();
            assertTrue(Files.exists(staging));
        }
        finally {
            deleteRecursively(stagingParent.toFile());
        }
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = "Configured staging path is not a directory: .*")
    public void testCreateWithStagingDirectoryFile()
        throws Exception
    {
        java.nio.file.Path tmpdir = Paths.get(StandardSystemProperty.JAVA_IO_TMPDIR.value());
        java.nio.file.Path staging = Files.createTempFile(tmpdir, "staging", null);
        // staging = /tmp/stagingXXX.tmp

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            MockAmazonS3 s3 = new MockAmazonS3();
            Configuration conf = new Configuration();
            conf.set(PrestoS3FileSystem.S3_STAGING_DIRECTORY, staging.toString());
            fs.initialize(new URI("s3n://test-bucket/"), conf);
            fs.setS3Client(s3);
            fs.create(new Path("s3n://test-bucket/test"));
        }
        finally {
            Files.deleteIfExists(staging);
        }
    }

    @Test
    public void testCreateWithStagingDirectorySymlink()
        throws Exception
    {
        java.nio.file.Path tmpdir = Paths.get(StandardSystemProperty.JAVA_IO_TMPDIR.value());
        java.nio.file.Path staging = Files.createTempDirectory(tmpdir, "staging");
        java.nio.file.Path link = Paths.get(staging + ".symlink");
        // staging = /tmp/stagingXXX
        // link = /tmp/stagingXXX.symlink -> /tmp/stagingXXX

        try {
            try {
                Files.createSymbolicLink(link, staging);
            }
            catch (UnsupportedOperationException e) {
                throw new SkipException("Filesystem does not support symlinks", e);
            }

            try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
                MockAmazonS3 s3 = new MockAmazonS3();
                Configuration conf = new Configuration();
                conf.set(PrestoS3FileSystem.S3_STAGING_DIRECTORY, link.toString());
                fs.initialize(new URI("s3n://test-bucket/"), conf);
                fs.setS3Client(s3);
                FSDataOutputStream stream = fs.create(new Path("s3n://test-bucket/test"));
                stream.close();
                assertTrue(Files.exists(link));
            }
        }
        finally {
            deleteRecursively(link.toFile());
            deleteRecursively(staging.toFile());
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

    @Test
    public void testEncryptionMaterialsProvider()
            throws Exception
    {
        Configuration config = new Configuration();
        config.set(S3_ENCRYPTION_MATERIALS_PROVIDER, TestEncryptionMaterialsProvider.class.getName());

        try (PrestoS3FileSystem fs = new PrestoS3FileSystem()) {
            fs.initialize(new URI("s3n://test-bucket/"), config);
            assertInstanceOf(fs.getS3Client(), AmazonS3EncryptionClient.class);
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

    private static class TestEncryptionMaterialsProvider
            implements EncryptionMaterialsProvider
    {
        private final EncryptionMaterials encryptionMaterials;

        public TestEncryptionMaterialsProvider()
        {
            encryptionMaterials = new EncryptionMaterials(new SecretKeySpec(new byte[] {1, 2, 3}, "AES"));
        }

        @Override
        public void refresh()
        {
        }

        @Override
        public EncryptionMaterials getEncryptionMaterials(Map<String, String> materialsDescription)
        {
            return encryptionMaterials;
        }

        @Override
        public EncryptionMaterials getEncryptionMaterials()
        {
            return encryptionMaterials;
        }
    }
}
