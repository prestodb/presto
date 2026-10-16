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
package com.facebook.presto.tempstorage.s3;

import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.storage.TempDataOperationContext;
import org.testng.annotations.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.util.Optional;

import static java.util.Collections.emptySet;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestS3TempStorage
{
    private static final TempDataOperationContext CONTEXT = new TempDataOperationContext(
            Optional.of("source"),
            "query-123",
            Optional.of("client-info"),
            Optional.of(emptySet()),
            new Identity("user", Optional.empty()));

    @Test
    public void testExistsReturnsTrueOnHeadSuccess()
            throws IOException
    {
        TestingS3Client s3Client = new TestingS3Client();
        s3Client.headResponse = HeadObjectResponse.builder().build();
        S3TempStorage storage = createStorage(s3Client);

        boolean exists = storage.exists(CONTEXT, new S3TempStorageHandle("test-bucket", "cache/metadata.json"));

        assertTrue(exists);
    }

    @Test
    public void testExistsReturnsFalseOn404()
            throws IOException
    {
        TestingS3Client s3Client = new TestingS3Client();
        s3Client.headException = s3Exception(404);
        S3TempStorage storage = createStorage(s3Client);

        boolean exists = storage.exists(CONTEXT, new S3TempStorageHandle("test-bucket", "cache/metadata.json"));

        assertFalse(exists);
    }

    @Test
    public void testCreateIfNotExistsReturnsTrueOnSuccess()
            throws IOException
    {
        TestingS3Client s3Client = new TestingS3Client();
        S3TempStorage storage = createStorage(s3Client);

        boolean created = storage.createIfNotExists(CONTEXT, new S3TempStorageHandle("test-bucket", "cache/lock"), new byte[] {1, 2, 3});

        assertTrue(created);
    }

    @Test
    public void testCreateIfNotExistsReturnsFalseOn412()
            throws IOException
    {
        TestingS3Client s3Client = new TestingS3Client();
        s3Client.putException = s3Exception(412);
        S3TempStorage storage = createStorage(s3Client);

        boolean created = storage.createIfNotExists(CONTEXT, new S3TempStorageHandle("test-bucket", "cache/lock"), new byte[] {1, 2, 3});

        assertFalse(created);
    }

    @Test
    public void testCreateIfNotExistsThrowsIOExceptionOnOtherS3Exception()
    {
        TestingS3Client s3Client = new TestingS3Client();
        s3Client.putException = s3Exception(500);
        S3TempStorage storage = createStorage(s3Client);

        expectThrows(IOException.class, () -> storage.createIfNotExists(CONTEXT, new S3TempStorageHandle("test-bucket", "cache/lock"), new byte[] {1, 2, 3}));
    }

    private static S3TempStorage createStorage(S3Client s3Client)
    {
        S3TempStorageConfig config = new S3TempStorageConfig()
                .setBucket("test-bucket")
                .setKeyPrefix("presto/temp");
        return new S3TempStorage(s3Client, config);
    }

    private static S3Exception s3Exception(int statusCode)
    {
        return (S3Exception) S3Exception.builder()
                .statusCode(statusCode)
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("test").build())
                .message("test")
                .build();
    }

    private static class TestingS3Client
            implements S3Client
    {
        private HeadObjectResponse headResponse = HeadObjectResponse.builder().build();
        private S3Exception headException;
        private S3Exception putException;

        @Override
        public HeadObjectResponse headObject(HeadObjectRequest headObjectRequest)
        {
            if (headException != null) {
                throw headException;
            }
            return headResponse;
        }

        @Override
        public PutObjectResponse putObject(PutObjectRequest putObjectRequest, RequestBody requestBody)
        {
            if (putException != null) {
                throw putException;
            }
            return PutObjectResponse.builder().build();
        }

        @Override
        public String serviceName()
        {
            return "s3";
        }

        @Override
        public void close()
        {
        }
    }
}
