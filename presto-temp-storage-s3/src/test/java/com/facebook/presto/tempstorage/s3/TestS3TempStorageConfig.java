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

import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MinDuration;
import com.google.common.collect.ImmutableMap;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.airlift.testing.ValidationAssertions.assertFailsValidation;
import static com.facebook.airlift.testing.ValidationAssertions.assertValidates;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestS3TempStorageConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(S3TempStorageConfig.class)
                .setBucket(null)
                .setKeyPrefix("")
                .setS3SslEnabled(true)
                .setRegion(null)
                .setEndpoint(null)
                .setIamRole(null)
                .setAwsAccessKey(null)
                .setAwsSecretKey(null)
                .setS3PathStyleAccess(false)
                .setS3MaxErrorRetries(10)
                .setS3ConnectTimeout(new Duration(5, SECONDS))
                .setS3SocketTimeout(new Duration(5, SECONDS))
                .setS3MaxConnections(500)
                .setS3ChunkedEncodingEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("s3.bucket", "test-bucket")
                .put("s3.key-prefix", "presto/temp")
                .put("s3.ssl.enabled", "false")
                .put("s3.region", "us-west-2")
                .put("s3.endpoint", "http://endpoint.example.com:9000")
                .put("s3.iam-role", "roleArn")
                .put("s3.aws-access-key", "abc123")
                .put("s3.aws-secret-key", "secret")
                .put("s3.path-style-access", "true")
                .put("s3.max-error-retries", "8")
                .put("s3.connect-timeout", "8s")
                .put("s3.socket-timeout", "4m")
                .put("s3.max-connections", "77")
                .put("s3.chunked-encoding-enabled", "false")
                .build();

        S3TempStorageConfig expected = new S3TempStorageConfig()
                .setBucket("test-bucket")
                .setKeyPrefix("presto/temp")
                .setS3SslEnabled(false)
                .setRegion("us-west-2")
                .setEndpoint("http://endpoint.example.com:9000")
                .setIamRole("roleArn")
                .setAwsAccessKey("abc123")
                .setAwsSecretKey("secret")
                .setS3PathStyleAccess(true)
                .setS3MaxErrorRetries(8)
                .setS3ConnectTimeout(new Duration(8, SECONDS))
                .setS3SocketTimeout(new Duration(4, MINUTES))
                .setS3MaxConnections(77)
                .setS3ChunkedEncodingEnabled(false);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        assertValidates(new S3TempStorageConfig());

        assertFailsValidation(
                new S3TempStorageConfig().setS3MaxConnections(0),
                "s3MaxConnections",
                "must be greater than or equal to 1",
                Min.class);

        assertFailsValidation(
                new S3TempStorageConfig().setS3MaxErrorRetries(-1),
                "s3MaxErrorRetries",
                "must be greater than or equal to 0",
                Min.class);

        assertFailsValidation(
                new S3TempStorageConfig().setS3ConnectTimeout(null),
                "s3ConnectTimeout",
                "must not be null",
                NotNull.class);

        assertFailsValidation(
                new S3TempStorageConfig().setS3SocketTimeout(null),
                "s3SocketTimeout",
                "must not be null",
                NotNull.class);

        assertFailsValidation(
                new S3TempStorageConfig().setS3ConnectTimeout(new Duration(0, MILLISECONDS)),
                "s3ConnectTimeout",
                "must be greater than or equal to 1ms",
                MinDuration.class);

        assertFailsValidation(
                new S3TempStorageConfig().setS3SocketTimeout(new Duration(0, MILLISECONDS)),
                "s3SocketTimeout",
                "must be greater than or equal to 1ms",
                MinDuration.class);
    }
}
