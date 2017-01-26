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

import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestHiveS3Config
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HiveS3Config.class)
                .setS3AwsAccessKey(null)
                .setS3AwsSecretKey(null)
                .setS3Endpoint(null)
                .setS3SignerType(null)
                .setS3UseInstanceCredentials(true)
                .setS3SslEnabled(true)
                .setS3SseEnabled(false)
                .setS3KmsKeyId(null)
                .setS3EncryptionMaterialsProvider(null)
                .setS3MaxClientRetries(3)
                .setS3MaxErrorRetries(10)
                .setS3MaxBackoffTime(new Duration(10, TimeUnit.MINUTES))
                .setS3MaxRetryTime(new Duration(10, TimeUnit.MINUTES))
                .setS3ConnectTimeout(new Duration(5, TimeUnit.SECONDS))
                .setS3SocketTimeout(new Duration(5, TimeUnit.SECONDS))
                .setS3MultipartMinFileSize(new DataSize(16, Unit.MEGABYTE))
                .setS3MultipartMinPartSize(new DataSize(5, Unit.MEGABYTE))
                .setS3MaxConnections(500)
                .setS3StagingDirectory(new File(StandardSystemProperty.JAVA_IO_TMPDIR.value()))
                .setPinS3ClientToCurrentRegion(false)
                .setS3UserAgentPrefix(""));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.s3.aws-access-key", "abc123")
                .put("hive.s3.aws-secret-key", "secret")
                .put("hive.s3.endpoint", "endpoint.example.com")
                .put("hive.s3.signer-type", "S3SignerType")
                .put("hive.s3.use-instance-credentials", "false")
                .put("hive.s3.ssl.enabled", "false")
                .put("hive.s3.sse.enabled", "true")
                .put("hive.s3.encryption-materials-provider", "EMP_CLASS")
                .put("hive.s3.kms-key-id", "KEY_ID")
                .put("hive.s3.max-client-retries", "9")
                .put("hive.s3.max-error-retries", "8")
                .put("hive.s3.max-backoff-time", "4m")
                .put("hive.s3.max-retry-time", "20m")
                .put("hive.s3.connect-timeout", "8s")
                .put("hive.s3.socket-timeout", "4m")
                .put("hive.s3.multipart.min-file-size", "32MB")
                .put("hive.s3.multipart.min-part-size", "15MB")
                .put("hive.s3.max-connections", "77")
                .put("hive.s3.staging-directory", "/s3-staging")
                .put("hive.s3.pin-client-to-current-region", "true")
                .put("hive.s3.user-agent-prefix", "user-agent-prefix")
                .build();

        HiveS3Config expected = new HiveS3Config()
                .setS3AwsAccessKey("abc123")
                .setS3AwsSecretKey("secret")
                .setS3Endpoint("endpoint.example.com")
                .setS3SignerType(PrestoS3SignerType.S3SignerType)
                .setS3UseInstanceCredentials(false)
                .setS3SslEnabled(false)
                .setS3SseEnabled(true)
                .setS3EncryptionMaterialsProvider("EMP_CLASS")
                .setS3KmsKeyId("KEY_ID")
                .setS3MaxClientRetries(9)
                .setS3MaxErrorRetries(8)
                .setS3MaxBackoffTime(new Duration(4, TimeUnit.MINUTES))
                .setS3MaxRetryTime(new Duration(20, TimeUnit.MINUTES))
                .setS3ConnectTimeout(new Duration(8, TimeUnit.SECONDS))
                .setS3SocketTimeout(new Duration(4, TimeUnit.MINUTES))
                .setS3MultipartMinFileSize(new DataSize(32, Unit.MEGABYTE))
                .setS3MultipartMinPartSize(new DataSize(15, Unit.MEGABYTE))
                .setS3MaxConnections(77)
                .setS3StagingDirectory(new File("/s3-staging"))
                .setPinS3ClientToCurrentRegion(true)
                .setS3UserAgentPrefix("user-agent-prefix");

        assertFullMapping(properties, expected);
    }
}
