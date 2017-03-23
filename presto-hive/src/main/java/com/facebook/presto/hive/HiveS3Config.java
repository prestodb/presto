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
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class HiveS3Config
{
    private String s3AwsAccessKey;
    private String s3AwsSecretKey;
    private String s3Endpoint;
    private PrestoS3SignerType s3SignerType;
    private boolean s3UseInstanceCredentials = true;
    private boolean s3SslEnabled = true;
    private boolean s3SseEnabled;
    private PrestoS3SseType s3SseType = PrestoS3SseType.S3;
    private String s3EncryptionMaterialsProvider;
    private String s3KmsKeyId;
    private String s3SseKmsKeyId;
    private int s3MaxClientRetries = 5;
    private int s3MaxErrorRetries = 10;
    private Duration s3MaxBackoffTime = new Duration(10, TimeUnit.MINUTES);
    private Duration s3MaxRetryTime = new Duration(10, TimeUnit.MINUTES);
    private Duration s3ConnectTimeout = new Duration(5, TimeUnit.SECONDS);
    private Duration s3SocketTimeout = new Duration(5, TimeUnit.SECONDS);
    private int s3MaxConnections = 500;
    private File s3StagingDirectory = new File(StandardSystemProperty.JAVA_IO_TMPDIR.value());
    private DataSize s3MultipartMinFileSize = new DataSize(16, MEGABYTE);
    private DataSize s3MultipartMinPartSize = new DataSize(5, MEGABYTE);
    private boolean pinS3ClientToCurrentRegion;
    private String s3UserAgentPrefix = "";

    public String getS3AwsAccessKey()
    {
        return s3AwsAccessKey;
    }

    @Config("hive.s3.aws-access-key")
    public HiveS3Config setS3AwsAccessKey(String s3AwsAccessKey)
    {
        this.s3AwsAccessKey = s3AwsAccessKey;
        return this;
    }

    public String getS3AwsSecretKey()
    {
        return s3AwsSecretKey;
    }

    @Config("hive.s3.aws-secret-key")
    public HiveS3Config setS3AwsSecretKey(String s3AwsSecretKey)
    {
        this.s3AwsSecretKey = s3AwsSecretKey;
        return this;
    }

    public String getS3Endpoint()
    {
        return s3Endpoint;
    }

    @Config("hive.s3.endpoint")
    public HiveS3Config setS3Endpoint(String s3Endpoint)
    {
        this.s3Endpoint = s3Endpoint;
        return this;
    }

    public PrestoS3SignerType getS3SignerType()
    {
        return s3SignerType;
    }

    @Config("hive.s3.signer-type")
    public HiveS3Config setS3SignerType(PrestoS3SignerType s3SignerType)
    {
        this.s3SignerType = s3SignerType;
        return this;
    }

    public boolean isS3UseInstanceCredentials()
    {
        return s3UseInstanceCredentials;
    }

    @Config("hive.s3.use-instance-credentials")
    public HiveS3Config setS3UseInstanceCredentials(boolean s3UseInstanceCredentials)
    {
        this.s3UseInstanceCredentials = s3UseInstanceCredentials;
        return this;
    }

    public boolean isS3SslEnabled()
    {
        return s3SslEnabled;
    }

    @Config("hive.s3.ssl.enabled")
    public HiveS3Config setS3SslEnabled(boolean s3SslEnabled)
    {
        this.s3SslEnabled = s3SslEnabled;
        return this;
    }

    public String getS3EncryptionMaterialsProvider()
    {
        return s3EncryptionMaterialsProvider;
    }

    @Config("hive.s3.encryption-materials-provider")
    @ConfigDescription("Use a custom encryption materials provider for S3 data encryption")
    public HiveS3Config setS3EncryptionMaterialsProvider(String s3EncryptionMaterialsProvider)
    {
        this.s3EncryptionMaterialsProvider = s3EncryptionMaterialsProvider;
        return this;
    }

    public String getS3KmsKeyId()
    {
        return s3KmsKeyId;
    }

    @Config("hive.s3.kms-key-id")
    @ConfigDescription("Use an AWS KMS key for S3 data encryption")
    public HiveS3Config setS3KmsKeyId(String s3KmsKeyId)
    {
        this.s3KmsKeyId = s3KmsKeyId;
        return this;
    }

    public String getS3SseKmsKeyId()
    {
        return s3SseKmsKeyId;
    }

    @Config("hive.s3.sse.kms-key-id")
    @ConfigDescription("KMS Key ID to use for S3 server-side encryption with KMS-managed key")
    public HiveS3Config setS3SseKmsKeyId(String s3SseKmsKeyId)
    {
        this.s3SseKmsKeyId = s3SseKmsKeyId;
        return this;
    }

    public boolean isS3SseEnabled()
    {
        return s3SseEnabled;
    }

    @Config("hive.s3.sse.enabled")
    @ConfigDescription("Enable S3 server side encryption")
    public HiveS3Config setS3SseEnabled(boolean s3SseEnabled)
    {
        this.s3SseEnabled = s3SseEnabled;
        return this;
    }

    @NotNull
    public PrestoS3SseType getS3SseType()
    {
        return s3SseType;
    }

    @Config("hive.s3.sse.type")
    @ConfigDescription("Key management type for S3 server-side encryption (S3 or KMS)")
    public HiveS3Config setS3SseType(PrestoS3SseType s3SseType)
    {
        this.s3SseType = s3SseType;
        return this;
    }

    @Min(0)
    public int getS3MaxClientRetries()
    {
        return s3MaxClientRetries;
    }

    @Config("hive.s3.max-client-retries")
    public HiveS3Config setS3MaxClientRetries(int s3MaxClientRetries)
    {
        this.s3MaxClientRetries = s3MaxClientRetries;
        return this;
    }

    @Min(0)
    public int getS3MaxErrorRetries()
    {
        return s3MaxErrorRetries;
    }

    @Config("hive.s3.max-error-retries")
    public HiveS3Config setS3MaxErrorRetries(int s3MaxErrorRetries)
    {
        this.s3MaxErrorRetries = s3MaxErrorRetries;
        return this;
    }

    @MinDuration("1s")
    @NotNull
    public Duration getS3MaxBackoffTime()
    {
        return s3MaxBackoffTime;
    }

    @Config("hive.s3.max-backoff-time")
    public HiveS3Config setS3MaxBackoffTime(Duration s3MaxBackoffTime)
    {
        this.s3MaxBackoffTime = s3MaxBackoffTime;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getS3MaxRetryTime()
    {
        return s3MaxRetryTime;
    }

    @Config("hive.s3.max-retry-time")
    public HiveS3Config setS3MaxRetryTime(Duration s3MaxRetryTime)
    {
        this.s3MaxRetryTime = s3MaxRetryTime;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getS3ConnectTimeout()
    {
        return s3ConnectTimeout;
    }

    @Config("hive.s3.connect-timeout")
    public HiveS3Config setS3ConnectTimeout(Duration s3ConnectTimeout)
    {
        this.s3ConnectTimeout = s3ConnectTimeout;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getS3SocketTimeout()
    {
        return s3SocketTimeout;
    }

    @Config("hive.s3.socket-timeout")
    public HiveS3Config setS3SocketTimeout(Duration s3SocketTimeout)
    {
        this.s3SocketTimeout = s3SocketTimeout;
        return this;
    }

    @Min(1)
    public int getS3MaxConnections()
    {
        return s3MaxConnections;
    }

    @Config("hive.s3.max-connections")
    public HiveS3Config setS3MaxConnections(int s3MaxConnections)
    {
        this.s3MaxConnections = s3MaxConnections;
        return this;
    }

    @NotNull
    public File getS3StagingDirectory()
    {
        return s3StagingDirectory;
    }

    @Config("hive.s3.staging-directory")
    @ConfigDescription("Temporary directory for staging files before uploading to S3")
    public HiveS3Config setS3StagingDirectory(File s3StagingDirectory)
    {
        this.s3StagingDirectory = s3StagingDirectory;
        return this;
    }

    @NotNull
    @MinDataSize("16MB")
    public DataSize getS3MultipartMinFileSize()
    {
        return s3MultipartMinFileSize;
    }

    @Config("hive.s3.multipart.min-file-size")
    @ConfigDescription("Minimum file size for an S3 multipart upload")
    public HiveS3Config setS3MultipartMinFileSize(DataSize size)
    {
        this.s3MultipartMinFileSize = size;
        return this;
    }

    @NotNull
    @MinDataSize("5MB")
    public DataSize getS3MultipartMinPartSize()
    {
        return s3MultipartMinPartSize;
    }

    @Config("hive.s3.multipart.min-part-size")
    @ConfigDescription("Minimum part size for an S3 multipart upload")
    public HiveS3Config setS3MultipartMinPartSize(DataSize size)
    {
        this.s3MultipartMinPartSize = size;
        return this;
    }

    public boolean isPinS3ClientToCurrentRegion()
    {
        return pinS3ClientToCurrentRegion;
    }

    @Config("hive.s3.pin-client-to-current-region")
    @ConfigDescription("Should the S3 client be pinned to the current EC2 region")
    public HiveS3Config setPinS3ClientToCurrentRegion(boolean pinS3ClientToCurrentRegion)
    {
        this.pinS3ClientToCurrentRegion = pinS3ClientToCurrentRegion;
        return this;
    }

    @NotNull
    public String getS3UserAgentPrefix()
    {
        return s3UserAgentPrefix;
    }

    @Config("hive.s3.user-agent-prefix")
    @ConfigDescription("The user agent prefix to use for S3 calls")
    public HiveS3Config setS3UserAgentPrefix(String s3UserAgentPrefix)
    {
        this.s3UserAgentPrefix = s3UserAgentPrefix;
        return this;
    }
}
