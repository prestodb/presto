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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MinDuration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class S3TempStorageConfig
{
    private String bucket;
    private String keyPrefix = "";
    private boolean s3SslEnabled = true;
    private String region;
    private Optional<String> endpoint = Optional.empty();
    private Optional<String> iamRole = Optional.empty();
    private Optional<String> awsAccessKey = Optional.empty();
    private Optional<String> awsSecretKey = Optional.empty();
    private boolean s3PathStyleAccess;
    private int s3MaxErrorRetries = 10;
    private Duration s3ConnectTimeout = new Duration(5, TimeUnit.SECONDS);
    private Duration s3SocketTimeout = new Duration(5, TimeUnit.SECONDS);
    private int s3MaxConnections = 500;
    private boolean s3ChunkedEncodingEnabled = true;

    public boolean isS3ChunkedEncodingEnabled()
    {
        return s3ChunkedEncodingEnabled;
    }

    @Config("s3.chunked-encoding-enabled")
    @ConfigDescription("Use chunked encoding for S3 uploads. Disable for S3-compatible storage that doesn't support AWS chunked encoding")
    public S3TempStorageConfig setS3ChunkedEncodingEnabled(boolean s3ChunkedEncodingEnabled)
    {
        this.s3ChunkedEncodingEnabled = s3ChunkedEncodingEnabled;
        return this;
    }

    @Min(1)
    public int getS3MaxConnections()
    {
        return s3MaxConnections;
    }

    @Config("s3.max-connections")
    @ConfigDescription("Maximum number of concurrent HTTP connections to S3.")
    public S3TempStorageConfig setS3MaxConnections(int s3MaxConnections)
    {
        this.s3MaxConnections = s3MaxConnections;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getS3SocketTimeout()
    {
        return s3SocketTimeout;
    }

    @Config("s3.socket-timeout")
    @ConfigDescription("Socket read/write timeout for S3 operations.")
    public S3TempStorageConfig setS3SocketTimeout(Duration s3SocketTimeout)
    {
        this.s3SocketTimeout = s3SocketTimeout;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getS3ConnectTimeout()
    {
        return s3ConnectTimeout;
    }

    @Config("s3.connect-timeout")
    @ConfigDescription("Timeout for establishing a connection to S3.")
    public S3TempStorageConfig setS3ConnectTimeout(Duration s3ConnectTimeout)
    {
        this.s3ConnectTimeout = s3ConnectTimeout;
        return this;
    }

    @Min(0)
    public int getS3MaxErrorRetries()
    {
        return s3MaxErrorRetries;
    }

    @Config("s3.max-error-retries")
    @ConfigDescription("Maximum number of retries for retryable S3 service errors.")
    public S3TempStorageConfig setS3MaxErrorRetries(int s3MaxErrorRetries)
    {
        this.s3MaxErrorRetries = s3MaxErrorRetries;
        return this;
    }

    public boolean isS3PathStyleAccess()
    {
        return s3PathStyleAccess;
    }

    @Config("s3.path-style-access")
    @ConfigDescription("Use path-style access for all request to S3")
    public S3TempStorageConfig setS3PathStyleAccess(boolean s3PathStyleAccess)
    {
        this.s3PathStyleAccess = s3PathStyleAccess;
        return this;
    }

    public String getBucket()
    {
        return bucket;
    }

    @Config("s3.bucket")
    @ConfigDescription("Name of the S3 bucket used for temporary storage.")
    public S3TempStorageConfig setBucket(String bucket)
    {
        this.bucket = bucket;
        return this;
    }

    public String getKeyPrefix()
    {
        return keyPrefix;
    }

    @Config("s3.key-prefix")
    @ConfigDescription("Prefix applied to all objects written to the bucket. Useful for isolating data within a shared bucket.")
    public S3TempStorageConfig setKeyPrefix(String keyPrefix)
    {
        this.keyPrefix = keyPrefix;
        return this;
    }

    public boolean isS3SslEnabled()
    {
        return s3SslEnabled;
    }

    @Config("s3.ssl.enabled")
    @ConfigDescription("Enables SSL/TLS for communication with S3.")
    public S3TempStorageConfig setS3SslEnabled(boolean s3SslEnabled)
    {
        this.s3SslEnabled = s3SslEnabled;
        return this;
    }

    public String getRegion()
    {
        return region;
    }

    @Config("s3.region")
    @ConfigDescription("AWS region of the S3 bucket.")
    public S3TempStorageConfig setRegion(String region)
    {
        this.region = region;
        return this;
    }

    public Optional<String> getEndpoint()
    {
        return endpoint;
    }

    @Config("s3.endpoint")
    @ConfigDescription("Custom S3 endpoint. Useful for S3-compatible object stores such as MinIO, Ceph, or other cloud providers.")
    public S3TempStorageConfig setEndpoint(String endpoint)
    {
        this.endpoint = Optional.ofNullable(endpoint);
        return this;
    }

    public Optional<String> getIamRole()
    {
        return iamRole;
    }

    @Config("s3.iam-role")
    @ConfigDescription("IAM role to assume when accessing S3.")
    public S3TempStorageConfig setIamRole(String iamRole)
    {
        this.iamRole = Optional.ofNullable(iamRole);
        return this;
    }

    public Optional<String> getAwsAccessKey()
    {
        return awsAccessKey;
    }

    @Config("s3.aws-access-key")
    @ConfigDescription("AWS access key used for authentication.")
    @ConfigSecuritySensitive
    public S3TempStorageConfig setAwsAccessKey(String awsAccessKey)
    {
        this.awsAccessKey = Optional.ofNullable(awsAccessKey);
        return this;
    }

    public Optional<String> getAwsSecretKey()
    {
        return awsSecretKey;
    }

    @Config("s3.aws-secret-key")
    @ConfigDescription("AWS secret key used for authentication.")
    @ConfigSecuritySensitive
    public S3TempStorageConfig setAwsSecretKey(String awsSecretKey)
    {
        this.awsSecretKey = Optional.ofNullable(awsSecretKey);
        return this;
    }
}
