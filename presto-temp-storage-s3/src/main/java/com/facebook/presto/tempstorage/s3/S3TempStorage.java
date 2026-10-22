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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.storage.StorageCapabilities;
import com.facebook.presto.spi.storage.TempDataOperationContext;
import com.facebook.presto.spi.storage.TempDataSink;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.spi.storage.TempStorageHandle;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofMillis;
import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.core.sync.RequestBody.fromBytes;
import static software.amazon.awssdk.regions.Region.US_EAST_1;

public class S3TempStorage
        implements TempStorage
{
    private static final Logger log = Logger.get(S3TempStorage.class);

    private final S3Client s3Client;
    private final String bucket;
    private final String keyPrefix;
    private final String rootKey;
    private final S3TempStorageHandle rootHandle;

    @Inject
    public S3TempStorage(S3TempStorageConfig config)
    {
        this(createS3Client(config), config);
    }

    @VisibleForTesting
    protected S3TempStorage(S3Client s3Client, S3TempStorageConfig config)
    {
        this.s3Client = requireNonNull(s3Client, "s3Client is null");
        requireNonNull(config, "config is null");
        this.bucket = requireNonNull(config.getBucket(), "bucket is null");
        this.keyPrefix = normalizeKeyPrefix(config.getKeyPrefix());
        this.rootKey = keyPrefix.isEmpty() ? "" : keyPrefix + "/";
        this.rootHandle = new S3TempStorageHandle(bucket, rootKey);
    }

    @Override
    public TempDataSink create(TempDataOperationContext context)
    {
        String key = buildTemporaryKey(context.getQueryId());
        return new S3TempDataSink(s3Client, bucket, key);
    }

    @Override
    public TempDataSink create(TempDataOperationContext context, TempStorageHandle handle, boolean createFile)
    {
        S3TempStorageHandle storageHandle = toS3Handle(handle);
        return new S3TempDataSink(s3Client, storageHandle.getBucket(), storageHandle.getKey());
    }

    @Override
    public InputStream open(TempDataOperationContext context, TempStorageHandle handle)
            throws IOException
    {
        S3TempStorageHandle storageHandle = toS3Handle(handle);
        try {
            return s3Client.getObject(GetObjectRequest.builder()
                    .bucket(storageHandle.getBucket())
                    .key(storageHandle.getKey())
                    .build());
        }
        catch (SdkException e) {
            throw new IOException("Failed to open S3 temp storage object", e);
        }
    }

    @Override
    public void remove(TempDataOperationContext context, TempStorageHandle handle)
            throws IOException
    {
        S3TempStorageHandle storageHandle = toS3Handle(handle);
        try {
            s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(storageHandle.getBucket())
                    .key(storageHandle.getKey())
                    .build());
        }
        catch (SdkException e) {
            throw new IOException("Failed to remove S3 temp storage object", e);
        }
    }

    @Override
    public boolean exists(TempDataOperationContext context, TempStorageHandle handle)
            throws IOException
    {
        S3TempStorageHandle storageHandle = toS3Handle(handle);
        try {
            s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(storageHandle.getBucket())
                    .key(storageHandle.getKey())
                    .build());
            return true;
        }
        catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false;
            }
            throw new IOException("Failed to check existence for S3 temp storage object", e);
        }
        catch (SdkException e) {
            throw new IOException("Failed to check existence for S3 temp storage object", e);
        }
    }

    @Override
    public boolean createIfNotExists(TempDataOperationContext context, TempStorageHandle handle, byte[] data)
            throws IOException
    {
        S3TempStorageHandle storageHandle = toS3Handle(handle);
        try {
            PutObjectRequest.Builder request = PutObjectRequest.builder()
                    .bucket(storageHandle.getBucket())
                    .key(storageHandle.getKey())
                    .ifNoneMatch("*");

            s3Client.putObject(request.build(), fromBytes(data));
            return true;
        }
        catch (S3Exception e) {
            if (e.statusCode() == 412) {
                return false;
            }
            throw new IOException("Failed to create S3 temp storage object conditionally", e);
        }
        catch (SdkException e) {
            throw new IOException("Failed to create S3 temp storage object conditionally", e);
        }
    }

    @Override
    public TempStorageHandle getRootDirectoryHandle()
    {
        return rootHandle;
    }

    @Override
    public byte[] serializeHandle(TempStorageHandle storageHandle)
    {
        return toS3Handle(storageHandle).getKey().getBytes(UTF_8);
    }

    @Override
    public TempStorageHandle deserialize(byte[] serializedStorageHandle)
    {
        return new S3TempStorageHandle(bucket, new String(serializedStorageHandle, UTF_8));
    }

    @Override
    public List<StorageCapabilities> getStorageCapabilities()
    {
        return ImmutableList.of(StorageCapabilities.REMOTELY_ACCESSIBLE);
    }

    private String buildTemporaryKey(String queryId)
    {
        String suffix = "tmp/" + queryId + "/" + UUID.randomUUID();
        if (keyPrefix.isEmpty()) {
            return suffix;
        }
        return keyPrefix + "/" + suffix;
    }

    private static String normalizeKeyPrefix(String keyPrefix)
    {
        requireNonNull(keyPrefix, "keyPrefix is null");
        String normalized = keyPrefix.strip();
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private static S3TempStorageHandle toS3Handle(TempStorageHandle handle)
    {
        return (S3TempStorageHandle) requireNonNull(handle, "handle is null");
    }

    private static S3Client createS3Client(S3TempStorageConfig config)
    {
        requireNonNull(config, "config is null");

        Optional<String> endpoint = config.getEndpoint();
        URI endpointUri = null;
        boolean isHttpEndpoint = false;

        if (endpoint.isPresent()) {
            try {
                endpointUri = URI.create(endpoint.get());
                if (endpointUri.getScheme() == null) {
                    endpointUri = URI.create((config.isS3SslEnabled() ? "https://" : "http://") + endpoint.get());
                }
                isHttpEndpoint = "http".equalsIgnoreCase(endpointUri.getScheme());
            }
            catch (IllegalStateException e) {
                log.error("Invalid Temp Storage S3 endpoint: %s", endpoint.get());
                throw new RuntimeException(String.format("Invalid Temp Storage S3 endpoint: %s", endpoint.get()), e);
            }
        }

        Region region = config.getRegion() != null ? Region.of(config.getRegion()) : US_EAST_1;

        S3ClientBuilder builder = S3Client.builder()
                .credentialsProvider(createCredentialsProvider(config, endpointUri, region))
                .forcePathStyle(config.isS3PathStyleAccess());

        if (endpointUri != null) {
            builder.endpointOverride(endpointUri);
        }

        builder.region(region);
        if (config.getRegion() == null) {
            builder.crossRegionAccessEnabled(true);
        }

        S3Configuration s3Configuration = S3Configuration.builder()
                .chunkedEncodingEnabled(config.isS3ChunkedEncodingEnabled())
                .checksumValidationEnabled(!isHttpEndpoint)
                .build();
        builder.serviceConfiguration(s3Configuration);

        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder()
                .maxConnections(config.getS3MaxConnections())
                .connectionTimeout(ofMillis(config.getS3ConnectTimeout().toMillis()))
                .socketTimeout(ofMillis(config.getS3SocketTimeout().toMillis()));
        builder.httpClientBuilder(httpClientBuilder);

        StandardRetryStrategy strategy = AwsRetryStrategy.standardRetryStrategy()
                .toBuilder()
                .maxAttempts(config.getS3MaxErrorRetries())
                .build();
        builder.overrideConfiguration(b -> b.retryStrategy(strategy));

        return builder.build();
    }

    private static AwsCredentialsProvider createCredentialsProvider(S3TempStorageConfig config, URI endpointUri, Region region)
    {
        if (config.getAwsAccessKey().isPresent() || config.getAwsSecretKey().isPresent()) {
            if (config.getAwsAccessKey().isEmpty() || config.getAwsSecretKey().isEmpty()) {
                throw new IllegalArgumentException("Both s3.aws-access-key and s3.aws-secret-key must be set together");
            }
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(config.getAwsAccessKey().get(), config.getAwsSecretKey().get()));
        }

        if (config.getIamRole().isPresent()) {
            StsClientBuilder stsClientBuilder = StsClient.builder();

            if (endpointUri != null) {
                stsClientBuilder.endpointOverride(endpointUri);
            }
            stsClientBuilder.region(region);

            StsClient stsClient = stsClientBuilder.build();
            return StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(AssumeRoleRequest.builder()
                            .roleArn(config.getIamRole().get())
                            .roleSessionName("presto-temp-storage-s3")
                            .build())
                    .build();
        }

        return DefaultCredentialsProvider.create();
    }
}
