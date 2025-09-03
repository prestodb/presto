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
package com.facebook.presto.iceberg.container;

import com.facebook.presto.testing.containers.MinIOContainer;
import com.facebook.presto.util.AutoCloseableCloser;
import com.google.common.collect.ImmutableMap;
import org.testcontainers.containers.Network;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.Network.newNetwork;

public class IcebergMinIODataLake
        implements Closeable
{
    public static final String ACCESS_KEY = "minioadmin";
    public static final String SECRET_KEY = "minioadmin";

    private final String bucketName;
    private final String warehouseDir;
    private final MinIOContainer minIOContainer;
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final AutoCloseableCloser closer = AutoCloseableCloser.create();

    public IcebergMinIODataLake(String bucketName, String warehouseDir)
    {
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        this.warehouseDir = requireNonNull(warehouseDir, "warehouseDir is null");
        Network network = closer.register(newNetwork());
        this.minIOContainer = closer.register(
                MinIOContainer.builder()
                        .withNetwork(network)
                        .withEnvVars(ImmutableMap.<String, String>builder()
                                .put("MINIO_ACCESS_KEY", ACCESS_KEY)
                                .put("MINIO_SECRET_KEY", SECRET_KEY)
                                .build())
                        .build());
    }

    public void start()
    {
        if (isStarted()) {
            return;
        }

        try {
            this.minIOContainer.start();

            S3Client s3Client = S3Client.builder()
                    .endpointOverride(URI.create("http://localhost:" + minIOContainer.getMinioApiEndpoint().getPort()))
                    .region(Region.US_EAST_1)
                    .forcePathStyle(true)
                    .serviceConfiguration(S3Configuration.builder()
                            // Disable checksum validation and chunked encoding for MinIO compatibility
                            // MinIO checksum handling differs from AWS S3
                            // Prevents chunked transfer encoding issues with MinIO
                            .checksumValidationEnabled(false)
                            .chunkedEncodingEnabled(false)
                            .build())
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                    .build();

            s3Client.createBucket(CreateBucketRequest.builder()
                    .bucket(this.bucketName)
                    .build());
            String objectKey = this.warehouseDir.endsWith("/")
                    ? this.warehouseDir + ".keep"
                    : this.warehouseDir + "/.keep";

            s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(this.bucketName)
                            .key(objectKey)
                            .build(),
                    RequestBody.fromString("placeholder"));
            closer.register(s3Client);
        }
        finally {
            isStarted.set(true);
        }
    }

    public boolean isStarted()
    {
        return isStarted.get();
    }

    public void stop()
    {
        if (!isStarted()) {
            return;
        }
        try {
            closer.close();
            isStarted.set(false);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to stop IcebergMinioDataLake", e);
        }
    }

    public MinIOContainer getMinio()
    {
        return minIOContainer;
    }

    @Override
    public void close()
            throws IOException
    {
        stop();
    }
}
