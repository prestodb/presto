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
package com.facebook.presto.hive.containers;

import com.facebook.presto.testing.containers.MinIOContainer;
import com.facebook.presto.util.AutoCloseableCloser;
import com.google.common.collect.ImmutableMap;
import org.testcontainers.containers.Network;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.hive.containers.HiveHadoopContainer.HIVE3_IMAGE;
import static com.facebook.presto.tests.SslKeystoreManager.getKeystorePath;
import static com.facebook.presto.tests.SslKeystoreManager.getTruststorePath;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.Network.newNetwork;

public class HiveMinIODataLake
        implements Closeable
{
    public static final String ACCESS_KEY = "accesskey";
    public static final String SECRET_KEY = "secretkey";
    private static final Object SSL_LOCK = new Object();

    private final String bucketName;
    private final MinIOContainer minIOContainer;
    private final HiveHadoopContainer hiveHadoopContainer;

    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final AutoCloseableCloser closer = AutoCloseableCloser.create();

    public HiveMinIODataLake(String bucketName, Map<String, String> hiveHadoopFilesToMount)
    {
        this(bucketName, hiveHadoopFilesToMount, HiveHadoopContainer.DEFAULT_IMAGE, false);
    }

    public HiveMinIODataLake(String bucketName, Map<String, String> hiveHadoopFilesToMount, String hiveHadoopImage, boolean isSslEnabledTest)
    {
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        Network network = closer.register(newNetwork());
        this.minIOContainer = closer.register(
                MinIOContainer.builder()
                        .withNetwork(network)
                        .withEnvVars(ImmutableMap.<String, String>builder()
                                .put("MINIO_ACCESS_KEY", ACCESS_KEY)
                                .put("MINIO_SECRET_KEY", SECRET_KEY)
                                .build())
                        .build());

        ImmutableMap.Builder filesToMount = ImmutableMap.<String, String>builder()
                .putAll(hiveHadoopFilesToMount);

        String hadoopCoreSitePath = "/etc/hadoop/conf/core-site.xml";

        if (Objects.equals(hiveHadoopImage, HIVE3_IMAGE)) {
            hadoopCoreSitePath = "/opt/hadoop/etc/hadoop/core-site.xml";
            filesToMount.put("hive_s3_insert_overwrite/hive-site.xml", "/opt/hive/conf/hive-site.xml");
        }
        filesToMount.put("hive_s3_insert_overwrite/hadoop-core-site.xml", hadoopCoreSitePath);
        if (isSslEnabledTest) {
            try {
                // Copy dynamically generated keystore files into target/test-classes so that
                // Testcontainers can resolve them.
                // Without this step, the files would only exist on the filesystem and not
                // on the test runtime classpath, causing classpath lookups to fail.
                Path targetDir = Paths.get("target", "test-classes", "ssl_enable");
                Files.createDirectories(targetDir);

                Path keyStoreTarget = targetDir.resolve("keystore.jks");
                Path trustStoreTarget = targetDir.resolve("truststore.jks");

                synchronized (SSL_LOCK) {
                    // Copy freshly generated keystores, replacing if they exist
                    Files.copy(Paths.get(getKeystorePath()), keyStoreTarget, REPLACE_EXISTING);
                    Files.copy(Paths.get(getTruststorePath()), trustStoreTarget, REPLACE_EXISTING);

                    filesToMount.put("ssl_enable/keystore.jks", "/opt/hive/conf/hive-metastore.jks");
                    filesToMount.put("ssl_enable/truststore.jks", "/opt/hive/conf/hive-metastore-truststore.jks");
                }

                filesToMount.put("hive_ssl_enable/hive-site.xml", "/opt/hive/conf/hive-site.xml");
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to prepare keystore files for Testcontainers", e);
            }
        }
        this.hiveHadoopContainer = closer.register(
                HiveHadoopContainer.builder()
                        .withFilesToMount(filesToMount.build())
                        .withImage(hiveHadoopImage)
                        .withNetwork(network)
                        .build());
    }

    public void start()
    {
        if (isStarted()) {
            return;
        }
        try {
            this.minIOContainer.start();
            this.hiveHadoopContainer.start();

            // Create S3 client using AWS SDK v2
            S3Client s3Client = S3Client.builder()
                    .endpointOverride(URI.create("http://localhost:" + minIOContainer.getMinioApiEndpoint().getPort()))
                    .region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                    .forcePathStyle(true)
                    .build();

            // Create bucket
            s3Client.createBucket(builder -> builder.bucket(this.bucketName));
            s3Client.close();
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
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to stop HiveMinioDataLake", e);
        }
        finally {
            isStarted.set(false);
        }
    }

    public MinIOContainer getMinio()
    {
        return minIOContainer;
    }

    public HiveHadoopContainer getHiveHadoop()
    {
        return hiveHadoopContainer;
    }

    @Override
    public void close()
            throws IOException
    {
        stop();
    }
}
