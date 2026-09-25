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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.facebook.presto.testing.containers.S3MockContainer;
import com.facebook.presto.util.AutoCloseableCloser;
import com.google.common.collect.ImmutableMap;
import org.testcontainers.containers.Network;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.hive.containers.HiveHadoopContainer.HIVE3_IMAGE;
import static com.facebook.presto.hive.containers.HiveHadoopContainer.HIVE4_IMAGE;
import static com.facebook.presto.tests.SslKeystoreManager.getKeystorePath;
import static com.facebook.presto.tests.SslKeystoreManager.getTruststorePath;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.Network.newNetwork;

public class HiveS3DataLake
        implements Closeable
{
    private static final Object SSL_LOCK = new Object();

    private final String bucketName;
    private final S3MockContainer s3Container;
    private final HiveHadoopContainer hiveHadoopContainer;

    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final AutoCloseableCloser closer = AutoCloseableCloser.create();

    public HiveS3DataLake(String bucketName, Map<String, String> hiveHadoopFilesToMount)
    {
        this(bucketName, hiveHadoopFilesToMount, HiveHadoopContainer.DEFAULT_IMAGE, false);
    }

    public HiveS3DataLake(String bucketName, Map<String, String> hiveHadoopFilesToMount, String hiveHadoopImage, boolean isSslEnabledTest)
    {
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        Network network = closer.register(newNetwork());
        this.s3Container = closer.register(
                S3MockContainer.builder()
                        .withNetwork(network)
                        .build());

        ImmutableMap.Builder filesToMount = ImmutableMap.<String, String>builder()
                .putAll(hiveHadoopFilesToMount);

        String hadoopCoreSitePath = "/etc/hadoop/conf/core-site.xml";

        if (Objects.equals(hiveHadoopImage, HIVE3_IMAGE) || Objects.equals(hiveHadoopImage, HIVE4_IMAGE)) {
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
            this.s3Container.start();
            this.hiveHadoopContainer.start();
            AmazonS3 s3Client = AmazonS3ClientBuilder
                    .standard()
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                            "http://localhost:" + s3Container.getApiEndpoint().getPort(),
                            "us-east-1"))
                    .withPathStyleAccessEnabled(true)
                    .withCredentials(new AWSStaticCredentialsProvider(
                            new BasicAWSCredentials(S3MockContainer.ACCESS_KEY, S3MockContainer.SECRET_KEY)))
                    .build();
            s3Client.createBucket(this.bucketName);
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
            throw new RuntimeException("Failed to stop HiveS3DataLake", e);
        }
        finally {
            isStarted.set(false);
        }
    }

    public S3MockContainer getS3Container()
    {
        return s3Container;
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
