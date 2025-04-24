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
import com.facebook.presto.testing.containers.MinIOContainer;
import com.facebook.presto.util.AutoCloseableCloser;
import com.google.common.collect.ImmutableMap;
import org.testcontainers.containers.Network;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.hive.containers.HiveHadoopContainer.HIVE3_IMAGE;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.Network.newNetwork;

public class HiveMinIODataLake
        implements Closeable
{
    public static final String ACCESS_KEY = "accesskey";
    public static final String SECRET_KEY = "secretkey";

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
            filesToMount.put("hive_ssl_enable/hive-site.xml", "/opt/hive/conf/hive-site.xml");
            filesToMount.put("hive_ssl_enable/hive-metastore.jks", "/opt/hive/conf/hive-metastore.jks");
            filesToMount.put("hive_ssl_enable/hive-metastore-truststore.jks", "/opt/hive/conf/hive-metastore-truststore.jks");
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
            AmazonS3 s3Client = AmazonS3ClientBuilder
                    .standard()
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                            "http://localhost:" + minIOContainer.getMinioApiEndpoint().getPort(),
                            "us-east-1"))
                    .withPathStyleAccessEnabled(true)
                    .withCredentials(new AWSStaticCredentialsProvider(
                            new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)))
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
