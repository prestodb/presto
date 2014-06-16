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
package com.facebook.presto.kafka.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class EmbeddedKafka
        implements Closeable
{
    private final EmbeddedZookeeper zookeeper;
    private final int port;
    private final File kafkaDataDir;
    private final KafkaServerStartable kafka;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    public static EmbeddedKafka createEmbeddedKafka()
            throws IOException
    {
        EmbeddedZookeeper zookeeper = new EmbeddedZookeeper();
        return new EmbeddedKafka(zookeeper, new Properties());
    }

    public static EmbeddedKafka createEmbeddedKafka(Properties overrideProperties)
            throws IOException
    {
        checkNotNull(overrideProperties, "overrideProperties is null");

        EmbeddedZookeeper zookeeper = new EmbeddedZookeeper();
        return new EmbeddedKafka(zookeeper, overrideProperties);
    }

    EmbeddedKafka(EmbeddedZookeeper zookeeper, Properties overrideProperties)
            throws IOException
    {
        this.zookeeper = zookeeper;

        this.port = TestUtils.findUnusedPort();
        this.kafkaDataDir = Files.createTempDir();

        Properties properties = new Properties();

        properties.putAll(ImmutableMap.<String, String>builder()
                .put("broker.id", "0")
                .put("host.name", "localhost")
                .put("num.partitions", "2")
                .put("log.flush.interval.messages", "10000")
                .put("log.flush.interval.ms", "1000")
                .put("log.retention.minutes", "60")
                .put("log.segment.bytes", "1048576")
                .put("auto.create.topics.enable", "false")
                .put("zookeeper.connection.timeout.ms", "1000000")
                .put("port", Integer.toString(port))
                .put("log.dirs", kafkaDataDir.getAbsolutePath())
                .put("zookeeper.connect", zookeeper.getConnectString())
                .build());

        properties.putAll(overrideProperties);

        KafkaConfig config = new KafkaConfig(properties);
        this.kafka = new KafkaServerStartable(config);
    }

    public void start()
            throws InterruptedException, IOException
    {
        if (!started.getAndSet(true)) {
            zookeeper.start();
            kafka.startup();
        }
    }

    @Override
    public void close()
    {
        if (started.get() && !stopped.getAndSet(true)) {
            kafka.shutdown();
            kafka.awaitShutdown();
            zookeeper.close();
        }
    }

    public void createTopics(String... topics)
    {
        createTopics(2, 1, new Properties(), topics);
    }

    public void createTopics(int partitions, int replication, Properties topicProperties, String... topics)
    {
        checkState(started.get() && !stopped.get(), "not started!");

        ZkClient zkClient = new ZkClient(getZookeeperConnectString(), 30_000, 30_000, ZKStringSerializer$.MODULE$);
        try {
            for (String topic : topics) {
                AdminUtils.createTopic(zkClient, topic, partitions, replication, topicProperties);
            }
        }
        finally {
            zkClient.close();
        }
    }

    public CloseableProducer<Long, Object> createProducer()
    {
        Properties props = new Properties();
        props.putAll(ImmutableMap.<String, String>builder()
                .put("metadata.broker.list", getConnectString())
                .put("serializer.class", JsonEncoder.class.getName())
                .put("key.serializer.class", NumberEncoder.class.getName())
                .put("partitioner.class", NumberPartitioner.class.getName())
                .put("request.required.acks", "1")
                .build());

        ProducerConfig producerConfig = new ProducerConfig(props);
        return new CloseableProducer<>(producerConfig);
    }

    public static class CloseableProducer<K, V>
            extends Producer<K, V>
            implements AutoCloseable
    {
        public CloseableProducer(ProducerConfig config)
        {
            super(config);
        }
    }

    public int getZookeeperPort()
    {
        return zookeeper.getPort();
    }

    public int getPort()
    {
        return port;
    }

    public String getConnectString()
    {
        return "localhost:" + Integer.toString(port);
    }

    public String getZookeeperConnectString()
    {
        return zookeeper.getConnectString();
    }

    public void cleanup()
    {
        checkState(stopped.get(), "not stopped");

        for (File file : Files.fileTreeTraverser().postOrderTraversal(kafkaDataDir)) {
            file.delete();
        }

        zookeeper.cleanup();
    }
}
