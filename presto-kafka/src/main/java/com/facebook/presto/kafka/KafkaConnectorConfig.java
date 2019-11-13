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
package com.facebook.presto.kafka;

import com.facebook.presto.spi.HostAddress;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import java.io.File;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Iterables.transform;

public class KafkaConnectorConfig
{
    private static final int KAFKA_DEFAULT_PORT = 9092;

    /**
     * Seed nodes for Kafka cluster. At least one must exist.
     */
    private List<HostAddress> nodes;

    /**
     * Timeout to connect to Kafka.
     */
    private Duration kafkaConnectTimeout = Duration.valueOf("10s");

    /**
     * The schema name to use in the connector.
     */
    private String defaultSchema = "default";

    /**
     * Set of tables known to this connector. For each table, a description file may be present in the catalog folder which describes columns for the given topic.
     */
    private Set<String> tableNames = ImmutableSet.of();

    /**
     * Folder holding the JSON description files for Kafka topics.
     */
    private File tableDescriptionDir = new File("etc/kafka/");

    /**
     * Whether internal columns are shown in table metadata or not. Default is no.
     */
    private boolean hideInternalColumns = true;

    /**
     * Use either 'static' or 'zookeeper' to discover kafka brokers
     */
    private String discoveryMode = "static";

    /**
     * The Zookeeper URI.
     * Example: hostAddress:2181
     */
    private String kafkaZookeeperUri;

    /**
     * The Zookeeper path for Kafka services.
     * Example: /company/service/kafka/prod/path
     */
    private String kafkaZookeeperPath;

    /**
     * Maximum number of records per poll()
     */
    private int maxPollRecords = 500;

    /**
     * Maximum number of bytes from one partition per poll()
     */
    private int maxPartitionFetchBytes = 1048576;

    /**
     * Retry sleep time to connect to Zookeeper
     */
    private int zookeeperRetrySleepTime = 100;

    /**
     * Max retries to connect to Zookeeper
     */
    private int zookeeperMaxRetries = 3;

    @NotNull
    public File getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    @Config("kafka.table-description-dir")
    public KafkaConnectorConfig setTableDescriptionDir(File tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }

    @NotNull
    public Set<String> getTableNames()
    {
        return tableNames;
    }

    @Config("kafka.table-names")
    public KafkaConnectorConfig setTableNames(String tableNames)
    {
        this.tableNames = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames));
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("kafka.default-schema")
    public KafkaConnectorConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    public List<HostAddress> getNodes()
    {
        return nodes;
    }

    @Config("kafka.nodes")
    public KafkaConnectorConfig setNodes(String nodes)
    {
        this.nodes = (nodes == null) ? null : parseNodes(nodes).asList();
        return this;
    }

    @MinDuration("1s")
    public Duration getKafkaConnectTimeout()
    {
        return kafkaConnectTimeout;
    }

    @Config("kafka.connect-timeout")
    public KafkaConnectorConfig setKafkaConnectTimeout(String kafkaConnectTimeout)
    {
        this.kafkaConnectTimeout = Duration.valueOf(kafkaConnectTimeout);
        return this;
    }

    public String getDiscoveryMode()
    {
        return discoveryMode;
    }

    @Config("kafka.discovery.mode")
    @Pattern(regexp = "static|zookeeper", flags = Pattern.Flag.CASE_INSENSITIVE)
    public KafkaConnectorConfig setDiscoveryMode(String discoveryMode)
    {
        this.discoveryMode = discoveryMode;
        return this;
    }

    @Config("kafka.zookeeper.path")
    public KafkaConnectorConfig setZookeeperPath(String zkPath)
    {
        this.kafkaZookeeperPath = zkPath;
        return this;
    }

    public String getZookeeperPath()
    {
        return kafkaZookeeperPath;
    }

    @Config("kafka.zookeeper.uri")
    public KafkaConnectorConfig setZookeeperUri(String setZookeeperUri)
    {
        this.kafkaZookeeperUri = setZookeeperUri;
        return this;
    }

    public String getZookeeperUri()
    {
        return kafkaZookeeperUri;
    }

    public int getMaxPollRecords()
    {
        return maxPollRecords;
    }

    @Config("kafka.max.poll.records")
    public KafkaConnectorConfig setMaxPollRecords(int maxPollRecords)
    {
        this.maxPollRecords = maxPollRecords;
        return this;
    }

    @NotNull
    public int getZookeeperRetrySleepTime()
    {
        return Integer.valueOf(zookeeperRetrySleepTime);
    }

    @Config("kafka.zookeeper.retry.sleeptime")
    @ConfigDescription("Zookeeper sleep time between reties")
    public KafkaConnectorConfig setZookeeperRetrySleepTime(int zookeeperRetrySleepTime)
    {
        this.zookeeperRetrySleepTime = zookeeperRetrySleepTime;
        return this;
    }

    @Min(1)
    public int getZookeeperMaxRetries()
    {
        return zookeeperMaxRetries;
    }

    @Config("kafka.zookeeper.max.retries")
    @ConfigDescription("Zookeeper max reties")
    public KafkaConnectorConfig setZookeeperMaxRetries(int zookeeperMaxRetries)
    {
        this.zookeeperMaxRetries = zookeeperMaxRetries;
        return this;
    }

    public int getMaxPartitionFetchBytes()
    {
        return maxPartitionFetchBytes;
    }

    @Config("kafka.max.partition.fetch.bytes")
    public KafkaConnectorConfig setMaxPartitionFetchBytes(int maxPartitionFetchBytes)
    {
        this.maxPartitionFetchBytes = maxPartitionFetchBytes;
        return this;
    }

    public boolean isHideInternalColumns()
    {
        return hideInternalColumns;
    }

    @Config("kafka.hide-internal-columns")
    public KafkaConnectorConfig setHideInternalColumns(boolean hideInternalColumns)
    {
        this.hideInternalColumns = hideInternalColumns;
        return this;
    }

    public static ImmutableSet<HostAddress> parseNodes(String nodes)
    {
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
        return ImmutableSet.copyOf(transform(splitter.split(nodes), KafkaConnectorConfig::toHostAddress));
    }

    private static HostAddress toHostAddress(String value)
    {
        return HostAddress.fromString(value).withDefaultPort(KAFKA_DEFAULT_PORT);
    }
}
