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

import com.facebook.airlift.configuration.Config;
import com.facebook.presto.kafka.schema.file.FileTableDescriptionSupplier;
import com.facebook.presto.spi.HostAddress;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import java.util.List;

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
     * Whether internal columns are shown in table metadata or not. Default is no.
     */
    private boolean hideInternalColumns = true;

    /**
     * Maximum number of records per poll()
     */
    private int maxPollRecords = 500;

    /**
     * Maximum number of bytes from one partition per poll()
     */
    private int maxPartitionFetchBytes = 1024 * 1024;

    /**
     * The table description supplier to use, default is FILE
     */
    private String tableDescriptionSupplier = FileTableDescriptionSupplier.NAME;

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

    public int getMaxPollRecords()
    {
        return maxPollRecords;
    }

    @Config("kafka.max-poll-records")
    public KafkaConnectorConfig setMaxPollRecords(int maxPollRecords)
    {
        this.maxPollRecords = maxPollRecords;
        return this;
    }

    public int getMaxPartitionFetchBytes()
    {
        return maxPartitionFetchBytes;
    }

    @Config("kafka.max-partition-fetch-bytes")
    public KafkaConnectorConfig setMaxPartitionFetchBytes(int maxPartitionFetchBytes)
    {
        this.maxPartitionFetchBytes = maxPartitionFetchBytes;
        return this;
    }

    @NotNull
    public String getTableDescriptionSupplier()
    {
        return tableDescriptionSupplier;
    }

    @Config("kafka.table-description-supplier")
    public KafkaConnectorConfig setTableDescriptionSupplier(String tableDescriptionSupplier)
    {
        this.tableDescriptionSupplier = tableDescriptionSupplier;
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
