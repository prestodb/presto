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
package io.prestosql.plugin.kafka;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import io.prestosql.spi.HostAddress;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.io.File;
import java.util.Set;

import static com.google.common.collect.Iterables.transform;

public class KafkaConnectorConfig
{
    private static final int KAFKA_DEFAULT_PORT = 9092;

    /**
     * Seed nodes for Kafka cluster. At least one must exist.
     */
    private Set<HostAddress> nodes = ImmutableSet.of();

    /**
     * Timeout to connect to Kafka.
     */
    private Duration kafkaConnectTimeout = Duration.valueOf("10s");

    /**
     * Buffer size for connecting to Kafka.
     */
    private DataSize kafkaBufferSize = new DataSize(64, Unit.KILOBYTE);

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

    @Size(min = 1)
    public Set<HostAddress> getNodes()
    {
        return nodes;
    }

    @Config("kafka.nodes")
    public KafkaConnectorConfig setNodes(String nodes)
    {
        this.nodes = (nodes == null) ? null : parseNodes(nodes);
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

    public DataSize getKafkaBufferSize()
    {
        return kafkaBufferSize;
    }

    @Config("kafka.buffer-size")
    public KafkaConnectorConfig setKafkaBufferSize(String kafkaBufferSize)
    {
        this.kafkaBufferSize = DataSize.valueOf(kafkaBufferSize);
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
