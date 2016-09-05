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
package com.facebook.presto.hbase;

import com.facebook.presto.spi.HostAddress;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.io.File;
import java.util.Set;

import static com.google.common.collect.Iterables.transform;

public class HBaseConnectorConfig
{
    private static final int HBASE_DEFAULT_PORT = 9092;

    /**
     * Seed nodes for HBase cluster. At least one must exist.
     */
    private Set<HostAddress> nodes = ImmutableSet.of();

    /**
     * Timeout to connect to HBase.
     */
    private Duration hbaseConnectTimeout = Duration.valueOf("10s");

    /**
     * Buffer size for connecting to HBase.
     */
    private DataSize hbaseBufferSize = new DataSize(64, Unit.KILOBYTE);

    /**
     * The schema name to use in the connector.
     */
    private String defaultSchema = "default";

    /**
     * Set of tables known to this connector. For each table, a description file may be present in the catalog folder which describes columns for the given topic.
     */
    private Set<String> tableNames = ImmutableSet.of();

    /**
     * Folder holding the JSON description files for HBase topics.
     */
    private File tableDescriptionDir = new File("etc/hbase/");

    /**
     * Whether internal columns are shown in table metadata or not. Default is no.
     */
    private boolean hideInternalColumns = true;
    
    private String zookeeperZnodeParent = "/hbase";

    @NotNull
    public String getZookeeperZnodeParent()
    {
        return zookeeperZnodeParent;
    }

    @Config("zookeeper.znode.parent")
    public HBaseConnectorConfig setZookeeperZnodeParent(String zookeeperZnodeParent)
    {
        this.zookeeperZnodeParent = zookeeperZnodeParent;
        return this;
    }

    private String hbaseZkQuorumzookeeper = "localhost";

    @NotNull
    public String getHbaseZkQuorumzookeeper()
    {
        return hbaseZkQuorumzookeeper;
    }

    @Config("hbase.zk-quorum")
    public HBaseConnectorConfig setHbaseZkQuorumzookeeper(String hbaseZkQuorumzookeeper)
    {
        this.hbaseZkQuorumzookeeper = hbaseZkQuorumzookeeper;
        return this;
    }

    private String hbaseZkPort = "2181";

    @NotNull
    public String getHbaseZkPort()
    {
        return hbaseZkPort;
    }

    @Config("hbase.zk-port")
    public HBaseConnectorConfig setHbaseZkPort(String hbaseZkPort)
    {
        this.hbaseZkPort = hbaseZkPort;
        return this;
    }

    @NotNull
    public File getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    @Config("hbase.table-description-dir")
    public HBaseConnectorConfig setTableDescriptionDir(File tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }

    @NotNull
    public Set<String> getTableNames()
    {
        return tableNames;
    }

    @Config("hbase.table-names")
    public HBaseConnectorConfig setTableNames(String tableNames)
    {
        this.tableNames = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames));
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("hbase.default-schema")
    public HBaseConnectorConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @Size(min = 1)
    public Set<HostAddress> getNodes()
    {
        return nodes;
    }

    @Config("hbase.nodes")
    public HBaseConnectorConfig setNodes(String nodes)
    {
        this.nodes = (nodes == null) ? null : parseNodes(nodes);
        return this;
    }

    @MinDuration("1s")
    public Duration getHBaseConnectTimeout()
    {
        return hbaseConnectTimeout;
    }

    @Config("hbase.connect-timeout")
    public HBaseConnectorConfig setHBaseConnectTimeout(String hbaseConnectTimeout)
    {
        this.hbaseConnectTimeout = Duration.valueOf(hbaseConnectTimeout);
        return this;
    }

    public DataSize getHBaseBufferSize()
    {
        return hbaseBufferSize;
    }

    @Config("hbase.buffer-size")
    public HBaseConnectorConfig setHBaseBufferSize(String hbaseBufferSize)
    {
        this.hbaseBufferSize = DataSize.valueOf(hbaseBufferSize);
        return this;
    }

    public boolean isHideInternalColumns()
    {
        return hideInternalColumns;
    }

    @Config("hbase.hide-internal-columns")
    public HBaseConnectorConfig setHideInternalColumns(boolean hideInternalColumns)
    {
        this.hideInternalColumns = hideInternalColumns;
        return this;
    }

    public static ImmutableSet<HostAddress> parseNodes(String nodes)
    {
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
        return ImmutableSet.copyOf(transform(splitter.split(nodes), HBaseConnectorConfig::toHostAddress));
    }

    private static HostAddress toHostAddress(String value)
    {
        return HostAddress.fromString(value).withDefaultPort(HBASE_DEFAULT_PORT);
    }
}
