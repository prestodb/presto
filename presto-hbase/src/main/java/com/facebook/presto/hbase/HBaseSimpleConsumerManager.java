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

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeManager;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.primitives.Ints;

import io.airlift.log.Logger;

/**
 * Manages connections to the HBase nodes. A worker may connect to multiple HBase nodes depending on the segments and
 * partitions it needs to process.
 * According to the HBase source code, a HBase {@link kafka.javaapi.consumer.SimpleConsumer} is thread-safe.
 */
public class HBaseSimpleConsumerManager
{
    private static final Logger log = Logger.get(HBaseSimpleConsumerManager.class);

    private final LoadingCache<HostAddress, Connection> consumerCache;

    private final String connectorId;
    private final NodeManager nodeManager;
    private final int connectTimeoutMillis;
    private final int bufferSizeBytes;
    
    private final Configuration config = HBaseConfiguration.create();

    @Inject
    public HBaseSimpleConsumerManager(
            HBaseConnectorId connectorId,
            HBaseConnectorConfig hbaseConnectorConfig,
            NodeManager nodeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");

        requireNonNull(hbaseConnectorConfig, "hbaseConfig is null");
        this.connectTimeoutMillis = Ints.checkedCast(hbaseConnectorConfig.getHBaseConnectTimeout().toMillis());
        this.bufferSizeBytes = Ints.checkedCast(hbaseConnectorConfig.getHBaseBufferSize().toBytes());

        this.consumerCache = CacheBuilder.newBuilder().build(new SimpleConsumerCacheLoader());

        config.set("zookeeper.znode.parent", hbaseConnectorConfig.getZookeeperZnodeParent());
        config.set("hbase.cluster.distributed", "true");
        config.set("hbase.zookeeper.quorum", hbaseConnectorConfig.getHbaseZkQuorumzookeeper());
    }

    @PreDestroy
    public void tearDown()
    {
        for (Map.Entry<HostAddress, Connection> entry : consumerCache.asMap().entrySet()) {
            try {
                entry.getValue().close();
            }
            catch (Exception e) {
                log.warn(e, "While closing consumer %s:", entry.getKey());
            }
        }
    }

    public Connection getConsumer(HostAddress host)
    {
        requireNonNull(host, "host is null");
        try {
            return consumerCache.get(host);
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private class SimpleConsumerCacheLoader
            extends CacheLoader<HostAddress, Connection>
    {
        @Override
        public Connection load(HostAddress host)
                throws Exception
        {
        	Connection connection = ConnectionFactory.createConnection(config);
            log.info("Creating new Connection for %s", host);
            return connection;
            /*return new Connection(host.getHostText(),
                    host.getPort(),
                    connectTimeoutMillis,
                    bufferSizeBytes,
                    format("presto-hbase-%s-%s", connectorId, nodeManager.getCurrentNode().getNodeIdentifier()));*/
        }
    }
}
