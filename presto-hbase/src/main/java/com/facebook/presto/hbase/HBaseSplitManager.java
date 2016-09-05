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

import static com.facebook.presto.hbase.HBaseHandleResolver.convertLayout;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import javax.inject.Inject;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.airlift.log.Logger;

/**
 * HBase specific implementation of {@link ConnectorSplitManager}.
 */
public class HBaseSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(HBaseSplitManager.class);

    private final String connectorId;
    private final HBaseSimpleConsumerManager consumerManager;
    private final Set<HostAddress> nodes;

    @Inject
    public HBaseSplitManager(
            HBaseConnectorId connectorId,
            HBaseConnectorConfig hbaseConnectorConfig,
            HBaseSimpleConsumerManager consumerManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");

        requireNonNull(hbaseConnectorConfig, "hbaseConnectorConfig is null");
        this.nodes = ImmutableSet.copyOf(hbaseConnectorConfig.getNodes());
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
    	ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        
    	try {
	        HBaseTableHandle hbaseTableHandle = convertLayout(layout).getTable();
	        TableName tableName = TableName.valueOf(hbaseTableHandle.getTopicName());
	
	        Connection connection = consumerManager.getConsumer(selectRandom(nodes));
	        
	        RegionLocator regionLocator=  connection.getRegionLocator(tableName);
	        List<HRegionLocation> regionLocationList = regionLocator.getAllRegionLocations();
	
	
	        
	        for (HRegionLocation regionLocation : regionLocationList) {
	        	HRegionInfo regionInfo = regionLocation.getRegionInfo();
	            log.debug("Adding Region %s", regionInfo.getEncodedName());
	            HBaseSplit split = new HBaseSplit(
	                    connectorId,
	                    hbaseTableHandle.getTopicName(),
	                    hbaseTableHandle.getKeyDataFormat(),
	                    hbaseTableHandle.getMessageDataFormat(),
	                    regionInfo.getRegionId(),
	                    regionInfo.getStartKey(),
	                    regionInfo.getEndKey(),
	                    HostAddress.fromParts(regionLocation.getHostname(), regionLocation.getPort())
	                    );
	
	            splits.add(split);
	        }
    	} catch (IOException ex) {
    		log.error(ex.getMessage(), ex);
    	}
        
        return new FixedSplitSource(splits.build());
    }

    private static <T> T selectRandom(Iterable<T> iterable)
    {
        List<T> list = ImmutableList.copyOf(iterable);
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }
}
