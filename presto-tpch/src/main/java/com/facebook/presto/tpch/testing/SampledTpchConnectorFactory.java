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
package com.facebook.presto.tpch.testing;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.tpch.TpchHandleResolver;
import com.facebook.presto.tpch.TpchSplitManager;

import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.util.Objects.requireNonNull;

public class SampledTpchConnectorFactory
        implements ConnectorFactory
{
    private final NodeManager nodeManager;
    private final int defaultSplitsPerNode;
    private final int sampleWeight;

    public SampledTpchConnectorFactory(NodeManager nodeManager, int defaultSplitsPerNode, int sampleWeight)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.defaultSplitsPerNode = defaultSplitsPerNode;
        this.sampleWeight = sampleWeight;
    }

    @Override
    public String getName()
    {
        return "tpch_sampled";
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> properties)
    {
        requireNonNull(properties, "properties is null");
        final int splitsPerNode = getSplitsPerNode(properties);

        return new Connector() {
            @Override
            public ConnectorMetadata getMetadata()
            {
                return new SampledTpchMetadata(connectorId);
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                return new TpchSplitManager(connectorId, nodeManager, splitsPerNode);
            }

            @Override
            public ConnectorHandleResolver getHandleResolver()
            {
                return new TpchHandleResolver(connectorId);
            }

            @Override
            public ConnectorRecordSetProvider getRecordSetProvider()
            {
                return new SampledTpchRecordSetProvider(connectorId, sampleWeight);
            }
        };
    }

    private int getSplitsPerNode(Map<String, String> properties)
    {
        try {
            return Integer.parseInt(firstNonNull(properties.get("tpch.splits-per-node"), String.valueOf(defaultSplitsPerNode)));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid property tpch.splits-per-node");
        }
    }
}
