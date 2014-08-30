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
package com.facebook.presto.connector.adapter;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SafePartitionResult;
import com.facebook.presto.spi.SafeSplitManager;
import com.facebook.presto.spi.SafeSplitSource;
import com.facebook.presto.spi.TupleDomain;

import java.util.List;

public class SafeSplitManagerAdapter<TH extends ConnectorTableHandle, CH extends ConnectorColumnHandle, S extends ConnectorSplit, P extends ConnectorPartition>
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final SafeSplitManager<TH, CH, S, P> delegate;
    private final SafeTypeAdapter<TH, CH, ?, ?, ?, ?, P> typeAdapter;

    public SafeSplitManagerAdapter(String connectorId, SafeSplitManager<TH, CH, S, P> delegate, SafeTypeAdapter<TH, CH, ?, ?, ?, ?, P> typeAdapter)
    {
        this.connectorId = connectorId;
        this.delegate = delegate;
        this.typeAdapter = typeAdapter;
    }

    @Override

    public ConnectorPartitionResult getPartitions(ConnectorTableHandle table, TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        SafePartitionResult<CH, P> partitionResult = delegate.getPartitions(typeAdapter.castTableHandle(table), typeAdapter.castTupleDomain(tupleDomain));
        return new ConnectorPartitionResult(
                (List<ConnectorPartition>) partitionResult.getPartitions(),
                typeAdapter.castToGenericTupleDomain(partitionResult.getUndeterminedTupleDomain()));
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle table, List<ConnectorPartition> partitions)
    {
        SafeSplitSource<S> partitionSplits = delegate.getPartitionSplits(typeAdapter.castTableHandle(table), typeAdapter.castPartitions(partitions));
        return new SafeSplitSourceAdapter<>(connectorId, partitionSplits);
    }
}
