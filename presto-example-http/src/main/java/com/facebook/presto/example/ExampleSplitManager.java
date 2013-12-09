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
package com.facebook.presto.example;

import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.inject.Inject;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ExampleSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final ExampleClient exampleClient;

    @Inject
    public ExampleSplitManager(ExampleConnectorId connectorId, ExampleClient exampleClient)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.exampleClient = checkNotNull(exampleClient, "client is null");
    }

    @Override
    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof ExampleTableHandle && ((ExampleTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public PartitionResult getPartitions(TableHandle tableHandle, TupleDomain tupleDomain)
    {
        checkArgument(tableHandle instanceof ExampleTableHandle, "tableHandle is not an instance of ExampleTableHandle");
        ExampleTableHandle exampleTableHandle = (ExampleTableHandle) tableHandle;

        // example connector has only one partition
        List<Partition> partitions = ImmutableList.<Partition>of(new ExamplePartition(exampleTableHandle.getSchemaName(), exampleTableHandle.getTableName()));
        // example connector does not do any additional processing/filtering with the TupleDomain, so just return the whole TupleDomain
        return new PartitionResult(partitions, tupleDomain);
    }

    @Override
    public Iterable<Split> getPartitionSplits(TableHandle tableHandle, List<Partition> partitions)
    {
        checkNotNull(partitions, "partitions is null");
        checkArgument(partitions.size() == 1, "Expected one partition but got %s", partitions.size());
        Partition partition = partitions.get(0);

        checkArgument(partition instanceof ExamplePartition, "partition is not an instance of ExamplePartition");
        ExamplePartition examplePartition = (ExamplePartition) partition;

        ExampleTableHandle exampleTableHandle = (ExampleTableHandle) tableHandle;
        ExampleTable table = exampleClient.getTable(exampleTableHandle.getSchemaName(), exampleTableHandle.getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", exampleTableHandle.getSchemaName(), exampleTableHandle.getTableName());

        List<Split> splits = Lists.newArrayList();
        for (URI uri : table.getSources()) {
            splits.add(new ExampleSplit(connectorId, examplePartition.getSchemaName(), examplePartition.getTableName(), uri));
        }
        Collections.shuffle(splits);

        return splits;
    }
}
