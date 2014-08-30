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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.FixedSafeSplitSource;
import com.facebook.presto.spi.SafePartitionResult;
import com.facebook.presto.spi.SafeSplitManager;
import com.facebook.presto.spi.SafeSplitSource;
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
        implements SafeSplitManager<ExampleTableHandle, ExampleColumnHandle, ExampleSplit, ExamplePartition>
{
    private final ExampleClient exampleClient;

    @Inject
    public ExampleSplitManager(ExampleClient exampleClient)
    {
        this.exampleClient = checkNotNull(exampleClient, "client is null");
    }

    @Override
    public SafePartitionResult<ExampleColumnHandle, ExamplePartition> getPartitions(ExampleTableHandle tableHandle, TupleDomain<ExampleColumnHandle> tupleDomain)
    {
        // example connector has only one partition
        List<ExamplePartition> partitions = ImmutableList.of(new ExamplePartition(tableHandle.getSchemaName(), tableHandle.getTableName()));
        // example connector does not do any additional processing/filtering with the TupleDomain, so just return the whole TupleDomain
        return new SafePartitionResult<>(partitions, tupleDomain);
    }

    @Override
    public SafeSplitSource<ExampleSplit> getPartitionSplits(ExampleTableHandle tableHandle, List<ExamplePartition> partitions)
    {
        checkNotNull(partitions, "partitions is null");
        checkArgument(partitions.size() == 1, "Expected one partition but got %s", partitions.size());
        ExamplePartition partition = partitions.get(0);

        ExampleTable table = exampleClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

        List<ConnectorSplit> splits = Lists.newArrayList();
        for (URI uri : table.getSources()) {
            splits.add(new ExampleSplit(partition.getSchemaName(), partition.getTableName(), uri));
        }
        Collections.shuffle(splits);

        return new FixedSafeSplitSource(splits);
    }
}
