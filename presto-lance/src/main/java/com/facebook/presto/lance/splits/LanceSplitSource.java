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
package com.facebook.presto.lance.splits;

import com.facebook.presto.lance.metadata.LanceTableHandle;
import com.facebook.presto.lance.client.LanceClient;
import com.facebook.presto.lance.fragments.FragmentInfo;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.google.common.collect.ImmutableList;
import com.lancedb.lance.DatasetFragment;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class LanceSplitSource implements ConnectorSplitSource
{
    private final LanceClient lanceClient;
    private final LanceTableHandle lanceTable;
    private boolean isFinished;

    LanceSplitSource(LanceClient lanceClient, LanceTableHandle lanceTable) {
        this.lanceClient = requireNonNull(lanceClient, "lanceClient is null");
        this.lanceTable = requireNonNull(lanceTable, "lanceTable is null");
    }
    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        ImmutableList.Builder<ConnectorSplit> splitListBuilder = ImmutableList.builder();
        List<DatasetFragment> fragments = lanceClient.getFragments(lanceTable.getTableName());
        for (DatasetFragment fragment : fragments) {
            FragmentInfo fragmentInfo = new FragmentInfo(fragment.getId());
            splitListBuilder.add(LanceSplit.createFragmentSplit(ImmutableList.of(fragmentInfo)));
        }
        isFinished = true;  //TODO: streaming the splits
        return completedFuture(new ConnectorSplitBatch(splitListBuilder.build(), isFinished()));
    }

    @Override
    public void close()
    {

    }

    @Override
    public boolean isFinished()
    {
        return isFinished;
    }
}
