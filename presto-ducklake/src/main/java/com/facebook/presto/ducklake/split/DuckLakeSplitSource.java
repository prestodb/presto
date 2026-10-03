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
package com.facebook.presto.ducklake.split;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Hands out an already-built, fixed list of splits (the Parquet splits followed by the inlined
 * splits, in the order {@link DuckLakeSplitManager} built them) one batch at a time. No pruning
 * or lazy catalog access happens here; that is left to later tasks.
 */
public class DuckLakeSplitSource
        implements ConnectorSplitSource
{
    private final Iterator<ConnectorSplit> splits;

    public DuckLakeSplitSource(List<ConnectorSplit> splits)
    {
        this.splits = ImmutableList.copyOf(requireNonNull(splits, "splits is null")).iterator();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        ImmutableList.Builder<ConnectorSplit> batch = ImmutableList.builder();
        int count = 0;
        while (count < maxSize && splits.hasNext()) {
            batch.add(splits.next());
            count++;
        }
        return completedFuture(new ConnectorSplitBatch(batch.build(), isFinished()));
    }

    @Override
    public boolean isFinished()
    {
        return !splits.hasNext();
    }

    @Override
    public void close()
    {
    }
}
