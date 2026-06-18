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

package com.facebook.presto.hudi;

import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.util.AsyncQueue;
import com.facebook.presto.hudi.split.HudiBackgroundSplitLoader;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.google.common.util.concurrent.Futures;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.MoreFutures.toCompletableFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public class HudiSplitSource
        implements ConnectorSplitSource
{
    private final AsyncQueue<ConnectorSplit> queue;
    private final HudiBackgroundSplitLoader splitLoader;
    private final ScheduledFuture splitLoaderFuture;

    public HudiSplitSource(
            ConnectorSession session,
            ExtendedHiveMetastore metastore,
            HudiTableLayoutHandle layout,
            HoodieTableFileSystemView fsView,
            List<String> partitions,
            String latestInstant,
            ExecutorService asyncQueueExecutor,
            ScheduledExecutorService splitLoaderExecutorService,
            ExecutorService splitGeneratorExecutorService,
            int maxOutstandingSplits)
    {
        this.queue = new AsyncQueue<>(maxOutstandingSplits, asyncQueueExecutor);
        this.splitLoader = new HudiBackgroundSplitLoader(
                session,
                metastore,
                splitGeneratorExecutorService,
                layout,
                fsView,
                queue,
                partitions,
                latestInstant);
        this.splitLoaderFuture = splitLoaderExecutorService.schedule(
                this.splitLoader, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(
            ConnectorPartitionHandle partitionHandle,
            int maxSize)
    {
        boolean noMoreSplits = isFinished();

        return toCompletableFuture(Futures.transform(
                queue.getBatchAsync(maxSize),
                splits -> new ConnectorSplitBatch(splits, noMoreSplits),
                directExecutor()));
    }

    @Override
    public void close()
    {
        queue.finish();
    }

    @Override
    public boolean isFinished()
    {
        return splitLoaderFuture.isDone() && queue.isFinished();
    }
}
