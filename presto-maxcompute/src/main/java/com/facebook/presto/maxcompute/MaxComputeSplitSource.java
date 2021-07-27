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
package com.facebook.presto.maxcompute;

import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.facebook.airlift.concurrent.MoreFutures;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.maxcompute.util.AsyncQueue;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class MaxComputeSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(MaxComputeSplitSource.class);
    private final List<Partition> partitions;
    private final AsyncQueue<ConnectorSplit> queue;
    private final MaxComputeClient maxComputeClient;
    private final String projectName;
    private final String tableName;
    private final MaxComputeTableHandle maxComputeTableHandle;
    private final Table tableMeta;
    private final TableTunnel tableTunnel;
    private final String connectorId;
    private final int rowsPerSplit;
    private AtomicLong runningCount;

    public MaxComputeSplitSource(MaxComputeClient maxComputeClient,
                                 String projectName,
                                 String tableName,
                                 MaxComputeTableHandle maxComputeTableHandle,
                                 Table tableMeta,
                                 TableTunnel tableTunnel,
                                 List<Partition> partitions,
                                 String connectorId,
                                 ExecutorService executorService,
                                 int rowsPerSplit)
    {
        this.maxComputeClient = requireNonNull(maxComputeClient, "odpsClient is null");
        this.projectName = requireNonNull(projectName, "projectName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.maxComputeTableHandle = requireNonNull(maxComputeTableHandle, "odpsTableHandle is null");
        this.tableMeta = requireNonNull(tableMeta, "tableMeta is null");
        this.tableTunnel = requireNonNull(tableTunnel, "tableMeta is null");
        this.partitions = partitions;
        this.connectorId = requireNonNull(connectorId, "tableMeta is null");
        this.rowsPerSplit = rowsPerSplit;
        this.queue = new AsyncQueue<>(1000, executorService);
        runningCount = new AtomicLong(partitions.size());
        for (Partition partition : partitions) {
            executorService.submit(() -> loadSplit(partition));
        }
    }

    @Override public CompletableFuture<ConnectorSplitBatch> getNextBatch(
            ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        ListenableFuture<List<ConnectorSplit>> future = queue.getBatchAsync(maxSize);
        ListenableFuture<ConnectorSplitBatch> transform = Futures.transform(future, splits -> {
            requireNonNull(splits, "splits is null");
            if (runningCount.get() == 0) {
                return new ConnectorSplitBatch(splits, splits.isEmpty() && queue.isFinished());
            }
            else {
                return new ConnectorSplitBatch(splits, false);
            }
        }, directExecutor());
        return MoreFutures.toCompletableFuture(transform);
    }

    @Override public void close()
    {
        queue.finish();
    }

    @Override public boolean isFinished()
    {
        return queue.isFinished() || runningCount.get() == 0;
    }

    private void loadSplit(Partition part)
    {
        PartitionSpec partSpec = part.getPartitionSpec();
        Long rowCount = maxComputeClient.getRecordCount(projectName, tableName, partSpec, tableMeta.getLastDataModifiedTime());
        if (rowCount == null) {
            DownloadSession downloadSession = maxComputeClient.getDownloadSession(tableTunnel, projectName, tableName, partSpec);
            rowCount = downloadSession.getRecordCount();
            maxComputeClient.cacheRecordCount(projectName, tableName, partSpec, tableMeta.getLastDataModifiedTime(), rowCount);
        }
        long splitCount = getSplitCount(rowCount);
        long step = rowCount / splitCount;

        log.info("ODPS total_row_count=" + rowCount + ", split_count=" + splitCount + ", PartitionSpec=" + partSpec);

        for (int i = 0; i < splitCount - 1; i++) {
            MaxComputeSplit split = new MaxComputeSplit(UUID.randomUUID().toString(), connectorId,
                    maxComputeTableHandle.getTableName(), maxComputeTableHandle.getProjectName(),
                    partSpec.toString(), step * i, step, part.getLastDataModifiedTime());
            queue.offer(split);
            rowCount -= step;
        }

        if (rowCount > 0) {
            long lastSplitOffset = step * (splitCount - 1);
            MaxComputeSplit split = new MaxComputeSplit(UUID.randomUUID().toString(), connectorId,
                    maxComputeTableHandle.getTableName(), maxComputeTableHandle.getProjectName(),
                    partSpec.toString(), lastSplitOffset, rowCount, part.getLastDataModifiedTime());
            queue.offer(split);
        }
        if (runningCount.decrementAndGet() == 0) {
            queue.finish();
        }
    }
    private long getSplitCount(long totalRowCount)
    {
        long splitRowCount = rowsPerSplit;
        int[] stepFactors = new int[] {1, 2, 10, 20, 50, 100, 200, 500};

        for (int step : stepFactors) {
            if (totalRowCount / step < splitRowCount) {
                return step;
            }
        }

        return stepFactors[stepFactors.length - 1];
    }
}
