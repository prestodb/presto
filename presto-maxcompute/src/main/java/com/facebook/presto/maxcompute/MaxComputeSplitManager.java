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
import com.aliyun.odps.Table;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.facebook.presto.maxcompute.MaxComputeHandleResolver.convertLayout;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

/**
 * ODPS specific implementation of {@link ConnectorSplitManager}.
 */
public class MaxComputeSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(MaxComputeSplitManager.class);

    private final String connectorId;
    private final MaxComputeClient maxComputeClient;
    private final MaxComputeMetadata maxComputeMetadata;
    private final ExecutorService executorService;

    @Inject
    public MaxComputeSplitManager(MaxComputeConnectorId connectorId,
                                  MaxComputeClient maxComputeClient,
                                  MaxComputeMetadata maxComputeMetadata,
                                  MaxComputeConfig config)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.maxComputeClient = requireNonNull(maxComputeClient, "odpsClient is null");
        this.maxComputeMetadata = requireNonNull(maxComputeMetadata, "odpsMetadata is null");
        this.executorService = newFixedThreadPool(config.getLoadSplitThreads());
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout, SplitSchedulingContext splitSchedulingContext)
    {
        // Determine the split limitation.
        int splitLimit = MaxComputeSessionProperties.getSplitLimit(session);
        MaxComputeTableLayoutHandle maxComputeTableLayoutHandle = convertLayout(layout);
        MaxComputeTableHandle maxComputeTableHandle = maxComputeTableLayoutHandle.getTable();
        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();

        String tableName = maxComputeTableHandle.getTableName();

        String projectName = maxComputeTableHandle.getProjectName();
        TableTunnel tableTunnel = maxComputeClient.getTableTunnel(session, projectName);

        try {
            Table tableMeta = maxComputeClient.getTableMeta(projectName, tableName);
            if (tableMeta.isPartitioned()) {
                List<MaxComputeColumnHandle> partitionColumns = maxComputeTableHandle.getOrderedColumnHandles().stream().filter(MaxComputeColumnHandle::isPartitionColumn).collect(
                        Collectors.toList());
                Optional<TupleDomain<ColumnHandle>> predicates = maxComputeTableLayoutHandle.getTupleDomain();
                boolean testIgnorePartitionCache = MaxComputeSessionProperties.getIgnorePartitionCache(session);
                List<Partition> matchedPartitions = maxComputeClient.getTablePartitions(tableMeta, predicates, partitionColumns, testIgnorePartitionCache);

                log.info("ODPS matchedPartSpec=" + matchedPartitions);

                // short circuit if we don't have any partitions
                if (matchedPartitions.isEmpty()) {
                    return new FixedSplitSource(ImmutableList.of());
                }

                return new MaxComputeSplitSource(maxComputeClient, projectName, tableName, maxComputeTableHandle,
                        tableMeta, tableTunnel, matchedPartitions, connectorId, executorService, MaxComputeSessionProperties.getRowsPerSplit(session));
            }
            else {
                DownloadSession downloadSession = maxComputeClient.getDownloadSession(tableTunnel, projectName, tableName);

                long rowCount = downloadSession.getRecordCount();
                long splitCount = getSplitCount(rowCount, splitLimit);
                long step = rowCount / splitCount;

                log.info("ODPS total_row_count=" + rowCount + ", split_count=" + splitCount);

                for (int i = 0; i < splitCount - 1; i++) {
                    MaxComputeSplit split = new MaxComputeSplit(UUID.randomUUID().toString(),
                            connectorId,
                            maxComputeTableHandle.getTableName(),
                            maxComputeTableHandle.getProjectName(),
                            null,
                            step * i,
                            step,
                            tableMeta.getLastDataModifiedTime());
                    builder.add(split);
                    rowCount -= step;
                }

                if (rowCount > 0) {
                    long lastSplitOffset = step * (splitCount - 1);
                    MaxComputeSplit split = new MaxComputeSplit(UUID.randomUUID().toString(),
                            connectorId,
                            maxComputeTableHandle.getTableName(),
                            maxComputeTableHandle.getProjectName(),
                            null,
                            lastSplitOffset,
                            rowCount,
                            tableMeta.getLastDataModifiedTime());
                    builder.add(split);
                }
            }
        }
        catch (Exception e) {
            Throwables.propagate(e);
        }

        return new FixedSplitSource(builder.build());
    }

    private long getSplitCount(long totalRowCount, int splitLimit)
    {
        int[] stepFactors = new int[] {1, 2, 10, 20, 50, 100, 200, 500};
        long splitRowCount = 1000;

        for (int step : stepFactors) {
            if (totalRowCount / step < splitRowCount) {
                return Math.min(step, splitLimit);
            }
        }

        return splitLimit;
    }
}
