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
package com.facebook.presto.spark.planner;

import com.facebook.presto.Session;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.split.SplitSource.SplitBatch;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getSplitAssignmentBatchSize;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class PrestoSparkPartitionedSplitAssigner
        implements PrestoSparkSplitAssigner
{
    private final PlanNodeId tableScanNodeId;
    private final SplitSource splitSource;
    private final ToIntFunction<ConnectorSplit> splitBucketFunction;

    private final int maxBatchSize;

    private int sequenceId;

    public static PrestoSparkPartitionedSplitAssigner create(
            Session session,
            PlanNodeId tableScanNodeId,
            SplitSource splitSource,
            PartitioningHandle fragmentPartitioning,
            PartitioningProviderManager partitioningProviderManager)
    {
        return new PrestoSparkPartitionedSplitAssigner(
                tableScanNodeId,
                splitSource,
                getSplitBucketFunction(session, fragmentPartitioning, partitioningProviderManager),
                getSplitAssignmentBatchSize(session));
    }

    private static ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            Session session,
            PartitioningHandle partitioning,
            PartitioningProviderManager partitioningProviderManager)
    {
        ConnectorNodePartitioningProvider partitioningProvider = getPartitioningProvider(partitioning, partitioningProviderManager);
        return partitioningProvider.getSplitBucketFunction(
                partitioning.getTransactionHandle().orElse(null),
                session.toConnectorSession(),
                partitioning.getConnectorHandle());
    }

    private static ConnectorNodePartitioningProvider getPartitioningProvider(PartitioningHandle partitioning, PartitioningProviderManager partitioningProviderManager)
    {
        ConnectorId connectorId = partitioning.getConnectorId()
                .orElseThrow(() -> new IllegalArgumentException("Unexpected partitioning: " + partitioning));
        return partitioningProviderManager.getPartitioningProvider(connectorId);
    }

    public PrestoSparkPartitionedSplitAssigner(
            PlanNodeId tableScanNodeId,
            SplitSource splitSource,
            ToIntFunction<ConnectorSplit> splitBucketFunction,
            int maxBatchSize)
    {
        this.tableScanNodeId = requireNonNull(tableScanNodeId, "tableScanNodeId is null");
        this.splitSource = requireNonNull(splitSource, "splitSource is null");
        this.splitBucketFunction = requireNonNull(splitBucketFunction, "splitBucketFunction is null");
        this.maxBatchSize = maxBatchSize;
        checkArgument(maxBatchSize > 0, "maxBatchSize must be greater than zero");
    }

    @Override
    public Optional<SetMultimap<Integer, ScheduledSplit>> getNextBatch()
    {
        if (splitSource.isFinished()) {
            return Optional.empty();
        }

        List<ScheduledSplit> scheduledSplits = new ArrayList<>();
        while (true) {
            int remaining = maxBatchSize - scheduledSplits.size();
            if (remaining <= 0) {
                break;
            }
            SplitBatch splitBatch = getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), min(remaining, 1000)));
            for (Split split : splitBatch.getSplits()) {
                scheduledSplits.add(new ScheduledSplit(sequenceId++, tableScanNodeId, split));
            }
            if (splitBatch.isLastBatch() || splitSource.isFinished()) {
                break;
            }
        }

        return Optional.of(assignSplitsToTasks(scheduledSplits));
    }

    private SetMultimap<Integer, ScheduledSplit> assignSplitsToTasks(List<ScheduledSplit> splits)
    {
        // expected to be mutable for efficiency reasons
        HashMultimap<Integer, ScheduledSplit> result = HashMultimap.create();
        for (ScheduledSplit scheduledSplit : splits) {
            int partitionId = splitBucketFunction.applyAsInt(scheduledSplit.getSplit().getConnectorSplit());
            result.put(partitionId, scheduledSplit);
        }
        return result;
    }

    @Override
    public void close()
    {
        splitSource.close();
    }
}
