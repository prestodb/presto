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
package com.facebook.presto.spark.planner.optimizers;

import com.facebook.presto.Session;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizerResult;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;

import static com.facebook.presto.SystemSessionProperties.getTaskPartitionedWriterCount;
import static com.facebook.presto.SystemSessionProperties.isNativeExecutionEnabled;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_TABLE_WRITER_PARTITION_COUNT;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getSparkTableWriterPartitionCount;
import static com.facebook.presto.spi.StandardWarningCode.PERFORMANCE_WARNING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * When spark_table_writer_partition_count is set, sets the bucket to partition mapping of the remote exchange
 * that feeds a table write requiring a single writer per partition, so that the write runs with at most that many
 * partitions, capped at the bucket count of the table partitioning. Must run after AddExchanges and AddLocalExchanges.
 */
public class AssignTableWriterPartitionCount
        implements PlanOptimizer
{
    private final PartitioningProviderManager partitioningProviderManager;

    public AssignTableWriterPartitionCount(PartitioningProviderManager partitioningProviderManager)
    {
        this.partitioningProviderManager = requireNonNull(partitioningProviderManager, "partitioningProviderManager is null");
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector, boolean forceEnableOptimizer)
    {
        OptionalInt partitionCount = getSparkTableWriterPartitionCount(session);
        if (!partitionCount.isPresent()) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }
        Rewriter rewriter = new Rewriter(session, partitionCount.getAsInt());
        PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, plan);
        if (rewriter.isTableWriteFound() && !rewriter.isPlanChanged()) {
            warningCollector.add(new PrestoWarning(
                    PERFORMANCE_WARNING,
                    format("%s has no effect, because no table write in the query is shuffled on its table partitioning. Likely causes: " +
                                    "the table is neither partitioned nor bucketed; the input is already partitioned like the table; or a write to " +
                                    "a partitioned table keeps its layout to stay safe to retry, because the query calls a non-deterministic function " +
                                    "or uses TABLESAMPLE, its input depends on the order of rows, or it copies an aggregate value that can differ " +
                                    "between attempts, or because a partition column type cannot be hashed", SPARK_TABLE_WRITER_PARTITION_COUNT)));
        }
        return PlanOptimizerResult.optimizerResult(rewrittenPlan, rewriter.isPlanChanged());
    }

    /**
     * Maps the buckets to at most partitionCount partitions. Every partition owns whole buckets and nearly the same
     * number of them. The buckets are dealt to the partitions round robin, in runs of partitionCount buckets in a
     * pseudo-random order, so values with nearby hashes, such as consecutive dates, mostly go to different partitions.
     * For writerCount greater than one, the buckets b with the same b % writerCount are dealt separately, so that the
     * buckets of every partition are also spread evenly over the writer drivers of a Java worker, whose local exchange
     * takes bucket b on driver b % writerCount. The seed is fixed, so the mapping depends only on the arguments.
     */
    @VisibleForTesting
    static int[] assignBucketsToPartitions(int bucketCount, int partitionCount, int writerCount)
    {
        int effectivePartitionCount = min(partitionCount, bucketCount);
        Random random = new Random(0);
        int[] bucketToPartition = new int[bucketCount];
        int position = 0;
        for (int driver = 0; driver < writerCount; driver++) {
            List<Integer> driverBuckets = new ArrayList<>();
            for (int bucket = driver; bucket < bucketCount; bucket += writerCount) {
                driverBuckets.add(bucket);
            }
            for (int runStart = 0; runStart < driverBuckets.size(); runStart += effectivePartitionCount) {
                List<Integer> run = new ArrayList<>(driverBuckets.subList(runStart, min(runStart + effectivePartitionCount, driverBuckets.size())));
                Collections.shuffle(run, random);
                for (int bucket : run) {
                    bucketToPartition[bucket] = position % effectivePartitionCount;
                    position++;
                }
            }
        }
        return bucketToPartition;
    }

    private class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Session session;
        private final int partitionCount;
        private boolean tableWriteFound;
        private boolean planChanged;

        private Rewriter(Session session, int partitionCount)
        {
            this.session = requireNonNull(session, "session is null");
            this.partitionCount = partitionCount;
        }

        public boolean isTableWriteFound()
        {
            return tableWriteFound;
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<Void> context)
        {
            // temporary table writers come from CTE materialization, not from a write of the query
            if (node.getIsTemporaryTableWriter().orElse(false)) {
                return context.defaultRewrite(node);
            }
            tableWriteFound = true;
            if (!node.isSingleWriterPerPartitionRequired()) {
                return context.defaultRewrite(node);
            }
            PartitioningHandle tablePartitioning = node.getTablePartitioningScheme().get().getPartitioning().getHandle();
            return node.replaceChildren(ImmutableList.of(rewriteWriterInput(node.getSource(), tablePartitioning, context)));
        }

        private PlanNode rewriteWriterInput(PlanNode node, PartitioningHandle tablePartitioning, RewriteContext<Void> context)
        {
            if (isRemoteRepartition(node, tablePartitioning)) {
                ExchangeNode exchange = (ExchangeNode) node;
                planChanged = true;
                return new ExchangeNode(
                        exchange.getSourceLocation(),
                        exchange.getId(),
                        exchange.getStatsEquivalentPlanNode(),
                        exchange.getType(),
                        exchange.getScope(),
                        exchange.getPartitioningScheme().withBucketToPartition(Optional.of(assignBucketsToPartitions(
                                getBucketCount(tablePartitioning),
                                partitionCount,
                                // native workers assign the buckets of a task to its writer drivers in their own order
                                isNativeExecutionEnabled(session) ? 1 : getTaskPartitionedWriterCount(session)))),
                        exchange.getSources().stream()
                                .map(context::rewrite)
                                .collect(toImmutableList()),
                        exchange.getInputs(),
                        exchange.isEnsureSourceOrdering(),
                        exchange.getOrderingScheme());
            }
            // Nodes between the writer and its input exchange run in the writer fragment, for example local exchanges,
            // projections or unnests
            if (node.getSources().size() == 1 && !isRemoteExchange(node)) {
                return node.replaceChildren(ImmutableList.of(rewriteWriterInput(getOnlyElement(node.getSources()), tablePartitioning, context)));
            }
            // The writer shares a fragment with its source, so the source decides the partition count
            return context.rewrite(node);
        }

        private boolean isRemoteExchange(PlanNode node)
        {
            return node instanceof ExchangeNode && ((ExchangeNode) node).getScope().isRemote();
        }

        private boolean isRemoteRepartition(PlanNode node, PartitioningHandle tablePartitioning)
        {
            if (!isRemoteExchange(node) || !tablePartitioning.getConnectorId().isPresent()) {
                return false;
            }
            ExchangeNode exchange = (ExchangeNode) node;
            return exchange.getType() == REPARTITION
                    && exchange.getPartitioningScheme().getPartitioning().getHandle().equals(tablePartitioning);
        }

        private int getBucketCount(PartitioningHandle partitioning)
        {
            ConnectorId connectorId = partitioning.getConnectorId().get();
            return partitioningProviderManager.getPartitioningProvider(connectorId).getBucketCount(
                    partitioning.getTransactionHandle().orElse(null),
                    session.toConnectorSession(connectorId),
                    partitioning.getConnectorHandle());
        }
    }
}
