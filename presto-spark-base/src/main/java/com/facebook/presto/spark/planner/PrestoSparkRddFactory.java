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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.spark.PrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.IntegerIdentityPartitioner;
import com.facebook.presto.spark.classloader_interface.PrestoSparkRow;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskExecutorFactoryProvider;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.SerializedTaskStats;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.CollectionAccumulator;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getSparkInitialPartitionCount;
import static com.facebook.presto.spark.classloader_interface.TaskProcessors.createTaskProcessor;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

public class PrestoSparkRddFactory
{
    private final JsonCodec<PrestoSparkTaskDescriptor> sparkTaskRequestJsonCodec;

    @Inject
    public PrestoSparkRddFactory(JsonCodec<PrestoSparkTaskDescriptor> sparkTaskRequestJsonCodec)
    {
        this.sparkTaskRequestJsonCodec = requireNonNull(sparkTaskRequestJsonCodec, "sparkTaskRequestJsonCodec is null");
    }

    public JavaPairRDD<Integer, PrestoSparkRow> createSparkRdd(
            JavaSparkContext sparkContext,
            Session session,
            PrestoSparkPlan prestoSparkPlan,
            PrestoSparkTaskExecutorFactoryProvider taskExecutorFactoryProvider,
            CollectionAccumulator<SerializedTaskStats> taskStatsCollector)
    {
        RddFactory rddFactory = new RddFactory(
                session,
                sparkTaskRequestJsonCodec,
                sparkContext,
                taskExecutorFactoryProvider,
                getSparkInitialPartitionCount(session),
                getHashPartitionCount(session),
                taskStatsCollector,
                prestoSparkPlan.getTableWriteInfo());
        return rddFactory.createRdd(prestoSparkPlan.getPlan());
    }

    private static class RddFactory
    {
        private final Session session;
        private final JsonCodec<PrestoSparkTaskDescriptor> sparkTaskDescriptorJsonCodec;
        private final JavaSparkContext sparkContext;
        private final PrestoSparkTaskExecutorFactoryProvider executorFactoryProvider;
        private final int initialSparkPartitionCount;
        private final int hashPartitionCount;
        private final CollectionAccumulator<SerializedTaskStats> taskStatsCollector;
        private final TableWriteInfo tableWriteInfo;

        private RddFactory(
                Session session,
                JsonCodec<PrestoSparkTaskDescriptor> sparkTaskDescriptorJsonCodec,
                JavaSparkContext sparkContext,
                PrestoSparkTaskExecutorFactoryProvider executorFactoryProvider,
                int initialSparkPartitionCount,
                int hashPartitionCount,
                CollectionAccumulator<SerializedTaskStats> taskStatsCollector,
                TableWriteInfo tableWriteInfo)
        {
            this.session = requireNonNull(session, "session is null");
            this.sparkTaskDescriptorJsonCodec = requireNonNull(sparkTaskDescriptorJsonCodec, "sparkTaskDescriptorJsonCodec is null");
            this.sparkContext = requireNonNull(sparkContext, "sparkContext is null");
            this.executorFactoryProvider = requireNonNull(executorFactoryProvider, "executorFactoryProvider is null");
            this.initialSparkPartitionCount = initialSparkPartitionCount;
            this.hashPartitionCount = hashPartitionCount;
            this.taskStatsCollector = requireNonNull(taskStatsCollector, "taskStatsCollector is null");
            this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
        }

        public JavaPairRDD<Integer, PrestoSparkRow> createRdd(PrestoSparkSubPlan subPlan)
        {
            PlanFragment fragment;
            // TODO: fragment adaption should be done prior to RDD creation
            if (subPlan.getFragment().getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_HASH_DISTRIBUTION)) {
                fragment = subPlan.getFragment().withBucketToPartition(Optional.of(IntStream.range(0, hashPartitionCount).toArray()));
            }
            else {
                fragment = subPlan.getFragment();
            }

            checkArgument(!fragment.getStageExecutionDescriptor().isStageGroupedExecution(), "unexpected grouped execution fragment: %s", fragment.getId());

            // scans
            List<PlanNodeId> tableScans = fragment.getTableScanSchedulingOrder();

            // source stages
            List<RemoteSourceNode> remoteSources = fragment.getRemoteSourceNodes();
            checkArgument(tableScans.isEmpty() || remoteSources.isEmpty(), "stages that have both, remote sources and table scans, are not supported");

            if (!tableScans.isEmpty()) {
                checkArgument(fragment.getPartitioning().equals(SOURCE_DISTRIBUTION), "unexpected table scan partitioning: %s", fragment.getPartitioning());

                // get all scheduled splits
                List<ScheduledSplit> scheduledSplits = subPlan.getTaskSources().stream()
                        .flatMap(taskSource -> taskSource.getSplits().stream())
                        .collect(toImmutableList());

                // get scheduled splits by task
                List<List<ScheduledSplit>> assignedSplits = assignSplitsToTasks(scheduledSplits, initialSparkPartitionCount);

                List<SerializedPrestoSparkTaskDescriptor> serializedRequests = assignedSplits.stream()
                        .map(splits -> createTaskDescriptor(fragment, splits))
                        .map(sparkTaskDescriptorJsonCodec::toJsonBytes)
                        .map(SerializedPrestoSparkTaskDescriptor::new)
                        .collect(toImmutableList());

                return sparkContext.parallelize(serializedRequests, initialSparkPartitionCount)
                        .mapPartitionsToPair(createTaskProcessor(executorFactoryProvider, taskStatsCollector));
            }

            List<PrestoSparkSubPlan> children = subPlan.getChildren();
            checkArgument(
                    remoteSources.size() == children.size(),
                    "number of remote sources doesn't match the number of child stages: %s != %s",
                    remoteSources.size(),
                    children.size());

            if (children.size() == 1) {
                // Single remote source
                PrestoSparkSubPlan childSubPlan = getOnlyElement(children);
                JavaPairRDD<Integer, PrestoSparkRow> childRdd = createRdd(childSubPlan);
                PartitioningHandle partitioning = fragment.getPartitioning();

                if (partitioning.equals(COORDINATOR_DISTRIBUTION)) {
                    // coordinator side work will be handled after JavaPairRDD#collect() call in PrestoSparkExecution
                    return childRdd;
                }

                PlanFragment childFragment = childSubPlan.getFragment();
                RemoteSourceNode remoteSource = getOnlyElement(remoteSources);
                List<PlanFragmentId> sourceFragmentIds = remoteSource.getSourceFragmentIds();
                checkArgument(sourceFragmentIds.size() == 1, "expected to have exactly only a single source fragment");
                checkArgument(childFragment.getId().equals(getOnlyElement(sourceFragmentIds)));

                PrestoSparkTaskDescriptor taskDescriptor = createTaskDescriptor(fragment, ImmutableList.of());
                SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor = new SerializedPrestoSparkTaskDescriptor(sparkTaskDescriptorJsonCodec.toJsonBytes(taskDescriptor));

                if (partitioning.equals(FIXED_HASH_DISTRIBUTION) ||
                        // when single distribution - there will be a single partition 0
                        partitioning.equals(SINGLE_DISTRIBUTION)) {
                    String planNodeId = remoteSource.getId().toString();
                    return childRdd
                            .partitionBy(partitioning.equals(FIXED_HASH_DISTRIBUTION) ? new IntegerIdentityPartitioner(hashPartitionCount) : new IntegerIdentityPartitioner(1))
                            .mapPartitionsToPair(createTaskProcessor(executorFactoryProvider, serializedTaskDescriptor, planNodeId, taskStatsCollector));
                }
                else {
                    // TODO: support (or do check state over) the following fragment partitioning:
                    //  - SOURCE_DISTRIBUTION
                    //  - FIXED_PASSTHROUGH_DISTRIBUTION
                    //  - ARBITRARY_DISTRIBUTION
                    //  - SCALED_WRITER_DISTRIBUTION
                    //  - FIXED_BROADCAST_DISTRIBUTION
                    //  - FIXED_ARBITRARY_DISTRIBUTION
                    throw new IllegalArgumentException("Unsupported fragment partitioning: " + partitioning);
                }
            }
            else if (children.size() == 2) {
                // TODO: support N way join
                PrestoSparkSubPlan leftSubPlan = children.get(0);
                PrestoSparkSubPlan rightSubPlan = children.get(1);

                RemoteSourceNode leftRemoteSource = remoteSources.get(0);
                RemoteSourceNode rightRemoteSource = remoteSources.get(1);

                // We need String representation since PlanNodeId is not serializable...
                String leftRemoteSourcePlanId = leftRemoteSource.getId().toString();
                String rightRemoteSourcePlanId = rightRemoteSource.getId().toString();

                JavaPairRDD<Integer, PrestoSparkRow> leftChildRdd = createRdd(leftSubPlan);
                JavaPairRDD<Integer, PrestoSparkRow> rightChildRdd = createRdd(rightSubPlan);

                PlanFragment leftFragment = leftSubPlan.getFragment();
                PlanFragment rightFragment = rightSubPlan.getFragment();

                List<PlanFragmentId> leftFragmentIds = leftRemoteSource.getSourceFragmentIds();
                checkArgument(leftFragmentIds.size() == 1, "expected to have exactly only a single source fragment");
                checkArgument(leftFragment.getId().equals(getOnlyElement(leftFragmentIds)));
                List<PlanFragmentId> rightFragmentIds = rightRemoteSource.getSourceFragmentIds();
                checkArgument(rightFragmentIds.size() == 1, "expected to have exactly only a single source fragment");
                checkArgument(rightFragment.getId().equals(getOnlyElement(rightFragmentIds)));

                // This fragment only contains remote source, thus there is no splits
                PrestoSparkTaskDescriptor taskDescriptor = createTaskDescriptor(fragment, ImmutableList.of());
                SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor = new SerializedPrestoSparkTaskDescriptor(sparkTaskDescriptorJsonCodec.toJsonBytes(taskDescriptor));

                PartitioningHandle partitioning = fragment.getPartitioning();
                checkArgument(partitioning.equals(FIXED_HASH_DISTRIBUTION));

                JavaPairRDD<Integer, PrestoSparkRow> shuffledLeftChildRdd = leftChildRdd.partitionBy(new IntegerIdentityPartitioner(hashPartitionCount));
                JavaPairRDD<Integer, PrestoSparkRow> shuffledRightChildRdd = rightChildRdd.partitionBy(new IntegerIdentityPartitioner(hashPartitionCount));
                return JavaPairRDD.fromJavaRDD(
                        shuffledLeftChildRdd.zipPartitions(
                                shuffledRightChildRdd,
                                createTaskProcessor(executorFactoryProvider, serializedTaskDescriptor, leftRemoteSourcePlanId, rightRemoteSourcePlanId, taskStatsCollector)));
            }
            else {
                throw new UnsupportedOperationException();
            }
        }

        private static List<List<ScheduledSplit>> assignSplitsToTasks(List<ScheduledSplit> scheduledSplits, int numTasks)
        {
            List<List<ScheduledSplit>> assignedSplits = new ArrayList<>();
            for (int i = 0; i < numTasks; i++) {
                assignedSplits.add(new ArrayList<>());
            }

            for (ScheduledSplit split : scheduledSplits) {
                int taskId = Objects.hash(split.getPlanNodeId(), split.getSequenceId()) % numTasks;
                if (taskId < 0) {
                    taskId += numTasks;
                }

                assignedSplits.get(taskId).add(split);
            }

            return assignedSplits;
        }

        private PrestoSparkTaskDescriptor createTaskDescriptor(PlanFragment fragment, List<ScheduledSplit> splits)
        {
            Map<PlanNodeId, Set<ScheduledSplit>> splitsByPlanNode = splits.stream()
                    .collect(Collectors.groupingBy(
                            ScheduledSplit::getPlanNodeId,
                            mapping(identity(), toSet())));

            List<TaskSource> taskSourceByPlanNode = splitsByPlanNode.entrySet().stream()
                    .map(entry -> new TaskSource(
                            entry.getKey(),
                            entry.getValue(),
                            ImmutableSet.of(),
                            true))
                    .collect(toImmutableList());

            return new PrestoSparkTaskDescriptor(
                    session.toSessionRepresentation(),
                    session.getIdentity().getExtraCredentials(),
                    fragment,
                    taskSourceByPlanNode,
                    tableWriteInfo);
        }
    }
}
