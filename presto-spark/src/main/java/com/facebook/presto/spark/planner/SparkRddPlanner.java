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
import com.facebook.presto.spark.SparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskCompilerFactory;
import com.facebook.presto.spark.common.IntegerIdentityPartitioner;
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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
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

public class SparkRddPlanner
{
    private final JsonCodec<SparkTaskDescriptor> sparkTaskRequestJsonCodec;
    private final int initialSparkPartitionCount = 100; // TODO: make this configurable

    @Inject
    public SparkRddPlanner(JsonCodec<SparkTaskDescriptor> sparkTaskRequestJsonCodec)
    {
        this.sparkTaskRequestJsonCodec = requireNonNull(sparkTaskRequestJsonCodec, "sparkTaskRequestJsonCodec is null");
    }

    public JavaPairRDD<Integer, byte[]> createSparkRdd(
            JavaSparkContext sparkContext,
            Session session,
            PreparedPlan preparedPlan,
            PrestoSparkTaskCompilerFactory taskCompilerFactory,
            CollectionAccumulator<byte[]> taskStatsCollector)
    {
        RddFactory rddFactory = new RddFactory(
                session,
                sparkTaskRequestJsonCodec,
                sparkContext,
                taskCompilerFactory,
                initialSparkPartitionCount,
                getHashPartitionCount(session),
                taskStatsCollector,
                preparedPlan.getTableWriteInfo());

        return rddFactory.createRdd(preparedPlan.getPlan());
    }

    private static class RddFactory
    {
        private final Session session;
        private final JsonCodec<SparkTaskDescriptor> sparkTaskRequestJsonCodec;
        private final JavaSparkContext sparkContext;
        private final PrestoSparkTaskCompilerFactory compilerFactory;
        private final int hashPartitionCount;
        private final int initialSparkPartitionCount;
        private final CollectionAccumulator<byte[]> taskStatsCollector;
        private final TableWriteInfo tableWriteInfo;

        private RddFactory(
                Session session,
                JsonCodec<SparkTaskDescriptor> sparkTaskRequestJsonCodec,
                JavaSparkContext sparkContext,
                PrestoSparkTaskCompilerFactory taskCompilerFactory,
                int initialSparkPartitionCount,
                int hashPartitionCount,
                CollectionAccumulator<byte[]> taskStatsCollector,
                TableWriteInfo tableWriteInfo)
        {
            this.session = requireNonNull(session, "session is null");
            this.sparkTaskRequestJsonCodec = requireNonNull(sparkTaskRequestJsonCodec, "sparkTaskRequestJsonCodec is null");
            this.sparkContext = requireNonNull(sparkContext, "sparkContext is null");
            this.compilerFactory = requireNonNull(taskCompilerFactory, "taskCompilerFactory is null");
            this.initialSparkPartitionCount = initialSparkPartitionCount;
            this.hashPartitionCount = hashPartitionCount;
            this.taskStatsCollector = requireNonNull(taskStatsCollector, "taskStatsCollector is null");
            this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
        }

        public JavaPairRDD<Integer, byte[]> createRdd(SubPlanWithTaskSources subPlan)
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
            checkArgument(tableScans.isEmpty() || remoteSources.isEmpty(), "stage has both, remote sources and table scans");

            if (!tableScans.isEmpty()) {
                checkArgument(fragment.getPartitioning().equals(SOURCE_DISTRIBUTION), "unexpected table scan partitioning: %s", fragment.getPartitioning());

                // get all scheduled splits
                List<ScheduledSplit> scheduledSplits = subPlan.getTaskSources().stream()
                        .flatMap(taskSource -> taskSource.getSplits().stream())
                        .collect(toImmutableList());

                // get scheduled splits by task
                List<List<ScheduledSplit>> assignedSplits = assignSplitsToTasks(scheduledSplits, initialSparkPartitionCount);

                List<byte[]> serializedRequests = assignedSplits.stream()
                        .map(splits -> createTaskDescriptor(fragment, splits))
                        .map(sparkTaskRequestJsonCodec::toJsonBytes)
                        .collect(toImmutableList());

                return sparkContext.parallelize(serializedRequests, initialSparkPartitionCount)
                        .mapPartitionsToPair(createTaskProcessor(compilerFactory, taskStatsCollector));
            }

            List<SubPlanWithTaskSources> children = subPlan.getChildren();
            checkArgument(
                    remoteSources.size() == children.size(),
                    "number of remote sources doesn't match the number of child stages: %s != %s",
                    remoteSources.size(),
                    children.size());

            if (children.size() == 1) {
                // Single remote source
                SubPlanWithTaskSources childSubPlan = getOnlyElement(children);
                JavaPairRDD<Integer, byte[]> childRdd = createRdd(childSubPlan);
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

                SparkTaskDescriptor sparkTaskDescriptor = createTaskDescriptor(fragment, ImmutableList.of());
                byte[] serializedTaskDescriptor = sparkTaskRequestJsonCodec.toJsonBytes(sparkTaskDescriptor);

                if (partitioning.equals(FIXED_HASH_DISTRIBUTION) ||
                        // when single distribution - there will be a single partition 0
                        partitioning.equals(SINGLE_DISTRIBUTION)) {
                    String planNodeId = remoteSource.getId().toString();
                    return childRdd
                            // TODO: What's the difference of using
                            //  partitionBy/mapPartitionsToPair vs. groupBy vs. mapToPair ???
                            .partitionBy(partitioning.equals(FIXED_HASH_DISTRIBUTION) ? new IntegerIdentityPartitioner(hashPartitionCount) : new IntegerIdentityPartitioner(1))
                            .mapPartitionsToPair(createTaskProcessor(compilerFactory, serializedTaskDescriptor, planNodeId, taskStatsCollector));
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
                // handle simple two-way join
                SubPlanWithTaskSources leftSubPlan = children.get(0);
                SubPlanWithTaskSources rightSubPlan = children.get(1);

                RemoteSourceNode leftRemoteSource = remoteSources.get(0);
                RemoteSourceNode rightRemoteSource = remoteSources.get(1);

                // We need String representation since PlanNodeId is not serializable...
                String leftRemoteSourcePlanId = leftRemoteSource.getId().toString();
                String rightRemoteSourcePlanId = rightRemoteSource.getId().toString();

                JavaPairRDD<Integer, byte[]> leftChildRdd = createRdd(leftSubPlan);
                JavaPairRDD<Integer, byte[]> rightChildRdd = createRdd(rightSubPlan);

                PlanFragment leftFragment = leftSubPlan.getFragment();
                PlanFragment rightFragment = rightSubPlan.getFragment();

                List<PlanFragmentId> leftFragmentIds = leftRemoteSource.getSourceFragmentIds();
                checkArgument(leftFragmentIds.size() == 1, "expected to have exactly only a single source fragment");
                checkArgument(leftFragment.getId().equals(getOnlyElement(leftFragmentIds)));
                List<PlanFragmentId> rightFragmentIds = rightRemoteSource.getSourceFragmentIds();
                checkArgument(rightFragmentIds.size() == 1, "expected to have exactly only a single source fragment");
                checkArgument(rightFragment.getId().equals(getOnlyElement(rightFragmentIds)));

                // This fragment only contains remote source, thus there is no splits
                SparkTaskDescriptor sparkTaskDescriptor = createTaskDescriptor(fragment, ImmutableList.of());
                byte[] serializedTaskDescriptor = sparkTaskRequestJsonCodec.toJsonBytes(sparkTaskDescriptor);

                PartitioningHandle partitioning = fragment.getPartitioning();
                checkArgument(partitioning.equals(FIXED_HASH_DISTRIBUTION));

                JavaPairRDD<Integer, byte[]> shuffledLeftChildRdd = leftChildRdd.partitionBy(new IntegerIdentityPartitioner(hashPartitionCount));
                JavaPairRDD<Integer, byte[]> shuffledRightChildRdd = rightChildRdd.partitionBy(new IntegerIdentityPartitioner(hashPartitionCount));

                return JavaPairRDD.fromJavaRDD(
                        shuffledLeftChildRdd.zipPartitions(
                                shuffledRightChildRdd,
                                createTaskProcessor(compilerFactory, serializedTaskDescriptor, leftRemoteSourcePlanId, rightRemoteSourcePlanId, taskStatsCollector)));
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
                int taskId = split.hashCode() % numTasks;
                if (taskId < 0) {
                    taskId += numTasks;
                }

                assignedSplits.get(taskId).add(split);
            }

            return assignedSplits;
        }

        private SparkTaskDescriptor createTaskDescriptor(PlanFragment fragment, List<ScheduledSplit> splits)
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

            return new SparkTaskDescriptor(
                    session.toSessionRepresentation(),
                    session.getIdentity().getExtraCredentials(),
                    fragment,
                    taskSourceByPlanNode,
                    tableWriteInfo);
        }
    }
}
