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
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.spark.PrestoSparkTaskCompilerFactory;
import com.facebook.presto.spark.SparkTaskDescriptor;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.spark.TaskProcessors.createTaskProcessor;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class SparkRddPlanner
{
    private final JsonCodec<SparkTaskDescriptor> sparkTaskRequestJsonCodec;

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
        private final CollectionAccumulator<byte[]> taskStatsCollector;
        private final TableWriteInfo tableWriteInfo;

        private RddFactory(
                Session session,
                JsonCodec<SparkTaskDescriptor> sparkTaskRequestJsonCodec,
                JavaSparkContext sparkContext,
                PrestoSparkTaskCompilerFactory taskCompilerFactory,
                int hashPartitionCount,
                CollectionAccumulator<byte[]> taskStatsCollector,
                TableWriteInfo tableWriteInfo)
        {
            this.session = requireNonNull(session, "session is null");
            this.sparkTaskRequestJsonCodec = requireNonNull(sparkTaskRequestJsonCodec, "sparkTaskRequestJsonCodec is null");
            this.sparkContext = requireNonNull(sparkContext, "sparkContext is null");
            this.compilerFactory = requireNonNull(taskCompilerFactory, "taskCompilerFactory is null");
            this.hashPartitionCount = hashPartitionCount;
            this.taskStatsCollector = requireNonNull(taskStatsCollector, "taskStatsCollector is null");
            this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
        }

        public JavaPairRDD<Integer, byte[]> createRdd(SubPlanWithTaskSources subPlan)
        {
            PlanFragment fragment;
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

                List<SparkTaskDescriptor> taskRequests = subPlan.getTaskSources().stream()
                        .flatMap(taskSource -> taskSource.getSplits().stream()
                                .map(split -> createTaskDescriptor(
                                        fragment,
                                        ImmutableList.of(
                                                new TaskSource(
                                                        taskSource.getPlanNodeId(),
                                                        ImmutableSet.of(split),
                                                        taskSource.getNoMoreSplitsForLifespan(),
                                                        taskSource.isNoMoreSplits())))))
                        .collect(toImmutableList());
                List<byte[]> serializedRequests = taskRequests.stream()
                        .map(sparkTaskRequestJsonCodec::toJsonBytes)
                        .collect(toImmutableList());
                return sparkContext.parallelize(serializedRequests)
                        .flatMapToPair(createTaskProcessor(compilerFactory, taskStatsCollector));
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

                PlanFragment childFragment = childSubPlan.getFragment();
                RemoteSourceNode remoteSource = getOnlyElement(remoteSources);
                List<PlanFragmentId> sourceFragmentIds = remoteSource.getSourceFragmentIds();
                checkArgument(sourceFragmentIds.size() == 1, "expected to have exactly only a single source fragment");
                checkArgument(childFragment.getId().equals(getOnlyElement(sourceFragmentIds)));

                SparkTaskDescriptor sparkTaskDescriptor = createTaskDescriptor(fragment, ImmutableList.of());
                byte[] serializedTaskDescriptor = sparkTaskRequestJsonCodec.toJsonBytes(sparkTaskDescriptor);

                PartitioningHandle partitioning = fragment.getPartitioning();
                if (partitioning.equals(COORDINATOR_DISTRIBUTION)) {
                    // TODO: We assume COORDINATOR_DISTRIBUTION always means OutputNode
                    // But it could also be TableFinishNode, in that case we should do collect and table commit on coordinator.

                    // TODO: Do we want to return an RDD for root stage? -- or we should consider the result will not be large?
                    List<Tuple2<Integer, byte[]>> collect = childRdd.collect();
                    List<Tuple2<Integer, byte[]>> result = ImmutableList.copyOf(compilerFactory.create().compile(
                            0,
                            serializedTaskDescriptor,
                            ImmutableMap.of(remoteSource.getId().toString(), collect.iterator()),
                            taskStatsCollector));
                    return sparkContext.parallelizePairs(result);
                }
                else if (partitioning.equals(FIXED_HASH_DISTRIBUTION) ||
                        // when single distribution - there will be a single partition 0
                        partitioning.equals(SINGLE_DISTRIBUTION)) {
                    String planNodeId = remoteSource.getId().toString();
                    return childRdd
                            // TODO: What's the difference of using
                            //  partitionBy/mapPartitionsToPair vs. groupBy vs. mapToPair ???
                            .partitionBy(partitioning.equals(FIXED_HASH_DISTRIBUTION) ? new HashPartitioner(hashPartitionCount) : new HashPartitioner(1))
                            .mapPartitionsToPair(createTaskProcessor(compilerFactory, serializedTaskDescriptor, planNodeId, taskStatsCollector));
                }
                else {
                    // SOURCE_DISTRIBUTION || FIXED_PASSTHROUGH_DISTRIBUTION || ARBITRARY_DISTRIBUTION || SCALED_WRITER_DISTRIBUTION || FIXED_BROADCAST_DISTRIBUTION || FIXED_ARBITRARY_DISTRIBUTION
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

                JavaPairRDD<Integer, byte[]> shuffledLeftChildRdd = leftChildRdd.partitionBy(new HashPartitioner(hashPartitionCount));
                JavaPairRDD<Integer, byte[]> shuffledRightChildRdd = rightChildRdd.partitionBy(new HashPartitioner(hashPartitionCount));

                return JavaPairRDD.fromJavaRDD(
                        shuffledLeftChildRdd.zipPartitions(
                                shuffledRightChildRdd,
                                createTaskProcessor(compilerFactory, serializedTaskDescriptor, leftRemoteSourcePlanId, rightRemoteSourcePlanId, taskStatsCollector)));
            }
            else {
                throw new UnsupportedOperationException();
            }
        }

        private SparkTaskDescriptor createTaskDescriptor(PlanFragment fragment, List<TaskSource> taskSources)
        {
            return new SparkTaskDescriptor(
                    session.toSessionRepresentation(),
                    session.getIdentity().getExtraCredentials(),
                    fragment,
                    taskSources,
                    tableWriteInfo);
        }
    }
}
