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

import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.spark.PrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkMutableRow;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskExecutorFactoryProvider;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskOutput;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskProcessor;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskRdd;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskSourceRdd;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskSource;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.split.CloseableSplitSourceProvider;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SplitSourceFactory;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import io.airlift.units.DataSize;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spark.util.PrestoSparkUtils.classTag;
import static com.facebook.presto.spark.util.PrestoSparkUtils.serializeZstdCompressed;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.union;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PrestoSparkRddFactory
{
    private static final Logger log = Logger.get(PrestoSparkRddFactory.class);

    private final SplitManager splitManager;
    private final PartitioningProviderManager partitioningProviderManager;
    private final JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec;
    private final Codec<TaskSource> taskSourceCodec;

    @Inject
    public PrestoSparkRddFactory(
            SplitManager splitManager,
            PartitioningProviderManager partitioningProviderManager,
            JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec,
            Codec<TaskSource> taskSourceCodec)
    {
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.partitioningProviderManager = requireNonNull(partitioningProviderManager, "partitioningProviderManager is null");
        this.taskDescriptorJsonCodec = requireNonNull(taskDescriptorJsonCodec, "taskDescriptorJsonCodec is null");
        this.taskSourceCodec = requireNonNull(taskSourceCodec, "taskSourceCodec is null");
    }

    public <T extends PrestoSparkTaskOutput> JavaPairRDD<MutablePartitionId, T> createSparkRdd(
            JavaSparkContext sparkContext,
            Session session,
            PlanFragment fragment,
            Map<PlanFragmentId, JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow>> rddInputs,
            Map<PlanFragmentId, Broadcast<?>> broadcastInputs,
            PrestoSparkTaskExecutorFactoryProvider executorFactoryProvider,
            CollectionAccumulator<SerializedTaskInfo> taskInfoCollector,
            CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector,
            TableWriteInfo tableWriteInfo,
            Class<T> outputType)
    {
        checkArgument(!fragment.getStageExecutionDescriptor().isStageGroupedExecution(), "unexpected grouped execution fragment: %s", fragment.getId());

        PartitioningHandle partitioning = fragment.getPartitioning();

        if (partitioning.equals(SCALED_WRITER_DISTRIBUTION)) {
            throw new PrestoException(NOT_SUPPORTED, "Automatic writers scaling is not supported by Presto on Spark");
        }

        checkArgument(!partitioning.equals(COORDINATOR_DISTRIBUTION), "COORDINATOR_DISTRIBUTION fragment must be run on the driver");
        checkArgument(!partitioning.equals(FIXED_BROADCAST_DISTRIBUTION), "FIXED_BROADCAST_DISTRIBUTION can only be set as an output partitioning scheme, and not as a fragment distribution");
        checkArgument(!partitioning.equals(FIXED_PASSTHROUGH_DISTRIBUTION), "FIXED_PASSTHROUGH_DISTRIBUTION can only be set as local exchange partitioning");

        // TODO: ARBITRARY_DISTRIBUTION is something very weird.
        // TODO: It doesn't have partitioning function, and it is never set as a fragment partitioning.
        // TODO: We should consider removing ARBITRARY_DISTRIBUTION.
        checkArgument(!partitioning.equals(ARBITRARY_DISTRIBUTION), "ARBITRARY_DISTRIBUTION is not expected to be set as a fragment distribution");

        if (partitioning.equals(SINGLE_DISTRIBUTION) ||
                partitioning.equals(FIXED_HASH_DISTRIBUTION) ||
                partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION) ||
                partitioning.equals(SOURCE_DISTRIBUTION) ||
                partitioning.getConnectorId().isPresent()) {
            for (RemoteSourceNode remoteSource : fragment.getRemoteSourceNodes()) {
                if (remoteSource.isEnsureSourceOrdering() || remoteSource.getOrderingScheme().isPresent()) {
                    throw new PrestoException(NOT_SUPPORTED, format(
                            "Order sensitive exchange is not supported by Presto on Spark. fragmentId: %s, sourceFragmentIds: %s",
                            fragment.getId(),
                            remoteSource.getSourceFragmentIds()));
                }
            }

            return createRdd(
                    sparkContext,
                    session,
                    fragment,
                    executorFactoryProvider,
                    taskInfoCollector,
                    shuffleStatsCollector,
                    tableWriteInfo,
                    rddInputs,
                    broadcastInputs,
                    outputType);
        }
        else {
            throw new IllegalArgumentException(format("Unexpected fragment partitioning %s, fragmentId: %s", partitioning, fragment.getId()));
        }
    }

    private <T extends PrestoSparkTaskOutput> JavaPairRDD<MutablePartitionId, T> createRdd(
            JavaSparkContext sparkContext,
            Session session,
            PlanFragment fragment,
            PrestoSparkTaskExecutorFactoryProvider executorFactoryProvider,
            CollectionAccumulator<SerializedTaskInfo> taskInfoCollector,
            CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector,
            TableWriteInfo tableWriteInfo,
            Map<PlanFragmentId, JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow>> rddInputs,
            Map<PlanFragmentId, Broadcast<?>> broadcastInputs,
            Class<T> outputType)
    {
        checkInputs(fragment.getRemoteSourceNodes(), rddInputs, broadcastInputs);

        PrestoSparkTaskDescriptor taskDescriptor = new PrestoSparkTaskDescriptor(
                session.toSessionRepresentation(),
                session.getIdentity().getExtraCredentials(),
                fragment,
                tableWriteInfo);
        SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor = new SerializedPrestoSparkTaskDescriptor(
                taskDescriptorJsonCodec.toJsonBytes(taskDescriptor));

        Optional<Integer> numberOfShufflePartitions = Optional.empty();
        Map<String, RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRddMap = new HashMap<>();
        for (Map.Entry<PlanFragmentId, JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow>> input : rddInputs.entrySet()) {
            RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>> rdd = input.getValue().rdd();
            shuffleInputRddMap.put(input.getKey().toString(), rdd);
            if (!numberOfShufflePartitions.isPresent()) {
                numberOfShufflePartitions = Optional.of(rdd.getNumPartitions());
            }
            else {
                checkArgument(
                        numberOfShufflePartitions.get() == rdd.getNumPartitions(),
                        "Incompatible number of input partitions: %s != %s",
                        numberOfShufflePartitions.get(),
                        rdd.getNumPartitions());
            }
        }

        PrestoSparkTaskProcessor<T> taskProcessor = new PrestoSparkTaskProcessor<>(
                executorFactoryProvider,
                serializedTaskDescriptor,
                taskInfoCollector,
                shuffleStatsCollector,
                toTaskProcessorBroadcastInputs(broadcastInputs),
                outputType);

        Optional<PrestoSparkTaskSourceRdd> taskSourceRdd;
        List<TableScanNode> tableScans = findTableScanNodes(fragment.getRoot());
        if (!tableScans.isEmpty()) {
            try (CloseableSplitSourceProvider splitSourceProvider = new CloseableSplitSourceProvider(splitManager::getSplits)) {
                SplitSourceFactory splitSourceFactory = new SplitSourceFactory(splitSourceProvider, WarningCollector.NOOP);
                Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(fragment, session, tableWriteInfo);
                taskSourceRdd = Optional.of(createTaskSourcesRdd(
                        fragment.getId(),
                        sparkContext,
                        session,
                        fragment.getPartitioning(),
                        tableScans,
                        splitSources,
                        numberOfShufflePartitions));
            }
        }
        else if (rddInputs.size() == 0) {
            checkArgument(fragment.getPartitioning().equals(SINGLE_DISTRIBUTION), "SINGLE_DISTRIBUTION partitioning is expected: %s", fragment.getPartitioning());
            // In case of no inputs we still need to schedule a task.
            // Task with no inputs may produce results (e.g.: ValuesNode).
            // To force the task to be scheduled we create a PrestoSparkTaskSourceRdd that contains exactly one partition.
            // Since there's also no table scans in the fragment, the list of TaskSource's for this partition is empty.
            taskSourceRdd = Optional.of(new PrestoSparkTaskSourceRdd(sparkContext.sc(), ImmutableList.of(ImmutableList.of())));
        }
        else {
            taskSourceRdd = Optional.empty();
        }

        return JavaPairRDD.fromRDD(
                PrestoSparkTaskRdd.create(sparkContext.sc(), taskSourceRdd, shuffleInputRddMap, taskProcessor),
                classTag(MutablePartitionId.class),
                classTag(outputType));
    }

    private PrestoSparkTaskSourceRdd createTaskSourcesRdd(
            PlanFragmentId fragmentId,
            JavaSparkContext sparkContext,
            Session session,
            PartitioningHandle partitioning,
            List<TableScanNode> tableScans,
            Map<PlanNodeId, SplitSource> splitSources,
            Optional<Integer> numberOfShufflePartitions)
    {
        ListMultimap<Integer, SerializedPrestoSparkTaskSource> taskSourcesMap = ArrayListMultimap.create();
        for (TableScanNode tableScan : tableScans) {
            int totalNumberOfSplits = 0;
            SplitSource splitSource = requireNonNull(splitSources.get(tableScan.getId()), "split source is missing for table scan node with id: " + tableScan.getId());
            try (PrestoSparkSplitAssigner splitAssigner = createSplitAssigner(session, tableScan.getId(), splitSource, partitioning)) {
                while (true) {
                    Optional<SetMultimap<Integer, ScheduledSplit>> batch = splitAssigner.getNextBatch();
                    if (!batch.isPresent()) {
                        break;
                    }
                    int numberOfSplitsInCurrentBatch = batch.get().size();
                    log.info("Found %s splits for table scan node with id %s", numberOfSplitsInCurrentBatch, tableScan.getId());
                    totalNumberOfSplits += numberOfSplitsInCurrentBatch;
                    taskSourcesMap.putAll(createTaskSources(tableScan.getId(), batch.get()));
                }
            }
            log.info("Total number of splits for table scan node with id %s: %s", tableScan.getId(), totalNumberOfSplits);
        }

        long allTaskSourcesSerializedSizeInBytes = taskSourcesMap.values().stream()
                .mapToLong(serializedTaskSource -> serializedTaskSource.getBytes().length)
                .sum();
        log.info("Total serialized size of all task sources for fragment %s: %s", fragmentId, DataSize.succinctBytes(allTaskSourcesSerializedSizeInBytes));

        List<List<SerializedPrestoSparkTaskSource>> taskSourcesByPartitionId = new ArrayList<>();
        // If the fragment contains any shuffle inputs, this value will be present
        if (numberOfShufflePartitions.isPresent()) {
            // All input RDD's are expected to have the same number of partitions in order to be zipped.
            // If task sources (splits) are missing for a partition, the partition itself must still be present.
            // Usually this can happen when joining a bucketed table with a non bucketed table.
            // The non bucketed table will be shuffled into K partitions, where K is the number of buckets.
            // The bucketed table may have some buckets missing. To make sure the partitions for bucketed and
            // non bucketed tables match, an empty partition must be inserted if bucket is missing.
            for (int partitionId = 0; partitionId < numberOfShufflePartitions.get(); partitionId++) {
                // Eagerly remove task sources from the map to let GC reclaim the memory
                // If task sources are missing for a partition the removeAll returns an empty list
                taskSourcesByPartitionId.add(requireNonNull(taskSourcesMap.removeAll(partitionId), "taskSources is null"));
            }
        }
        else {
            taskSourcesByPartitionId.addAll(Multimaps.asMap(taskSourcesMap).values());
        }

        return new PrestoSparkTaskSourceRdd(sparkContext.sc(), taskSourcesByPartitionId);
    }

    private PrestoSparkSplitAssigner createSplitAssigner(
            Session session,
            PlanNodeId tableScanNodeId,
            SplitSource splitSource,
            PartitioningHandle fragmentPartitioning)
    {
        // splits from unbucketed table
        if (fragmentPartitioning.equals(SOURCE_DISTRIBUTION)) {
            return PrestoSparkSourceDistributionSplitAssigner.create(session, tableScanNodeId, splitSource);
        }
        // splits from bucketed table
        return PrestoSparkPartitionedSplitAssigner.create(session, tableScanNodeId, splitSource, fragmentPartitioning, partitioningProviderManager);
    }

    private ListMultimap<Integer, SerializedPrestoSparkTaskSource> createTaskSources(PlanNodeId tableScanId, SetMultimap<Integer, ScheduledSplit> assignedSplits)
    {
        ListMultimap<Integer, SerializedPrestoSparkTaskSource> result = ArrayListMultimap.create();
        for (int partitionId : ImmutableSet.copyOf(assignedSplits.keySet())) {
            // remove the entry from the collection to let GC reclaim the memory
            Set<ScheduledSplit> splits = assignedSplits.removeAll(partitionId);
            TaskSource taskSource = new TaskSource(tableScanId, splits, true);
            SerializedPrestoSparkTaskSource serializedTaskSource = new SerializedPrestoSparkTaskSource(serializeZstdCompressed(taskSourceCodec, taskSource));
            result.put(partitionId, serializedTaskSource);
        }
        return result;
    }

    private static List<TableScanNode> findTableScanNodes(PlanNode node)
    {
        return searchFrom(node)
                .where(TableScanNode.class::isInstance)
                .findAll();
    }

    private static Map<String, Broadcast<?>> toTaskProcessorBroadcastInputs(Map<PlanFragmentId, Broadcast<?>> broadcastInputs)
    {
        return broadcastInputs.entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().toString(), Map.Entry::getValue));
    }

    private static void checkInputs(
            List<RemoteSourceNode> remoteSources,
            Map<PlanFragmentId, JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow>> rddInputs,
            Map<PlanFragmentId, Broadcast<?>> broadcastInputs)
    {
        Set<PlanFragmentId> expectedInputs = remoteSources.stream()
                .map(RemoteSourceNode::getSourceFragmentIds)
                .flatMap(List::stream)
                .collect(toImmutableSet());

        Set<PlanFragmentId> actualInputs = union(rddInputs.keySet(), broadcastInputs.keySet());

        Set<PlanFragmentId> missingInputs = difference(expectedInputs, actualInputs);
        Set<PlanFragmentId> extraInputs = difference(actualInputs, expectedInputs);
        checkArgument(
                missingInputs.isEmpty() && extraInputs.isEmpty(),
                "rddInputs mismatch discovered. expected inputs: %s, actual rdd inputs: %s, actual broadcast inputs: %s, missing inputs: %s, extra inputs: %s",
                expectedInputs,
                rddInputs.keySet(),
                broadcastInputs.keySet(),
                missingInputs,
                expectedInputs);
    }
}
