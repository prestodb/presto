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
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spark.PrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkMutableRow;
import com.facebook.presto.spark.classloader_interface.PrestoSparkPartitioner;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleSerializer;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskExecutorFactoryProvider;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskOutput;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskProcessor;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskRdd;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskSourceRdd;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskSource;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spark.util.PrestoSparkUtils;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.ShuffledRDD;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;
import scala.reflect.ClassTag;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getMaxSparkInputPartitionCountForAutoTune;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getMaxSplitsDataSizePerSparkPartition;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getMinSparkInputPartitionCountForAutoTune;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getSparkInitialPartitionCount;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.isSparkPartitionCountAutoTuneEnabled;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
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
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Multimaps.asMap;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.union;
import static java.lang.String.format;
import static java.util.Collections.shuffle;
import static java.util.Objects.requireNonNull;

public class PrestoSparkRddFactory
{
    private final SplitManager splitManager;
    private final PartitioningProviderManager partitioningProviderManager;
    private final JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec;
    private final JsonCodec<TaskSource> taskSourceJsonCodec;

    @Inject
    public PrestoSparkRddFactory(
            SplitManager splitManager,
            PartitioningProviderManager partitioningProviderManager,
            JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec,
            JsonCodec<TaskSource> taskSourceJsonCodec)
    {
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.partitioningProviderManager = requireNonNull(partitioningProviderManager, "partitioningProviderManager is null");
        this.taskDescriptorJsonCodec = requireNonNull(taskDescriptorJsonCodec, "taskDescriptorJsonCodec is null");
        this.taskSourceJsonCodec = requireNonNull(taskSourceJsonCodec, "taskSourceJsonCodec is null");
    }

    public <T extends PrestoSparkTaskOutput> JavaPairRDD<MutablePartitionId, T> createSparkRdd(
            JavaSparkContext sparkContext,
            Session session,
            PlanFragment fragment,
            Map<PlanFragmentId, JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow>> rddInputs,
            Map<PlanFragmentId, Broadcast<List<PrestoSparkSerializedPage>>> broadcastInputs,
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

        // set the number of output partitions
        fragment = configureOutputPartitioning(session, fragment);

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

            Map<PlanFragmentId, JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow>> partitionedInputs = rddInputs.entrySet().stream()
                    .collect(toImmutableMap(Entry::getKey, entry -> partitionBy(entry.getValue(), createPartitioner(session, partitioning))));

            return createRdd(
                    sparkContext,
                    session,
                    fragment,
                    executorFactoryProvider,
                    taskInfoCollector,
                    shuffleStatsCollector,
                    tableWriteInfo,
                    partitionedInputs,
                    broadcastInputs,
                    outputType);
        }
        else {
            throw new IllegalArgumentException(format("Unexpected fragment partitioning %s, fragmentId: %s", partitioning, fragment.getId()));
        }
    }

    private PlanFragment configureOutputPartitioning(Session session, PlanFragment fragment)
    {
        PartitioningHandle outputPartitioningHandle = fragment.getPartitioningScheme().getPartitioning().getHandle();
        if (outputPartitioningHandle.equals(FIXED_HASH_DISTRIBUTION)) {
            int hashPartitionCount = getHashPartitionCount(session);
            return fragment.withBucketToPartition(Optional.of(IntStream.range(0, hashPartitionCount).toArray()));
        }
        //  FIXED_ARBITRARY_DISTRIBUTION is used for UNION ALL
        //  UNION ALL inputs could be source inputs or shuffle inputs
        if (outputPartitioningHandle.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            // given modular hash function, partition count could be arbitrary size
            // simply reuse hash_partition_count for convenience
            // it can also be set by a separate session property if needed
            int partitionCount = getHashPartitionCount(session);
            return fragment.withBucketToPartition(Optional.of(IntStream.range(0, partitionCount).toArray()));
        }
        if (outputPartitioningHandle.getConnectorId().isPresent()) {
            int connectorPartitionCount = getPartitionCount(session, outputPartitioningHandle);
            return fragment.withBucketToPartition(Optional.of(IntStream.range(0, connectorPartitionCount).toArray()));
        }
        return fragment;
    }

    private static JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow> partitionBy(JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow> rdd, Partitioner partitioner)
    {
        JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow> javaPairRdd = rdd.partitionBy(partitioner);
        ShuffledRDD<MutablePartitionId, PrestoSparkMutableRow, PrestoSparkMutableRow> shuffledRdd = (ShuffledRDD<MutablePartitionId, PrestoSparkMutableRow, PrestoSparkMutableRow>) javaPairRdd.rdd();
        shuffledRdd.setSerializer(new PrestoSparkShuffleSerializer());
        return JavaPairRDD.fromRDD(
                shuffledRdd,
                classTag(MutablePartitionId.class),
                classTag(PrestoSparkMutableRow.class));
    }

    private Partitioner createPartitioner(Session session, PartitioningHandle partitioning)
    {
        if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            return new PrestoSparkPartitioner(1);
        }
        if (partitioning.equals(FIXED_HASH_DISTRIBUTION) || partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            int hashPartitionCount = getHashPartitionCount(session);
            return new PrestoSparkPartitioner(hashPartitionCount);
        }
        if (partitioning.getConnectorId().isPresent()) {
            int connectorPartitionCount = getPartitionCount(session, partitioning);
            return new PrestoSparkPartitioner(connectorPartitionCount);
        }
        throw new IllegalArgumentException(format("Unexpected fragment partitioning %s", partitioning));
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
            Map<PlanFragmentId, Broadcast<List<PrestoSparkSerializedPage>>> broadcastInputs,
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
            JavaSparkContext sparkContext,
            Session session,
            PartitioningHandle partitioning,
            List<TableScanNode> tableScans,
            Map<PlanNodeId, SplitSource> splitSources,
            Optional<Integer> numberOfShufflePartitions)
    {
        ListMultimap<Integer, TaskSource> taskSourcesMap = ArrayListMultimap.create();
        for (TableScanNode tableScan : tableScans) {
            SplitSource splitSource = requireNonNull(splitSources.get(tableScan.getId()), "split source is missing for table scan node with id: " + tableScan.getId());
            List<ScheduledSplit> scheduledSplits = getSplitsAndCloseSource(tableScan, splitSource);
            shuffle(scheduledSplits);
            SetMultimap<Integer, ScheduledSplit> assignedSplits = assignSplitsToTasks(session, partitioning, scheduledSplits);
            asMap(assignedSplits).forEach((partitionId, splits) ->
                    taskSourcesMap.put(partitionId, new TaskSource(tableScan.getId(), splits, true)));
        }

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
                List<TaskSource> taskSources = requireNonNull(taskSourcesMap.removeAll(partitionId), "taskSources is null");
                taskSourcesByPartitionId.add(serializeTaskSources(taskSources));
            }
        }
        else {
            Iterator<Entry<Integer, Collection<TaskSource>>> partitionsIterator = taskSourcesMap.asMap().entrySet().iterator();
            while (partitionsIterator.hasNext()) {
                Entry<Integer, Collection<TaskSource>> entry = partitionsIterator.next();
                taskSourcesByPartitionId.add(serializeTaskSources(entry.getValue()));
                // Eagerly remove task sources from the map to let GC reclaim the memory
                partitionsIterator.remove();
                // make sure the entry is removed
                verify(taskSourcesMap.get(entry.getKey()).isEmpty());
            }
        }

        return new PrestoSparkTaskSourceRdd(sparkContext.sc(), taskSourcesByPartitionId);
    }

    private List<ScheduledSplit> getSplitsAndCloseSource(TableScanNode tableScan, SplitSource splitSource)
    {
        try {
            List<ScheduledSplit> splits = new ArrayList<>();
            long sequenceId = 0;
            while (!splitSource.isFinished()) {
                List<Split> splitBatch = getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), 1000)).getSplits();
                for (Split split : splitBatch) {
                    splits.add(new ScheduledSplit(sequenceId++, tableScan.getId(), split));
                }
            }
            return splits;
        }
        finally {
            splitSource.close();
        }
    }

    private SetMultimap<Integer, ScheduledSplit> assignSplitsToTasks(Session session, PartitioningHandle partitioning, List<ScheduledSplit> splits)
    {
        // splits from unbucketed table
        if (partitioning.equals(SOURCE_DISTRIBUTION)) {
            return assignSourceDistributionSplits(session, splits);
        }
        // splits from bucketed table
        return assignPartitionedSplits(session, partitioning, splits);
    }

    @VisibleForTesting
    public static SetMultimap<Integer, ScheduledSplit> assignSourceDistributionSplits(Session session, List<ScheduledSplit> splits)
    {
        ImmutableSetMultimap.Builder<Integer, ScheduledSplit> result = ImmutableSetMultimap.builder();

        long maxSplitsSizeInBytesPerPartition = getMaxSplitsDataSizePerSparkPartition(session).toBytes();
        checkArgument(maxSplitsSizeInBytesPerPartition > 0,
                "maxSplitsSizeInBytesPerPartition must be greater than zero: %s", maxSplitsSizeInBytesPerPartition);
        int initialPartitionCount = getSparkInitialPartitionCount(session);
        checkArgument(initialPartitionCount > 0,
                "initialPartitionCount must be greater then zero: %s", initialPartitionCount);

        boolean splitsDataSizeAvailable = splits.stream()
                .allMatch(split -> split.getSplit().getConnectorSplit().getSplitSizeInBytes().isPresent());
        if (!splitsDataSizeAvailable) {
            for (int splitIndex = 0; splitIndex < splits.size(); splitIndex++) {
                result.put(splitIndex % initialPartitionCount, splits.get(splitIndex));
            }
            return result.build();
        }

        splits.sort((ScheduledSplit o1, ScheduledSplit o2) -> {
            long size1 = o1.getSplit().getConnectorSplit().getSplitSizeInBytes().getAsLong();
            long size2 = o2.getSplit().getConnectorSplit().getSplitSizeInBytes().getAsLong();
            return size1 == size2 ? 0 : size1 > size2 ? -1 : 1;
        });

        PriorityQueue<SparkPartition> queue = new PriorityQueue();
        int partitionCount = 0;
        boolean autoTunePartitionCount = isSparkPartitionCountAutoTuneEnabled(session);
        if (autoTunePartitionCount) {
            int minPartitionCount = getMinSparkInputPartitionCountForAutoTune(session);
            int maxPartitionCount = getMaxSparkInputPartitionCountForAutoTune(session);
            checkArgument(minPartitionCount >= 1 && minPartitionCount <= maxPartitionCount,
                    "Min partition count for auto tune (%s) should be a positive integer and not larger than max partition count (%s)",
                    minPartitionCount,
                    maxPartitionCount);

            for (int splitIndex = 0; splitIndex < splits.size(); splitIndex++) {
                int partitionId;
                long splitSizeInBytes = splits.get(splitIndex).getSplit().getConnectorSplit().getSplitSizeInBytes().getAsLong();

                if ((partitionCount >= minPartitionCount && queue.peek().getSplitsInBytes() + splitSizeInBytes <= maxSplitsSizeInBytesPerPartition)
                        || partitionCount == maxPartitionCount) {
                    SparkPartition partition = queue.poll();
                    partitionId = partition.getPartitionId();
                    partition.assignSplitWithSize(splitSizeInBytes);
                    queue.add(partition);
                }
                else {
                    partitionId = partitionCount++;
                    SparkPartition newPartition = new SparkPartition(partitionId);
                    newPartition.assignSplitWithSize(splitSizeInBytes);
                    queue.add(newPartition);
                }

                result.put(partitionId, splits.get(splitIndex));
            }
        }
        else {
            // partition count is fixed
            for (int splitIndex = 0; splitIndex < splits.size(); splitIndex++) {
                int partitionId;
                long splitSizeInBytes = splits.get(splitIndex).getSplit().getConnectorSplit().getSplitSizeInBytes().getAsLong();

                if (partitionCount < initialPartitionCount) {
                    partitionId = partitionCount++;
                    SparkPartition newPartition = new SparkPartition(partitionId);
                    newPartition.assignSplitWithSize(splitSizeInBytes);
                    queue.add(newPartition);
                }
                else {
                    SparkPartition partition = queue.poll();
                    partitionId = partition.getPartitionId();
                    partition.assignSplitWithSize(splitSizeInBytes);
                    queue.add(partition);
                }

                result.put(partitionId, splits.get(splitIndex));
            }
        }

        return result.build();
    }

    private List<SerializedPrestoSparkTaskSource> serializeTaskSources(Collection<TaskSource> taskSources)
    {
        return taskSources.stream()
                .map(taskSourceJsonCodec::toJsonBytes)
                .map(PrestoSparkUtils::compress)
                .map(SerializedPrestoSparkTaskSource::new)
                .collect(toImmutableList());
    }

    private SetMultimap<Integer, ScheduledSplit> assignPartitionedSplits(Session session, PartitioningHandle partitioning, List<ScheduledSplit> splits)
    {
        ToIntFunction<ConnectorSplit> splitBucketFunction = getSplitBucketFunction(session, partitioning);
        ImmutableSetMultimap.Builder<Integer, ScheduledSplit> result = ImmutableSetMultimap.builder();
        for (ScheduledSplit scheduledSplit : splits) {
            int partitionId = splitBucketFunction.applyAsInt(scheduledSplit.getSplit().getConnectorSplit());
            result.put(partitionId, scheduledSplit);
        }
        return result.build();
    }

    private ToIntFunction<ConnectorSplit> getSplitBucketFunction(Session session, PartitioningHandle partitioning)
    {
        ConnectorNodePartitioningProvider partitioningProvider = getPartitioningProvider(partitioning);
        return partitioningProvider.getSplitBucketFunction(
                partitioning.getTransactionHandle().orElse(null),
                session.toConnectorSession(),
                partitioning.getConnectorHandle());
    }

    private int getPartitionCount(Session session, PartitioningHandle partitioning)
    {
        ConnectorNodePartitioningProvider partitioningProvider = getPartitioningProvider(partitioning);
        return partitioningProvider.getBucketCount(
                partitioning.getTransactionHandle().orElse(null),
                session.toConnectorSession(),
                partitioning.getConnectorHandle());
    }

    private ConnectorNodePartitioningProvider getPartitioningProvider(PartitioningHandle partitioning)
    {
        ConnectorId connectorId = partitioning.getConnectorId()
                .orElseThrow(() -> new IllegalArgumentException("Unexpected partitioning: " + partitioning));
        return partitioningProviderManager.getPartitioningProvider(connectorId);
    }

    private static List<TableScanNode> findTableScanNodes(PlanNode node)
    {
        return searchFrom(node)
                .where(TableScanNode.class::isInstance)
                .findAll();
    }

    private static Map<String, Broadcast<List<PrestoSparkSerializedPage>>> toTaskProcessorBroadcastInputs(Map<PlanFragmentId, Broadcast<List<PrestoSparkSerializedPage>>> broadcastInputs)
    {
        return broadcastInputs.entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().toString(), Map.Entry::getValue));
    }

    private static void checkInputs(
            List<RemoteSourceNode> remoteSources,
            Map<PlanFragmentId, JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow>> rddInputs,
            Map<PlanFragmentId, Broadcast<List<PrestoSparkSerializedPage>>> broadcastInputs)
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

    private static <T> ClassTag<T> classTag(Class<T> clazz)
    {
        return scala.reflect.ClassTag$.MODULE$.apply(clazz);
    }

    private static class SparkPartition
            implements Comparable<SparkPartition>
    {
        private final int partitionId;
        private long splitsInBytes;

        public SparkPartition(int partitionId)
        {
            this.partitionId = partitionId;
        }

        @Override
        public int compareTo(SparkPartition o)
        {
            return splitsInBytes == o.splitsInBytes ?
                    0 :
                    splitsInBytes < o.splitsInBytes ? -1 : 1;
        }

        public int getPartitionId()
        {
            return partitionId;
        }

        public void assignSplitWithSize(long splitSizeInBytes)
        {
            splitsInBytes += splitSizeInBytes;
        }

        public long getSplitsInBytes()
        {
            return splitsInBytes;
        }
    }
}
