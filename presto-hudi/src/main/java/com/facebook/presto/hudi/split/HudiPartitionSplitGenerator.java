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

package com.facebook.presto.hudi.split;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.DataSize;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.util.AsyncQueue;
import com.facebook.presto.hudi.HudiFile;
import com.facebook.presto.hudi.HudiPartition;
import com.facebook.presto.hudi.HudiSplit;
import com.facebook.presto.hudi.HudiTableHandle;
import com.facebook.presto.hudi.HudiTableLayoutHandle;
import com.facebook.presto.hudi.HudiTableType;
import com.facebook.presto.hudi.query.HudiDirectoryLister;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.util.HoodieTimer;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Stream;

import static com.facebook.presto.hudi.HudiSessionProperties.getMinimumAssignedSplitWeight;
import static com.facebook.presto.hudi.HudiSessionProperties.getStandardSplitWeightSize;
import static com.facebook.presto.hudi.HudiSessionProperties.isSizeBasedSplitWeightsEnabled;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * A runnable to take partition names from a queue of partitions to process,
 * generate Hudi splits from the partition, and add the splits to the async
 * ConnectorSplit queue.
 */
public class HudiPartitionSplitGenerator
        implements Runnable
{
    private static final Logger log = Logger.get(HudiPartitionSplitGenerator.class);

    private final HudiTableLayoutHandle layout;
    private final HudiTableHandle table;
    private final HudiDirectoryLister hudiDirectoryLister;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final Queue<HudiPartition> concurrentPartitionQueue;
    private final HudiSplitWeightProvider splitWeightProvider;
    private final Map<String, Partition> partitionMap;
    private final boolean useIndex;

    private boolean isRunning;

    public HudiPartitionSplitGenerator(
            ConnectorSession session,
            HudiTableLayoutHandle layout,
            HudiDirectoryLister hudiDirectoryLister,
            Map<String, Partition> partitionMap,
            AsyncQueue<ConnectorSplit> asyncQueue,
            Deque<HudiPartition> concurrentPartitionQueue,
            boolean useIndex)
    {
        this.layout = requireNonNull(layout, "layout is null");
        this.table = layout.getTableHandle();
        this.hudiDirectoryLister = requireNonNull(hudiDirectoryLister, "fsView is null");
        this.partitionMap = requireNonNull(partitionMap, "partitionMap is null");
        this.asyncQueue = requireNonNull(asyncQueue, "asyncQueue is null");
        this.concurrentPartitionQueue = requireNonNull(concurrentPartitionQueue, "concurrentPartitionQueue is null");
        this.splitWeightProvider = createSplitWeightProvider(requireNonNull(session, "session is null"));
        this.useIndex = useIndex;
        this.isRunning = true;
    }

    @Override
    public void run()
    {
        HoodieTimer timer = HoodieTimer.start();
        while (isRunning || !concurrentPartitionQueue.isEmpty()) {
            HudiPartition hudiPartition = concurrentPartitionQueue.poll();

            if (hudiPartition != null && hudiPartition.getName() != null) {
                generateSplitsFromPartition(hudiPartition);
            }
        }
        log.debug("Partition split generator finished in %d ms", timer.endTimer());
    }

    private void generateSplitsFromPartition(HudiPartition hudiPartition)
    {
        Stream<FileSlice> partitionFileSlices = hudiDirectoryLister.listStatus(hudiPartition, useIndex);

        partitionFileSlices.map(fileSlice -> createHudiSplit(table, fileSlice, table.getLatestCommitTime(), hudiPartition, splitWeightProvider))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(asyncQueue::offer);
    }

    private Optional<HudiSplit> createHudiSplit(
            HudiTableHandle table,
            FileSlice slice,
            String timestamp,
            HudiPartition partition,
            HudiSplitWeightProvider splitWeightProvider)
    {
        HudiFile baseFile = slice.getBaseFile().map(f -> new HudiFile(f.getPath(), 0, f.getFileLen())).orElse(null);
        if (null == baseFile && table.getTableType() == HudiTableType.COW) {
            return Optional.empty();
        }
        List<HudiFile> logFiles = slice.getLogFiles()
                .map(logFile -> new HudiFile(logFile.getPath().toString(), 0, logFile.getFileSize()))
                .collect(toImmutableList());
        long logFilesSize = logFiles.isEmpty() ? 0L : logFiles.stream().map(HudiFile::getLength).reduce(0L, Long::sum);
        long sizeInBytes = baseFile != null ? baseFile.getLength() + logFilesSize : logFilesSize;

        return Optional.of(new HudiSplit(
                table,
                timestamp,
                partition,
                Optional.ofNullable(baseFile),
                logFiles,
                ImmutableList.of(),
                NodeSelectionStrategy.NO_PREFERENCE,
                splitWeightProvider.calculateSplitWeight(sizeInBytes)));
    }

    private static HudiSplitWeightProvider createSplitWeightProvider(ConnectorSession session)
    {
        if (isSizeBasedSplitWeightsEnabled(session)) {
            DataSize standardSplitWeightSize = getStandardSplitWeightSize(session);
            double minimumAssignedSplitWeight = getMinimumAssignedSplitWeight(session);
            return new SizeBasedSplitWeightProvider(minimumAssignedSplitWeight, standardSplitWeightSize);
        }
        return HudiSplitWeightProvider.uniformStandardWeightProvider();
    }

    public void stopRunning()
    {
        this.isRunning = false;
    }
}
