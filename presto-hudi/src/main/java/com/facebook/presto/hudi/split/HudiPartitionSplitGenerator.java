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
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.util.AsyncQueue;
import com.facebook.presto.hudi.HudiColumnHandle;
import com.facebook.presto.hudi.HudiFile;
import com.facebook.presto.hudi.HudiPartition;
import com.facebook.presto.hudi.HudiSplit;
import com.facebook.presto.hudi.HudiTableHandle;
import com.facebook.presto.hudi.HudiTableLayoutHandle;
import com.facebook.presto.hudi.HudiTableType;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Stream;

import static com.facebook.presto.hive.metastore.MetastoreUtil.extractPartitionValues;
import static com.facebook.presto.hudi.HudiErrorCode.HUDI_INVALID_METADATA;
import static com.facebook.presto.hudi.HudiMetadata.fromDataColumns;
import static com.facebook.presto.hudi.HudiSessionProperties.getMinimumAssignedSplitWeight;
import static com.facebook.presto.hudi.HudiSessionProperties.getStandardSplitWeightSize;
import static com.facebook.presto.hudi.HudiSessionProperties.isSizeBasedSplitWeightsEnabled;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePath;

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
    private final Path tablePath;
    private final HoodieTableFileSystemView fsView;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final Queue<String> concurrentPartitionQueue;
    private final String latestInstant;
    private final HudiSplitWeightProvider splitWeightProvider;
    private final Map<String, Partition> partitions;

    public HudiPartitionSplitGenerator(
            ConnectorSession session,
            HudiTableLayoutHandle layout,
            HoodieTableFileSystemView fsView,
            Map<String, Partition> partitions,
            AsyncQueue<ConnectorSplit> asyncQueue,
            Queue<String> concurrentPartitionQueue,
            String latestInstant)
    {
        this.layout = requireNonNull(layout, "layout is null");
        this.table = layout.getTable();
        this.tablePath = new Path(table.getPath());
        this.fsView = requireNonNull(fsView, "fsView is null");
        this.partitions = requireNonNull(partitions, "partitionMap is null");
        this.asyncQueue = requireNonNull(asyncQueue, "asyncQueue is null");
        this.concurrentPartitionQueue = requireNonNull(concurrentPartitionQueue, "concurrentPartitionQueue is null");
        this.latestInstant = requireNonNull(latestInstant, "latestInstant is null");
        this.splitWeightProvider = createSplitWeightProvider(requireNonNull(session, "session is null"));
    }

    @Override
    public void run()
    {
        HoodieTimer timer = HoodieTimer.start();
        while (!concurrentPartitionQueue.isEmpty()) {
            String partitionName = concurrentPartitionQueue.poll();
            if (partitionName != null) {
                generateSplitsFromPartition(partitionName);
            }
        }
        log.debug("Partition split generator finished in %d ms", timer.endTimer());
    }

    private void generateSplitsFromPartition(String partitionName)
    {
        HudiPartition hudiPartition = getHudiPartition(layout, partitionName);
        Path partitionPath = new Path(hudiPartition.getStorage().getLocation());
        String relativePartitionPath = getRelativePartitionPath(convertToStoragePath(tablePath), convertToStoragePath(partitionPath));
        Stream<FileSlice> fileSlices = HudiTableType.MOR.equals(table.getTableType()) ?
                fsView.getLatestMergedFileSlicesBeforeOrOn(relativePartitionPath, latestInstant) :
                fsView.getLatestFileSlicesBeforeOrOn(relativePartitionPath, latestInstant, false);
        fileSlices.map(fileSlice -> createHudiSplit(table, fileSlice, latestInstant, hudiPartition, splitWeightProvider))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(asyncQueue::offer);
    }

    private HudiPartition getHudiPartition(HudiTableLayoutHandle tableLayout, String partitionName)
    {
        String databaseName = tableLayout.getTable().getSchemaName();
        String tableName = tableLayout.getTable().getTableName();
        List<HudiColumnHandle> partitionColumns = tableLayout.getPartitionColumns();

        if (partitionColumns.isEmpty()) {
            // non-partitioned tableLayout
            Table metastoreTable = Optional.ofNullable(table.getTable())
                    .orElseThrow(() -> new PrestoException(HUDI_INVALID_METADATA, format("Table %s.%s expected but not found", databaseName, tableName)));
            return new HudiPartition(partitionName, ImmutableList.of(), ImmutableMap.of(), metastoreTable.getStorage(), tableLayout.getDataColumns());
        }
        else {
            // partitioned tableLayout
            List<String> partitionValues = extractPartitionValues(partitionName);
            checkArgument(partitionColumns.size() == partitionValues.size(),
                    format("Invalid partition name %s for partition columns %s", partitionName, partitionColumns));
            Partition partition = Optional.ofNullable(partitions.get(partitionName))
                    .orElseThrow(() -> new PrestoException(HUDI_INVALID_METADATA, format("Partition %s expected but not found", partitionName)));
            Map<String, String> keyValues = zipPartitionKeyValues(partitionColumns, partitionValues);
            return new HudiPartition(partitionName, partitionValues, keyValues, partition.getStorage(), fromDataColumns(partition.getColumns()));
        }
    }

    private Map<String, String> zipPartitionKeyValues(List<HudiColumnHandle> partitionColumns, List<String> partitionValues)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        Streams.forEachPair(partitionColumns.stream(), partitionValues.stream(),
                (column, value) -> builder.put(column.getName(), value));
        return builder.build();
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
        long logFilesSize = logFiles.isEmpty() ? 0L : logFiles.stream().mapToLong(HudiFile::getLength).sum();
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
}
