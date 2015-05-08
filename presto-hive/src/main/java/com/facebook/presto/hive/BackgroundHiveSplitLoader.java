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
package com.facebook.presto.hive;

import com.facebook.presto.hive.util.HiveFileIterator;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.hadoop.HadoopFileStatus.isDirectory;
import static com.facebook.presto.hadoop.HadoopFileStatus.isFile;
import static com.facebook.presto.hive.HiveBucketing.HiveBucket;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static com.facebook.presto.hive.HiveType.getSupportedHiveType;
import static com.facebook.presto.hive.HiveUtil.checkCondition;
import static com.facebook.presto.hive.HiveUtil.getInputFormat;
import static com.facebook.presto.hive.HiveUtil.isSplittable;
import static com.facebook.presto.hive.UnpartitionedPartition.isUnpartitioned;
import static com.google.common.base.Preconditions.checkState;

public class BackgroundHiveSplitLoader
        implements HiveSplitLoader
{
    private final String connectorId;
    private final Table table;
    private final Optional<HiveBucket> bucket;
    private final HdfsEnvironment hdfsEnvironment;
    private final NamenodeStats namenodeStats;
    private final DirectoryLister directoryLister;
    private final DataSize maxSplitSize;
    private final int maxPartitionBatchSize;
    private final DataSize maxInitialSplitSize;
    private final boolean recursiveDirWalkerEnabled;
    private final boolean forceLocalScheduling;
    private final Executor executor;
    private final ConnectorSession session;
    private final AtomicInteger outstandingTasks = new AtomicInteger();
    private final ConcurrentLazyQueue<HivePartitionMetadata> partitions;
    private final Deque<HiveFileIterator> fileIterators = new ConcurrentLinkedDeque<>();
    private final AtomicInteger remainingInitialSplits;

    private HiveSplitSource hiveSplitSource;
    private volatile boolean stopped;

    public BackgroundHiveSplitLoader(
            String connectorId,
            Table table,
            Iterable<HivePartitionMetadata> partitions,
            Optional<HiveBucket> bucket,
            DataSize maxSplitSize,
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            NamenodeStats namenodeStats,
            DirectoryLister directoryLister,
            Executor executor,
            int maxPartitionBatchSize,
            DataSize maxInitialSplitSize,
            int maxInitialSplits,
            boolean forceLocalScheduling,
            boolean recursiveDirWalkerEnabled)
    {
        this.connectorId = connectorId;
        this.table = table;
        this.bucket = bucket;
        this.maxSplitSize = maxSplitSize;
        this.maxPartitionBatchSize = maxPartitionBatchSize;
        this.session = session;
        this.hdfsEnvironment = hdfsEnvironment;
        this.namenodeStats = namenodeStats;
        this.directoryLister = directoryLister;
        this.maxInitialSplitSize = maxInitialSplitSize;
        this.remainingInitialSplits = new AtomicInteger(maxInitialSplits);
        this.recursiveDirWalkerEnabled = recursiveDirWalkerEnabled;
        this.forceLocalScheduling = forceLocalScheduling;
        this.executor = executor;
        this.partitions = new ConcurrentLazyQueue<>(partitions);
    }

    @Override
    public void start(HiveSplitSource splitSource)
    {
        this.hiveSplitSource = splitSource;
        startLoadSplits();
    }

    @Override
    public void resume()
    {
        if (outstandingTasks.get() == 0) {
            startLoadSplits();
        }
    }

    @Override
    public void stop()
    {
        stopped = true;
    }

    private void startLoadSplits()
    {
        if (stopped || (fileIterators.isEmpty() && partitions.isEmpty())) {
            return;
        }
        if (outstandingTasks.incrementAndGet() > maxPartitionBatchSize) {
            outstandingTasks.decrementAndGet();
            return;
        }
        executor.execute(() -> {
            try {
                loadSplits();
                if (outstandingTasks.decrementAndGet() == 0) {
                    if (fileIterators.isEmpty() && partitions.isEmpty()) {
                        hiveSplitSource.finished();
                        return;
                    }
                }
                if (!hiveSplitSource.isQueueFull()) {
                    // Start another task to replace this one
                    startLoadSplits();
                    // Ramp up if we're below the limit and the queue still isn't filled
                    if (outstandingTasks.get() < maxPartitionBatchSize) {
                        startLoadSplits();
                    }
                }
            }
            catch (Exception e) {
                hiveSplitSource.fail(e);
            }
        });
    }

    private void loadSplits()
            throws IOException
    {
        HiveFileIterator files = fileIterators.poll();
        if (files == null) {
            HivePartitionMetadata partition = partitions.poll();
            if (partition != null) {
                loadPartition(partition);
            }
            return;
        }

        while (files.hasNext() && !stopped) {
            LocatedFileStatus file = files.next();
            if (isDirectory(file)) {
                if (recursiveDirWalkerEnabled) {
                    HiveFileIterator fileIterator = new HiveFileIterator(
                            file.getPath(),
                            files.getFileSystem(),
                            files.getDirectoryLister(),
                            files.getNamenodeStats(),
                            files.getPartitionName(),
                            files.getInputFormat(),
                            files.getSchema(),
                            files.getPartitionKeys(),
                            files.getEffectivePredicate());
                    fileIterators.add(fileIterator);
                }
            }
            else {
                boolean splittable = isSplittable(files.getInputFormat(), hdfsEnvironment.getFileSystem(file.getPath()), file.getPath());

                hiveSplitSource.addToQueue(createHiveSplits(
                        files.getPartitionName(),
                        file.getPath().toString(),
                        file.getBlockLocations(),
                        0,
                        file.getLen(),
                        files.getSchema(),
                        files.getPartitionKeys(),
                        splittable,
                        session,
                        files.getEffectivePredicate()));
                if (hiveSplitSource.isQueueFull()) {
                    fileIterators.addFirst(files);
                    return;
                }
            }
        }

        // No need to put the iterator back, since it's either empty or we've stopped
    }

    private void loadPartition(HivePartitionMetadata partition)
            throws IOException
    {
        String partitionName = partition.getHivePartition().getPartitionId();
        Properties schema = getPartitionSchema(table, partition.getPartition());
        List<HivePartitionKey> partitionKeys = getPartitionKeys(table, partition.getPartition());
        TupleDomain<HiveColumnHandle> effectivePredicate = partition.getHivePartition().getEffectivePredicate();

        Path path = new Path(getPartitionLocation(table, partition.getPartition()));
        Configuration configuration = hdfsEnvironment.getConfiguration(path);
        InputFormat<?, ?> inputFormat = getInputFormat(configuration, schema, false);

        if (inputFormat instanceof SymlinkTextInputFormat) {
            JobConf jobConf = new JobConf(configuration);
            FileInputFormat.setInputPaths(jobConf, path);
            InputSplit[] splits = inputFormat.getSplits(jobConf, 0);

            // TODO: This should use an iterator like the HiveFileIterator
            for (InputSplit rawSplit : splits) {
                FileSplit split = ((SymlinkTextInputFormat.SymlinkTextInputSplit) rawSplit).getTargetSplit();

                // get the filesystem for the target path -- it may be a different hdfs instance
                FileSystem targetFilesystem = hdfsEnvironment.getFileSystem(split.getPath());
                FileStatus file = targetFilesystem.getFileStatus(split.getPath());
                hiveSplitSource.addToQueue(createHiveSplits(
                        partitionName,
                        file.getPath().toString(),
                        targetFilesystem.getFileBlockLocations(file, split.getStart(), split.getLength()),
                        split.getStart(),
                        split.getLength(),
                        schema,
                        partitionKeys,
                        false,
                        session,
                        effectivePredicate));
                if (stopped) {
                    return;
                }
            }
            return;
        }

        FileSystem fs = hdfsEnvironment.getFileSystem(path);
        if (bucket.isPresent()) {
            Optional<FileStatus> bucketFile = getBucketFile(bucket.get(), fs, path);
            if (bucketFile.isPresent()) {
                FileStatus file = bucketFile.get();
                BlockLocation[] blockLocations = fs.getFileBlockLocations(file, 0, file.getLen());
                boolean splittable = isSplittable(inputFormat, fs, file.getPath());

                hiveSplitSource.addToQueue(createHiveSplits(
                        partitionName,
                        file.getPath().toString(),
                        blockLocations,
                        0,
                        file.getLen(),
                        schema,
                        partitionKeys,
                        splittable,
                        session,
                        effectivePredicate));
                return;
            }
        }

        HiveFileIterator iterator = new HiveFileIterator(path, fs, directoryLister, namenodeStats, partitionName, inputFormat, schema, partitionKeys, effectivePredicate);
        fileIterators.addLast(iterator);
    }

    private static Optional<FileStatus> getBucketFile(HiveBucket bucket, FileSystem fs, Path path)
    {
        FileStatus[] statuses = listStatus(fs, path);

        if (statuses.length != bucket.getBucketCount()) {
            return Optional.empty();
        }

        Map<String, FileStatus> map = new HashMap<>();
        List<String> paths = new ArrayList<>();
        for (FileStatus status : statuses) {
            if (!isFile(status)) {
                return Optional.empty();
            }
            String pathString = status.getPath().toString();
            map.put(pathString, status);
            paths.add(pathString);
        }

        // Hive sorts the paths as strings lexicographically
        Collections.sort(paths);

        String pathString = paths.get(bucket.getBucketNumber());
        return Optional.of(map.get(pathString));
    }

    private static FileStatus[] listStatus(FileSystem fs, Path path)
    {
        try {
            return fs.listStatus(path);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private List<HiveSplit> createHiveSplits(
            String partitionName,
            String path,
            BlockLocation[] blockLocations,
            long start,
            long length,
            Properties schema,
            List<HivePartitionKey> partitionKeys,
            boolean splittable,
            ConnectorSession session,
            TupleDomain<HiveColumnHandle> effectivePredicate)
            throws IOException
    {
        ImmutableList.Builder<HiveSplit> builder = ImmutableList.builder();

        boolean forceLocalScheduling = HiveSessionProperties.getForceLocalScheduling(session, this.forceLocalScheduling);

        if (splittable) {
            for (BlockLocation blockLocation : blockLocations) {
                // get the addresses for the block
                List<HostAddress> addresses = toHostAddress(blockLocation.getHosts());

                long maxBytes = maxSplitSize.toBytes();

                if (remainingInitialSplits.get() > 0) {
                    maxBytes = maxInitialSplitSize.toBytes();
                }

                // divide the block into uniform chunks that are smaller than the max split size
                int chunks = Math.max(1, (int) (blockLocation.getLength() / maxBytes));
                // when block does not divide evenly into chunks, make the chunk size slightly bigger than necessary
                long targetChunkSize = (long) Math.ceil(blockLocation.getLength() * 1.0 / chunks);

                long chunkOffset = 0;
                while (chunkOffset < blockLocation.getLength()) {
                    // adjust the actual chunk size to account for the overrun when chunks are slightly bigger than necessary (see above)
                    long chunkLength = Math.min(targetChunkSize, blockLocation.getLength() - chunkOffset);

                    builder.add(new HiveSplit(connectorId,
                            table.getDbName(),
                            table.getTableName(),
                            partitionName,
                            path,
                            blockLocation.getOffset() + chunkOffset,
                            chunkLength,
                            schema,
                            partitionKeys,
                            addresses,
                            forceLocalScheduling,
                            session,
                            effectivePredicate));

                    chunkOffset += chunkLength;
                    remainingInitialSplits.decrementAndGet();
                }
                checkState(chunkOffset == blockLocation.getLength(), "Error splitting blocks");
            }
        }
        else {
            // not splittable, use the hosts from the first block if it exists
            List<HostAddress> addresses = ImmutableList.of();
            if (blockLocations.length > 0) {
                addresses = toHostAddress(blockLocations[0].getHosts());
            }

            builder.add(new HiveSplit(connectorId,
                    table.getDbName(),
                    table.getTableName(),
                    partitionName,
                    path,
                    start,
                    length,
                    schema,
                    partitionKeys,
                    addresses,
                    forceLocalScheduling,
                    session,
                    effectivePredicate));
        }
        return builder.build();
    }

    private static List<HostAddress> toHostAddress(String[] hosts)
    {
        ImmutableList.Builder<HostAddress> builder = ImmutableList.builder();
        for (String host : hosts) {
            builder.add(HostAddress.fromString(host));
        }
        return builder.build();
    }

    private static List<HivePartitionKey> getPartitionKeys(Table table, Partition partition)
    {
        if (isUnpartitioned(partition)) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<HivePartitionKey> partitionKeys = ImmutableList.builder();
        List<FieldSchema> keys = table.getPartitionKeys();
        List<String> values = partition.getValues();
        checkCondition(keys.size() == values.size(), HIVE_INVALID_METADATA, "Expected %s partition key values, but got %s", keys.size(), values.size());
        for (int i = 0; i < keys.size(); i++) {
            String name = keys.get(i).getName();
            HiveType hiveType = getSupportedHiveType(keys.get(i).getType());
            String value = values.get(i);
            checkCondition(value != null, HIVE_INVALID_PARTITION_VALUE, "partition key value cannot be null for field: %s", name);
            partitionKeys.add(new HivePartitionKey(name, hiveType, value));
        }
        return partitionKeys.build();
    }

    private static Properties getPartitionSchema(Table table, Partition partition)
    {
        if (isUnpartitioned(partition)) {
            return MetaStoreUtils.getTableMetadata(table);
        }
        return MetaStoreUtils.getSchema(partition, table);
    }

    private static String getPartitionLocation(Table table, Partition partition)
    {
        if (isUnpartitioned(partition)) {
            return table.getSd().getLocation();
        }
        return partition.getSd().getLocation();
    }
}
