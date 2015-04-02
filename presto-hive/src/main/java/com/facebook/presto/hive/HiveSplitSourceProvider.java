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

import com.facebook.presto.hive.util.AsyncWalker;
import com.facebook.presto.hive.util.SuspendingExecutor;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.concurrent.SetThreadName;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;

import static com.facebook.presto.hadoop.HadoopFileStatus.isFile;
import static com.facebook.presto.hive.HiveBucketing.HiveBucket;
import static com.facebook.presto.hive.HiveType.getSupportedHiveType;
import static com.facebook.presto.hive.HiveUtil.getInputFormat;
import static com.facebook.presto.hive.HiveUtil.isSplittable;
import static com.facebook.presto.hive.UnpartitionedPartition.isUnpartitioned;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

class HiveSplitSourceProvider
{
    public static final String FORCE_LOCAL_SCHEDULING = "force_local_scheduling";

    private final String connectorId;
    private final Table table;
    private final Iterable<HivePartitionMetadata> partitions;
    private final Optional<HiveBucket> bucket;
    private final int maxOutstandingSplits;
    private final int maxThreads;
    private final HdfsEnvironment hdfsEnvironment;
    private final NamenodeStats namenodeStats;
    private final DirectoryLister directoryLister;
    private final Executor executor;
    private final ClassLoader classLoader;
    private final DataSize maxSplitSize;
    private final int maxPartitionBatchSize;
    private final DataSize maxInitialSplitSize;
    private long remainingInitialSplits;
    private final ConnectorSession session;
    private final boolean recursiveDirWalkerEnabled;
    private final boolean forceLocalScheduling;

    HiveSplitSourceProvider(String connectorId,
            Table table,
            Iterable<HivePartitionMetadata> partitions,
            Optional<HiveBucket> bucket,
            DataSize maxSplitSize,
            int maxOutstandingSplits,
            int maxThreads,
            HdfsEnvironment hdfsEnvironment,
            NamenodeStats namenodeStats,
            DirectoryLister directoryLister,
            Executor executor,
            int maxPartitionBatchSize,
            ConnectorSession session,
            DataSize maxInitialSplitSize,
            int maxInitialSplits,
            boolean forceLocalScheduling,
            boolean recursiveDirWalkerEnabled)
    {
        this.connectorId = connectorId;
        this.table = table;
        this.partitions = partitions;
        this.bucket = bucket;
        this.maxSplitSize = maxSplitSize;
        this.maxPartitionBatchSize = maxPartitionBatchSize;
        this.maxOutstandingSplits = maxOutstandingSplits;
        this.maxThreads = maxThreads;
        this.hdfsEnvironment = hdfsEnvironment;
        this.namenodeStats = namenodeStats;
        this.directoryLister = directoryLister;
        this.executor = executor;
        this.session = session;
        this.classLoader = Thread.currentThread().getContextClassLoader();
        this.maxInitialSplitSize = maxInitialSplitSize;
        this.remainingInitialSplits = maxInitialSplits;
        this.recursiveDirWalkerEnabled = recursiveDirWalkerEnabled;
        this.forceLocalScheduling = forceLocalScheduling;
    }

    public ConnectorSplitSource get()
    {
        // Each iterator has its own bounded executor and can be independently suspended
        SuspendingExecutor suspendingExecutor = new SuspendingExecutor(new BoundedExecutor(executor, maxThreads));
        HiveSplitSource splitSource = new HiveSplitSource(connectorId, maxOutstandingSplits, suspendingExecutor);

        FutureTask<?> producer = new FutureTask<>(() -> {
            try (SetThreadName ignored = new SetThreadName("HiveSplitProducer")) {
                loadPartitionSplits(splitSource, suspendingExecutor, session);
            }
        }, null);

        executor.execute(producer);
        splitSource.setProducerFuture(producer);

        return splitSource;
    }

    private void loadPartitionSplits(HiveSplitSource hiveSplitSource, SuspendingExecutor suspendingExecutor, ConnectorSession session)
    {
        Semaphore semaphore = new Semaphore(maxPartitionBatchSize);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            ImmutableList.Builder<ListenableFuture<Void>> futureBuilder = ImmutableList.builder();

            for (HivePartitionMetadata partition : partitions) {
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
                    for (InputSplit rawSplit : splits) {
                        FileSplit split = ((SymlinkTextInputFormat.SymlinkTextInputSplit) rawSplit).getTargetSplit();

                        // get the filesystem for the target path -- it may be a different hdfs instance
                        FileSystem targetFilesystem = hdfsEnvironment.getFileSystem(split.getPath());
                        FileStatus fileStatus = targetFilesystem.getFileStatus(split.getPath());
                        hiveSplitSource.addToQueue(createHiveSplits(
                                partitionName,
                                fileStatus,
                                targetFilesystem.getFileBlockLocations(fileStatus, split.getStart(), split.getLength()),
                                split.getStart(),
                                split.getLength(),
                                schema,
                                partitionKeys,
                                false,
                                session,
                                effectivePredicate));
                    }
                    continue;
                }

                // TODO: this is currently serial across all partitions and should be done in suspendingExecutor
                FileSystem fs = hdfsEnvironment.getFileSystem(path);
                if (bucket.isPresent()) {
                    Optional<FileStatus> bucketFile = getBucketFile(bucket.get(), fs, path);
                    if (bucketFile.isPresent()) {
                        FileStatus file = bucketFile.get();
                        BlockLocation[] blockLocations = fs.getFileBlockLocations(file, 0, file.getLen());
                        boolean splittable = isSplittable(inputFormat, fs, file.getPath());

                        hiveSplitSource.addToQueue(createHiveSplits(
                                partitionName,
                                file,
                                blockLocations,
                                0,
                                file.getLen(),
                                schema,
                                partitionKeys,
                                splittable,
                                session,
                                effectivePredicate));

                        continue;
                    }
                }

                // Acquire semaphore so that we only have a fixed number of outstanding partitions being processed asynchronously
                // NOTE: there must not be any calls that throw in the space between acquiring the semaphore and setting the Future
                // callback to release it. Otherwise, we will need a try-finally block around this section.
                try {
                    semaphore.acquire();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                ListenableFuture<Void> partitionFuture = createAsyncWalker(fs, suspendingExecutor).beginWalk(path, (file, blockLocations) -> {
                    try {
                        boolean splittable = isSplittable(inputFormat, hdfsEnvironment.getFileSystem(file.getPath()), file.getPath());

                        hiveSplitSource.addToQueue(createHiveSplits(
                                partitionName,
                                file,
                                blockLocations,
                                0,
                                file.getLen(),
                                schema,
                                partitionKeys,
                                splittable,
                                session,
                                effectivePredicate));
                    }
                    catch (IOException e) {
                        hiveSplitSource.fail(e);
                    }
                });

                // release the semaphore when the partition finishes
                Futures.addCallback(partitionFuture, new FutureCallback<Void>()
                {
                    @Override
                    public void onSuccess(Void result)
                    {
                        semaphore.release();
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        semaphore.release();
                    }
                });

                futureBuilder.add(partitionFuture);
            }

            // when all partitions finish, mark the queue as finished
            Futures.addCallback(Futures.allAsList(futureBuilder.build()), new FutureCallback<List<Void>>()
            {
                @Override
                public void onSuccess(List<Void> result)
                {
                    hiveSplitSource.finished();
                }

                @Override
                public void onFailure(Throwable t)
                {
                    hiveSplitSource.fail(t);
                }
            });
        }
        catch (Throwable e) {
            hiveSplitSource.fail(e);
            Throwables.propagateIfInstanceOf(e, Error.class);
        }
    }

    private AsyncWalker createAsyncWalker(FileSystem fs, SuspendingExecutor suspendingExecutor)
    {
        return new AsyncWalker(fs, suspendingExecutor, directoryLister, namenodeStats, recursiveDirWalkerEnabled);
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
            FileStatus file,
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

        boolean forceLocalScheduling = getForceLocalScheduling(session);

        if (splittable) {
            for (BlockLocation blockLocation : blockLocations) {
                // get the addresses for the block
                List<HostAddress> addresses = toHostAddress(blockLocation.getHosts());

                long maxBytes = maxSplitSize.toBytes();

                if (remainingInitialSplits > 0) {
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
                            file.getPath().toString(),
                            blockLocation.getOffset() + chunkOffset,
                            chunkLength,
                            schema,
                            partitionKeys,
                            addresses,
                            forceLocalScheduling,
                            session,
                            effectivePredicate));

                    chunkOffset += chunkLength;
                    remainingInitialSplits--;
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
                    file.getPath().toString(),
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

    private boolean getForceLocalScheduling(ConnectorSession session)
    {
        String forceLocalScheduling = session.getProperties().get(FORCE_LOCAL_SCHEDULING);
        if (forceLocalScheduling == null) {
            return this.forceLocalScheduling;
        }

        try {
            return Boolean.valueOf(forceLocalScheduling);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NOT_SUPPORTED, "Invalid Hive session property '" + FORCE_LOCAL_SCHEDULING + "=" + forceLocalScheduling + "'");
        }
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
        checkArgument(keys.size() == values.size(), "Expected %s partition key values, but got %s", keys.size(), values.size());
        for (int i = 0; i < keys.size(); i++) {
            String name = keys.get(i).getName();
            HiveType hiveType = getSupportedHiveType(keys.get(i).getType());
            String value = values.get(i);
            checkNotNull(value, "partition key value cannot be null for field: %s", name);
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
