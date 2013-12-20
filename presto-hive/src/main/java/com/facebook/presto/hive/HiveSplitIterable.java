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

import com.facebook.presto.hive.util.AsyncRecursiveWalker;
import com.facebook.presto.hive.util.BoundedExecutor;
import com.facebook.presto.hive.util.FileStatusCallback;
import com.facebook.presto.hive.util.SuspendingExecutor;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Split;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.hive.HiveBucketing.HiveBucket;
import static com.facebook.presto.hive.HiveSplit.markAsLastSplit;
import static com.facebook.presto.hive.HiveType.getSupportedHiveType;
import static com.facebook.presto.hive.HiveUtil.convertNativeHiveType;
import static com.facebook.presto.hive.HiveUtil.getInputFormat;
import static com.facebook.presto.hive.HiveUtil.isSplittable;
import static com.facebook.presto.hive.UnpartitionedPartition.isUnpartitioned;
import static com.facebook.presto.hive.util.FileStatusUtil.isFile;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

class HiveSplitIterable
        implements Iterable<Split>
{
    private static final Split FINISHED_MARKER = new Split()
    {
        @Override
        public boolean isRemotelyAccessible()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getInfo()
        {
            throw new UnsupportedOperationException();
        }
    };

    @SuppressWarnings("ObjectEquality")
    private static boolean isFinished(Split split)
    {
        return split == FINISHED_MARKER;
    }

    private final String clientId;
    private final Table table;
    private final Iterable<String> partitionNames;
    private final Iterable<Partition> partitions;
    private final Optional<HiveBucket> bucket;
    private final int maxOutstandingSplits;
    private final int maxThreads;
    private final HdfsEnvironment hdfsEnvironment;
    private final ExecutorService executor;
    private final ClassLoader classLoader;
    private final DataSize maxSplitSize;
    private final int maxPartitionBatchSize;

    HiveSplitIterable(String clientId,
            Table table,
            Iterable<String> partitionNames,
            Iterable<Partition> partitions,
            Optional<HiveBucket> bucket,
            DataSize maxSplitSize,
            int maxOutstandingSplits,
            int maxThreads,
            HdfsEnvironment hdfsEnvironment,
            ExecutorService executor,
            int maxPartitionBatchSize)
    {
        this.clientId = clientId;
        this.table = table;
        this.partitionNames = partitionNames;
        this.partitions = partitions;
        this.bucket = bucket;
        this.maxSplitSize = maxSplitSize;
        this.maxPartitionBatchSize = maxPartitionBatchSize;
        this.maxOutstandingSplits = maxOutstandingSplits;
        this.maxThreads = maxThreads;
        this.hdfsEnvironment = hdfsEnvironment;
        this.executor = executor;
        this.classLoader = Thread.currentThread().getContextClassLoader();
    }

    @Override
    public Iterator<Split> iterator()
    {
        // Each iterator has its own bounded executor and can be independently suspended
        final SuspendingExecutor suspendingExecutor = new SuspendingExecutor(new BoundedExecutor(executor, maxThreads));
        final HiveSplitQueue hiveSplitQueue = new HiveSplitQueue(maxOutstandingSplits, suspendingExecutor);
        executor.submit(new Callable<Void>()
        {
            @Override
            public Void call()
                    throws Exception
            {
                loadPartitionSplits(hiveSplitQueue, suspendingExecutor);
                return null;
            }
        });
        return hiveSplitQueue;
    }

    private void loadPartitionSplits(final HiveSplitQueue hiveSplitQueue, SuspendingExecutor suspendingExecutor)
            throws InterruptedException
    {
        final Semaphore semaphore = new Semaphore(maxPartitionBatchSize);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            ImmutableList.Builder<ListenableFuture<Void>> futureBuilder = ImmutableList.builder();

            Iterator<String> nameIterator = partitionNames.iterator();
            for (Partition partition : partitions) {
                checkState(nameIterator.hasNext(), "different number of partitions and partition names!");
                semaphore.acquire();
                final String partitionName = nameIterator.next();
                final Properties schema = getPartitionSchema(table, partition);
                final List<HivePartitionKey> partitionKeys = getPartitionKeys(table, partition);

                Path path = new Path(getPartitionLocation(table, partition));
                final Configuration configuration = hdfsEnvironment.getConfiguration(path);
                final InputFormat<?, ?> inputFormat = getInputFormat(configuration, schema, false);
                Path partitionPath = hdfsEnvironment.getFileSystemWrapper().wrap(path);

                FileSystem fs = partitionPath.getFileSystem(configuration);
                final LastSplitMarkingQueue markerQueue = new LastSplitMarkingQueue(hiveSplitQueue);

                if (inputFormat instanceof SymlinkTextInputFormat) {
                    JobConf jobConf = new JobConf(configuration);
                    FileInputFormat.setInputPaths(jobConf, partitionPath);
                    InputSplit[] splits = inputFormat.getSplits(jobConf, 0);
                    for (InputSplit rawSplit : splits) {
                        FileSplit split = ((SymlinkTextInputFormat.SymlinkTextInputSplit) rawSplit).getTargetSplit();

                        // get the filesystem for the target path -- it may be a different hdfs instance
                        FileSystem targetFilesystem = split.getPath().getFileSystem(configuration);
                        FileStatus fileStatus = targetFilesystem.getFileStatus(split.getPath());
                        markerQueue.addToQueue(createHiveSplits(
                                partitionName,
                                fileStatus,
                                targetFilesystem.getFileBlockLocations(fileStatus, split.getStart(), split.getLength()),
                                split.getStart(),
                                split.getLength(),
                                schema,
                                partitionKeys,
                                false));
                    }
                    markerQueue.finish();
                    continue;
                }

                // TODO: this is currently serial across all partitions and should be done in suspendingExecutor
                if (bucket.isPresent()) {
                    Optional<FileStatus> bucketFile = getBucketFile(bucket.get(), fs, path);
                    if (bucketFile.isPresent()) {
                        FileStatus file = bucketFile.get();
                        BlockLocation[] blockLocations = fs.getFileBlockLocations(file, 0, file.getLen());
                        boolean splittable = isSplittable(inputFormat, fs, file.getPath());

                        markerQueue.addToQueue(createHiveSplits(partitionName, file, blockLocations, 0, file.getLen(), schema, partitionKeys, splittable));
                        markerQueue.finish();
                        continue;
                    }
                }

                ListenableFuture<Void> partitionFuture = new AsyncRecursiveWalker(fs, suspendingExecutor).beginWalk(partitionPath, new FileStatusCallback()
                {
                    @Override
                    public void process(FileStatus file, BlockLocation[] blockLocations)
                    {
                        try {
                            boolean splittable = isSplittable(inputFormat, file.getPath().getFileSystem(configuration), file.getPath());

                            markerQueue.addToQueue(createHiveSplits(partitionName, file, blockLocations, 0, file.getLen(), schema, partitionKeys, splittable));
                        }
                        catch (IOException e) {
                            hiveSplitQueue.fail(e);
                        }
                    }
                });

                // release the semaphore when the partition finishes
                Futures.addCallback(partitionFuture, new FutureCallback<Void>()
                {
                    @Override
                    public void onSuccess(Void result)
                    {
                        markerQueue.finish();
                        semaphore.release();
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        markerQueue.finish();
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
                    hiveSplitQueue.finished();
                }

                @Override
                public void onFailure(Throwable t)
                {
                    hiveSplitQueue.fail(t);
                }
            });
        }
        catch (Throwable e) {
            hiveSplitQueue.fail(e);
            Throwables.propagateIfInstanceOf(e, Error.class);
        }
    }

    private static Optional<FileStatus> getBucketFile(HiveBucket bucket, FileSystem fs, Path path)
    {
        FileStatus[] statuses = listStatus(fs, path);

        if (statuses.length != bucket.getBucketCount()) {
            return Optional.absent();
        }

        Map<String, FileStatus> map = new HashMap<>();
        List<String> paths = new ArrayList<>();
        for (FileStatus status : statuses) {
            if (!isFile(status)) {
                return Optional.absent();
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
            boolean splittable)
            throws IOException
    {
        ImmutableList.Builder<HiveSplit> builder = ImmutableList.builder();
        if (splittable) {
            for (BlockLocation blockLocation : blockLocations) {
                // get the addresses for the block
                List<HostAddress> addresses = toHostAddress(blockLocation.getHosts());

                // divide the block into uniform chunks that are smaller than the max split size
                int chunks = Math.max(1, (int) (blockLocation.getLength() / maxSplitSize.toBytes()));
                // when block does not divide evenly into chunks, make the chunk size slightly bigger than necessary
                long targetChunkSize = (long) Math.ceil(blockLocation.getLength() * 1.0 / chunks);

                long chunkOffset = 0;
                while (chunkOffset < blockLocation.getLength()) {
                    // adjust the actual chunk size to account for the overrun when chunks are slightly bigger than necessary (see above)
                    long chunkLength = Math.min(targetChunkSize, blockLocation.getLength() - chunkOffset);

                    builder.add(new HiveSplit(clientId,
                            table.getDbName(),
                            table.getTableName(),
                            partitionName,
                            false,
                            file.getPath().toString(),
                            blockLocation.getOffset() + chunkOffset,
                            chunkLength,
                            schema,
                            partitionKeys,
                            addresses));

                    chunkOffset += chunkLength;
                }
                checkState(chunkOffset == blockLocation.getLength(), "Error splitting blocks");
            }
        }
        else {
            // not splittable, use the hosts from the first block
            builder.add(new HiveSplit(clientId,
                    table.getDbName(),
                    table.getTableName(),
                    partitionName,
                    false,
                    file.getPath().toString(),
                    start,
                    length,
                    schema,
                    partitionKeys,
                    toHostAddress(blockLocations[0].getHosts())));
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

    /**
     * Buffers a single split for a given partition and when the queue
     * is finished, tags the final split so a reader of the stream can
     * know when
     */
    @ThreadSafe
    private static class LastSplitMarkingQueue
    {
        private final HiveSplitQueue hiveSplitQueue;

        private final AtomicReference<HiveSplit> bufferedSplit = new AtomicReference<>();
        private final AtomicBoolean done = new AtomicBoolean();

        private LastSplitMarkingQueue(HiveSplitQueue hiveSplitQueue)
        {
            this.hiveSplitQueue = checkNotNull(hiveSplitQueue, "split is null");
        }

        public synchronized void addToQueue(Iterable<HiveSplit> splits)
        {
            checkNotNull(splits, "splits is null");
            checkState(!done.get(), "already done");

            for (HiveSplit split : splits) {
                HiveSplit previousSplit = bufferedSplit.getAndSet(split);
                if (previousSplit != null) {
                    hiveSplitQueue.addToQueue(previousSplit);
                }
            }
        }

        private synchronized void finish()
        {
            checkState(!done.getAndSet(true), "already done");
            HiveSplit finalSplit = bufferedSplit.getAndSet(null);
            if (finalSplit != null) {
                hiveSplitQueue.addToQueue(markAsLastSplit(finalSplit));
            }
        }
    }

    private static class HiveSplitQueue
            extends AbstractIterator<Split>
    {
        private final BlockingQueue<Split> queue = new LinkedBlockingQueue<>();
        private final AtomicInteger outstandingSplitCount = new AtomicInteger();
        private final AtomicReference<Throwable> throwable = new AtomicReference<>();
        private final int maxOutstandingSplits;
        private final SuspendingExecutor suspendingExecutor;

        private HiveSplitQueue(int maxOutstandingSplits, SuspendingExecutor suspendingExecutor)
        {
            this.maxOutstandingSplits = maxOutstandingSplits;
            this.suspendingExecutor = suspendingExecutor;
        }

        private void addToQueue(Split split)
        {
            queue.add(split);
            if (outstandingSplitCount.incrementAndGet() == maxOutstandingSplits) {
                suspendingExecutor.suspend();
            }
        }

        private void finished()
        {
            queue.add(FINISHED_MARKER);
        }

        private void fail(Throwable e)
        {
            throwable.set(e);
            queue.add(FINISHED_MARKER);
        }

        @Override
        protected Split computeNext()
        {
            try {
                Split split = queue.take();
                if (isFinished(split)) {
                    if (throwable.get() != null) {
                        throw Throwables.propagate(throwable.get());
                    }

                    return endOfData();
                }
                if (outstandingSplitCount.getAndDecrement() == maxOutstandingSplits) {
                    suspendingExecutor.resume();
                }
                return split;
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }
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
            PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = convertNativeHiveType(keys.get(i).getType());
            HiveType hiveType = getSupportedHiveType(primitiveCategory);
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
