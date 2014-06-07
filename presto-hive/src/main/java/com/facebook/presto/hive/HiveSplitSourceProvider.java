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
import com.facebook.presto.hive.util.BoundedExecutor;
import com.facebook.presto.hive.util.FileStatusCallback;
import com.facebook.presto.hive.util.SetThreadName;
import com.facebook.presto.hive.util.SuspendingExecutor;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
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

import javax.annotation.concurrent.GuardedBy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.hadoop.HadoopFileStatus.isFile;
import static com.facebook.presto.hive.HiveBucketing.HiveBucket;
import static com.facebook.presto.hive.HiveType.getSupportedHiveType;
import static com.facebook.presto.hive.HiveUtil.convertNativeHiveType;
import static com.facebook.presto.hive.HiveUtil.getInputFormat;
import static com.facebook.presto.hive.HiveUtil.isSplittable;
import static com.facebook.presto.hive.UnpartitionedPartition.isUnpartitioned;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

class HiveSplitSourceProvider
{
    private static final ConnectorSplit FINISHED_MARKER = new ConnectorSplit()
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
    }

    public ConnectorSplitSource get()
    {
        // Each iterator has its own bounded executor and can be independently suspended
        final SuspendingExecutor suspendingExecutor = new SuspendingExecutor(new BoundedExecutor(executor, maxThreads));
        final HiveSplitSource splitSource = new HiveSplitSource(connectorId, maxOutstandingSplits, suspendingExecutor);

        FutureTask<?> producer = new FutureTask<>(new Runnable()
        {
            @Override
            public void run()
            {
                try (SetThreadName ignored = new SetThreadName("HiveSplitProducer")) {
                    loadPartitionSplits(splitSource, suspendingExecutor, session);
                }
            }
        }, null);

        executor.execute(producer);
        splitSource.setProducerFuture(producer);

        return splitSource;
    }

    private void loadPartitionSplits(final HiveSplitSource hiveSplitSource, SuspendingExecutor suspendingExecutor, final ConnectorSession session)
    {
        final Semaphore semaphore = new Semaphore(maxPartitionBatchSize);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            ImmutableList.Builder<ListenableFuture<Void>> futureBuilder = ImmutableList.builder();

            for (HivePartitionMetadata partition : partitions) {
                final String partitionName = partition.getHivePartition().getPartitionId();
                final Properties schema = getPartitionSchema(table, partition.getPartition());
                final List<HivePartitionKey> partitionKeys = getPartitionKeys(table, partition.getPartition());
                final TupleDomain<HiveColumnHandle> tupleDomain = (TupleDomain<HiveColumnHandle>) (Object) partition.getHivePartition().getTupleDomain();

                Path path = new Path(getPartitionLocation(table, partition.getPartition()));
                final Configuration configuration = hdfsEnvironment.getConfiguration(path);
                final InputFormat<?, ?> inputFormat = getInputFormat(configuration, schema, false);

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
                                tupleDomain));
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

                        hiveSplitSource.addToQueue(createHiveSplits(partitionName, file, blockLocations, 0, file.getLen(), schema, partitionKeys, splittable, session, tupleDomain));
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

                ListenableFuture<Void> partitionFuture = createAsyncWalker(fs, suspendingExecutor).beginWalk(path, new FileStatusCallback()
                {
                    @Override
                    public void process(FileStatus file, BlockLocation[] blockLocations)
                    {
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
                                    tupleDomain));
                        }
                        catch (IOException e) {
                            hiveSplitSource.fail(e);
                        }
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
            boolean splittable,
            ConnectorSession session,
            TupleDomain<HiveColumnHandle> tupleDomain)
            throws IOException
    {
        ImmutableList.Builder<HiveSplit> builder = ImmutableList.builder();
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
                            session,
                            tupleDomain));

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
                    session,
                    tupleDomain));
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

    @VisibleForTesting
    static class HiveSplitSource
            implements ConnectorSplitSource
    {
        private final String connectorId;
        private final BlockingQueue<ConnectorSplit> queue = new LinkedBlockingQueue<>();
        private final AtomicInteger outstandingSplitCount = new AtomicInteger();
        private final AtomicReference<Throwable> throwable = new AtomicReference<>();
        private final int maxOutstandingSplits;
        private final SuspendingExecutor suspendingExecutor;
        private volatile boolean closed;

        @GuardedBy("this")
        private Future<?> producerFuture;

        @VisibleForTesting
        HiveSplitSource(String connectorId, int maxOutstandingSplits, SuspendingExecutor suspendingExecutor)
        {
            this.connectorId = connectorId;
            this.maxOutstandingSplits = maxOutstandingSplits;
            this.suspendingExecutor = suspendingExecutor;
        }

        @VisibleForTesting
        int getOutstandingSplitCount()
        {
            return outstandingSplitCount.get();
        }

        void addToQueue(Iterable<? extends ConnectorSplit> splits)
        {
            for (ConnectorSplit split : splits) {
                addToQueue(split);
            }
        }

        @VisibleForTesting
        void addToQueue(ConnectorSplit split)
        {
            if (throwable.get() == null) {
                queue.add(split);
                if (outstandingSplitCount.incrementAndGet() >= maxOutstandingSplits) {
                    suspendingExecutor.suspend();
                }
            }
        }

        @VisibleForTesting
        void finished()
        {
            if (throwable.get() == null) {
                queue.add(FINISHED_MARKER);
            }
        }

        @VisibleForTesting
        void fail(Throwable e)
        {
            // only record the first error message
            if (throwable.compareAndSet(null, e)) {
                // add the finish marker
                queue.add(FINISHED_MARKER);

                // no need to process any more jobs
                suspendingExecutor.suspend();
            }
        }

        @Override
        public String getDataSourceName()
        {
            return connectorId;
        }

        @Override
        public List<ConnectorSplit> getNextBatch(int maxSize)
                throws InterruptedException
        {
            checkState(!closed, "Provider is already closed");

            // wait for at least one split and then take as may extra splits as possible
            // if an error has been registered, the take will succeed immediately because
            // will be at least one finished marker in the queue
            List<ConnectorSplit> splits = new ArrayList<>(maxSize);
            splits.add(queue.take());
            queue.drainTo(splits, maxSize - 1);

            // check if we got the finished marker in our list
            int finishedIndex = splits.indexOf(FINISHED_MARKER);
            if (finishedIndex >= 0) {
                // add the finish marker back to the queue so future callers will not block indefinitely
                queue.add(FINISHED_MARKER);
                // drop all splits after the finish marker (this shouldn't happen in a normal exit, but be safe)
                splits = splits.subList(0, finishedIndex);
            }

            // Before returning, check if there is a registered failure.
            // If so, we want to throw the error, instead of returning because the scheduler can block
            // while scheduling splits and wait for work to finish before continuing.  In this case,
            // we want to end the query as soon as possible and abort the work
            if (throwable.get() != null) {
                throw propagatePrestoException(throwable.get());
            }

            // decrement the outstanding split count by the number of splits we took
            if (outstandingSplitCount.addAndGet(-splits.size()) < maxOutstandingSplits) {
                // we are below the low water mark (and there isn't a failure) so resume scanning hdfs
                suspendingExecutor.resume();
            }

            return splits;
        }

        @Override
        public boolean isFinished()
        {
            // the finished marker must be checked before checking the throwable
            // to avoid a race with the fail method
            boolean isFinished = queue.peek() == FINISHED_MARKER;
            if (throwable.get() != null) {
                throw propagatePrestoException(throwable.get());
            }
            return isFinished;
        }

        @Override
        public void close()
        {
            queue.add(FINISHED_MARKER);
            suspendingExecutor.suspend();

            synchronized (this) {
                closed = true;

                if (producerFuture != null) {
                    producerFuture.cancel(true);
                }
            }
        }

        public synchronized void setProducerFuture(Future<?> future)
        {
            producerFuture = future;

            // someone may have called close before calling this method
            if (closed) {
                producerFuture.cancel(true);
            }
        }

        private RuntimeException propagatePrestoException(Throwable throwable)
        {
            if (throwable instanceof PrestoException) {
                throw (PrestoException) throwable;
            }
            if (throwable instanceof FileNotFoundException) {
                throw new PrestoException(HiveErrorCode.HIVE_FILE_NOT_FOUND.toErrorCode(), throwable);
            }
            throw new PrestoException(HiveErrorCode.HIVE_UNKNOWN_ERROR.toErrorCode(), throwable);
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
