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

import com.facebook.presto.hive.HdfsEnvironment.HdfsContext;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.util.HiveFileIterator;
import com.facebook.presto.hive.util.InternalHiveSplitFactory;
import com.facebook.presto.hive.util.ResumableTask;
import com.facebook.presto.hive.util.ResumableTasks;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.annotation.Annotation;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.IntPredicate;

import static com.facebook.presto.hadoop.HadoopFileStatus.isDirectory;
import static com.facebook.presto.hive.HiveBucketing.HiveBucket;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static com.facebook.presto.hive.HiveSessionProperties.isForceLocalScheduling;
import static com.facebook.presto.hive.HiveUtil.checkCondition;
import static com.facebook.presto.hive.HiveUtil.getInputFormat;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveSchema;
import static com.facebook.presto.hive.util.ConfigurationUtils.toJobConf;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.common.FileUtils.HIDDEN_FILES_PATH_FILTER;

public class BackgroundHiveSplitLoader
        implements HiveSplitLoader
{
    private static final String CORRUPT_BUCKETING = "Hive table is corrupt. It is declared as being bucketed, but the files do not match the bucketing declaration.";

    private static final ListenableFuture<?> COMPLETED_FUTURE = immediateFuture(null);

    private final Table table;
    private final TupleDomain<? extends ColumnHandle> compactEffectivePredicate;
    private final Optional<BucketSplitInfo> bucketSplitInfo;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final NamenodeStats namenodeStats;
    private final DirectoryLister directoryLister;
    private final int loaderConcurrency;
    private final boolean recursiveDirWalkerEnabled;
    private final Executor executor;
    private final ConnectorSession session;
    private final ConcurrentLazyQueue<HivePartitionMetadata> partitions;
    private final Deque<HiveFileIterator> fileIterators = new ConcurrentLinkedDeque<>();

    // Purpose of this lock:
    // * When write lock is acquired, except the holder, no one can do any of the following:
    // ** poll from partitions
    // ** poll from or push to fileIterators
    // ** push to hiveSplitSource
    // * When any of the above three operations is carried out, either a read lock or a write lock must be held.
    // * When a series of operations involving two or more of the above three operations are carried out, the lock
    //   must be continuously held throughout the series of operations.
    // Implications:
    // * if you hold a read lock but not a write lock, you can do any of the above three operations, but you may
    //   see a series of operations involving two or more of the operations carried out half way.
    private final ReentrantReadWriteLock taskExecutionLock = new ReentrantReadWriteLock();

    private HiveSplitSource hiveSplitSource;
    private volatile boolean stopped;

    public BackgroundHiveSplitLoader(
            Table table,
            Iterable<HivePartitionMetadata> partitions,
            TupleDomain<? extends ColumnHandle> compactEffectivePredicate,
            Optional<BucketSplitInfo> bucketSplitInfo,
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            NamenodeStats namenodeStats,
            DirectoryLister directoryLister,
            Executor executor,
            int loaderConcurrency,
            boolean recursiveDirWalkerEnabled)
    {
        this.table = table;
        this.compactEffectivePredicate = compactEffectivePredicate;
        this.bucketSplitInfo = bucketSplitInfo;
        this.loaderConcurrency = loaderConcurrency;
        this.session = session;
        this.hdfsEnvironment = hdfsEnvironment;
        this.namenodeStats = namenodeStats;
        this.directoryLister = directoryLister;
        this.recursiveDirWalkerEnabled = recursiveDirWalkerEnabled;
        this.executor = executor;
        this.partitions = new ConcurrentLazyQueue<>(partitions);
        this.hdfsContext = new HdfsContext(session, table.getDatabaseName(), table.getTableName());
    }

    @Override
    public void start(HiveSplitSource splitSource)
    {
        this.hiveSplitSource = splitSource;
        for (int i = 0; i < loaderConcurrency; i++) {
            ResumableTasks.submit(executor, new HiveSplitLoaderTask());
        }
    }

    @Override
    public void stop()
    {
        stopped = true;
    }

    private class HiveSplitLoaderTask
            implements ResumableTask
    {
        @Override
        public TaskStatus process()
        {
            while (true) {
                if (stopped) {
                    return TaskStatus.finished();
                }
                ListenableFuture<?> future;
                taskExecutionLock.readLock().lock();
                try {
                    future = loadSplits();
                }
                catch (Exception e) {
                    // Fail the split source before releasing the execution lock
                    // Otherwise, a race could occur where the split source is completed before we fail it.
                    hiveSplitSource.fail(e);
                    checkState(stopped);
                    return TaskStatus.finished();
                }
                finally {
                    taskExecutionLock.readLock().unlock();
                }
                invokeNoMoreSplitsIfNecessary();
                if (!future.isDone()) {
                    return TaskStatus.continueOn(future);
                }
            }
        }
    }

    private void invokeNoMoreSplitsIfNecessary()
    {
        if (partitions.isEmpty() && fileIterators.isEmpty()) {
            taskExecutionLock.writeLock().lock();
            try {
                // the write lock guarantees that no one is operating on the partitions, fileIterators, or hiveSplitSource, or half way through doing so.
                if (partitions.isEmpty() && fileIterators.isEmpty()) {
                    // It is legal to call `noMoreSplits` multiple times or after `stop` was called.
                    // Nothing bad will happen if `noMoreSplits` implementation calls methods that will try to obtain a read lock because the lock is re-entrant.
                    hiveSplitSource.noMoreSplits();
                }
            }
            finally {
                taskExecutionLock.writeLock().unlock();
            }
        }
    }

    private ListenableFuture<?> loadSplits()
            throws IOException
    {
        HiveFileIterator files = fileIterators.poll();
        if (files == null) {
            HivePartitionMetadata partition = partitions.poll();
            if (partition == null) {
                return COMPLETED_FUTURE;
            }
            return loadPartition(partition);
        }

        while (files.hasNext() && !stopped) {
            LocatedFileStatus file = files.next();
            if (isDirectory(file)) {
                if (recursiveDirWalkerEnabled) {
                    fileIterators.add(files.withPath(file.getPath()));
                }
            }
            else {
                Optional<InternalHiveSplit> internalHiveSplit = files.getSplitFactory().createInternalHiveSplit(file);
                if (!internalHiveSplit.isPresent()) {
                    continue;
                }
                ListenableFuture<?> future = hiveSplitSource.addToQueue(internalHiveSplit.get());
                if (!future.isDone()) {
                    fileIterators.addFirst(files);
                    return future;
                }
            }
        }

        // No need to put the iterator back, since it's either empty or we've stopped
        return COMPLETED_FUTURE;
    }

    private ListenableFuture<?> loadPartition(HivePartitionMetadata partition)
            throws IOException
    {
        String partitionName = partition.getHivePartition().getPartitionId();
        Properties schema = getPartitionSchema(table, partition.getPartition());
        List<HivePartitionKey> partitionKeys = getPartitionKeys(table, partition.getPartition());
        TupleDomain<HiveColumnHandle> effectivePredicate = (TupleDomain<HiveColumnHandle>) compactEffectivePredicate;

        Path path = new Path(getPartitionLocation(table, partition.getPartition()));
        Configuration configuration = hdfsEnvironment.getConfiguration(hdfsContext, path);
        InputFormat<?, ?> inputFormat = getInputFormat(configuration, schema, false);
        FileSystem fs = hdfsEnvironment.getFileSystem(hdfsContext, path);

        if (inputFormat instanceof SymlinkTextInputFormat) {
            if (bucketSplitInfo.isPresent()) {
                throw new PrestoException(NOT_SUPPORTED, "Bucketed table in SymlinkTextInputFormat is not yet supported");
            }

            // TODO: This should use an iterator like the HiveFileIterator
            ListenableFuture<?> lastResult = COMPLETED_FUTURE;
            for (Path targetPath : getTargetPathsFromSymlink(fs, path)) {
                // The input should be in TextInputFormat.
                TextInputFormat targetInputFormat = new TextInputFormat();
                // the splits must be generated using the file system for the target path
                // get the configuration for the target path -- it may be a different hdfs instance
                FileSystem targetFilesystem = hdfsEnvironment.getFileSystem(hdfsContext, targetPath);
                JobConf targetJob = toJobConf(targetFilesystem.getConf());
                targetJob.setInputFormat(TextInputFormat.class);
                targetInputFormat.configure(targetJob);
                FileInputFormat.setInputPaths(targetJob, targetPath);
                InputSplit[] targetSplits = targetInputFormat.getSplits(targetJob, 0);

                InternalHiveSplitFactory splitFactory = new InternalHiveSplitFactory(targetFilesystem, partitionName, inputFormat, schema, partitionKeys, effectivePredicate, partition.getColumnCoercions(), isForceLocalScheduling(session));
                lastResult = addSplitsToSource(targetSplits, splitFactory);
                if (stopped) {
                    return COMPLETED_FUTURE;
                }
            }
            return lastResult;
        }

        InternalHiveSplitFactory splitFactory = new InternalHiveSplitFactory(fs, partitionName, inputFormat, schema, partitionKeys, effectivePredicate, partition.getColumnCoercions(), isForceLocalScheduling(session));

        // To support custom input formats, we want to call getSplits()
        // on the input format to obtain file splits.
        if (shouldUseFileSplitsFromInputFormat(inputFormat)) {
            JobConf jobConf = toJobConf(configuration);
            FileInputFormat.setInputPaths(jobConf, path);
            InputSplit[] splits = inputFormat.getSplits(jobConf, 0);

            return addSplitsToSource(splits, splitFactory);
        }

        // Bucketed partitions are fully loaded immediately since all files must be loaded to determine the file to bucket mapping
        if (bucketSplitInfo.isPresent()) {
            return hiveSplitSource.addToQueue(getBucketedSplits(path, fs, splitFactory, bucketSplitInfo.get()));
        }

        fileIterators.addLast(new HiveFileIterator(path, fs, directoryLister, namenodeStats, partitionName, splitFactory));
        return COMPLETED_FUTURE;
    }

    private ListenableFuture<?> addSplitsToSource(InputSplit[] targetSplits, InternalHiveSplitFactory splitFactory)
            throws IOException
    {
        ListenableFuture<?> lastResult = COMPLETED_FUTURE;
        for (InputSplit inputSplit : targetSplits) {
            Optional<InternalHiveSplit> internalHiveSplit = splitFactory.createInternalHiveSplit((FileSplit) inputSplit);
            if (internalHiveSplit.isPresent()) {
                lastResult = hiveSplitSource.addToQueue(internalHiveSplit.get());
            }
            if (stopped) {
                return COMPLETED_FUTURE;
            }
        }
        return lastResult;
    }

    private static boolean shouldUseFileSplitsFromInputFormat(InputFormat<?, ?> inputFormat)
    {
        return Arrays.stream(inputFormat.getClass().getAnnotations())
                .map(Annotation::annotationType)
                .map(Class::getSimpleName)
                .anyMatch(name -> name.equals("UseFileSplitsFromInputFormat"));
    }

    private List<InternalHiveSplit> getBucketedSplits(Path path, FileSystem fileSystem, InternalHiveSplitFactory splitFactory, BucketSplitInfo bucketSplitInfo)
    {
        int bucketCount = bucketSplitInfo.getBucketCount();

        // list all files in the partition
        ArrayList<LocatedFileStatus> list = new ArrayList<>(bucketCount);
        HiveFileIterator hiveFileIterator = new HiveFileIterator(path, fileSystem, directoryLister, namenodeStats, splitFactory.getPartitionName(), splitFactory);
        while (hiveFileIterator.hasNext()) {
            LocatedFileStatus next = hiveFileIterator.next();
            if (isDirectory(next)) {
                // Fail here to be on the safe side. This seems to be the same as what Hive does
                throw new PrestoException(HIVE_INVALID_BUCKET_FILES, format("%s Found sub-directory in bucket directory for partition: %s", CORRUPT_BUCKETING, splitFactory.getPartitionName()));
            }
            list.add(next);
        }

        // verify we found one file per bucket
        if (list.size() != bucketCount) {
            throw new PrestoException(HIVE_INVALID_BUCKET_FILES, format("%s The number of files in the directory (%s) does not match the declared bucket count (%s) for partition: %s", CORRUPT_BUCKETING, list.size(),
                    bucketCount,
                    splitFactory.getPartitionName()));
        }

        // Sort FileStatus objects (instead of, e.g., fileStatus.getPath().toString). This matches org.apache.hadoop.hive.ql.metadata.Table.getSortedPaths
        list.sort(null);

        // convert files internal splits
        List<InternalHiveSplit> splitList = new ArrayList<>();
        for (int bucketNumber = 0; bucketNumber < bucketCount; bucketNumber++) {
            if (bucketSplitInfo.isBucketEnabled(bucketNumber)) {
                LocatedFileStatus file = list.get(bucketNumber);
                splitFactory.createInternalHiveSplit(file, bucketNumber)
                        .ifPresent(splitList::add);
            }
        }
        return splitList;
    }

    private static List<Path> getTargetPathsFromSymlink(FileSystem fileSystem, Path symlinkDir)
    {
        try {
            FileStatus[] symlinks = fileSystem.listStatus(symlinkDir, HIDDEN_FILES_PATH_FILTER);
            List<Path> targets = new ArrayList<>();

            for (FileStatus symlink : symlinks) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(symlink.getPath()), StandardCharsets.UTF_8))) {
                    CharStreams.readLines(reader).stream()
                            .map(Path::new)
                            .forEach(targets::add);
                }
            }
            return targets;
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_BAD_DATA, "Error parsing symlinks from: " + symlinkDir, e);
        }
    }

    private static List<HivePartitionKey> getPartitionKeys(Table table, Optional<Partition> partition)
    {
        if (!partition.isPresent()) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<HivePartitionKey> partitionKeys = ImmutableList.builder();
        List<Column> keys = table.getPartitionColumns();
        List<String> values = partition.get().getValues();
        checkCondition(keys.size() == values.size(), HIVE_INVALID_METADATA, "Expected %s partition key values, but got %s", keys.size(), values.size());
        for (int i = 0; i < keys.size(); i++) {
            String name = keys.get(i).getName();
            HiveType hiveType = keys.get(i).getType();
            if (!hiveType.isSupportedType()) {
                throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type %s found in partition keys of table %s.%s", hiveType, table.getDatabaseName(), table.getTableName()));
            }
            String value = values.get(i);
            checkCondition(value != null, HIVE_INVALID_PARTITION_VALUE, "partition key value cannot be null for field: %s", name);
            partitionKeys.add(new HivePartitionKey(name, value));
        }
        return partitionKeys.build();
    }

    private static Properties getPartitionSchema(Table table, Optional<Partition> partition)
    {
        if (!partition.isPresent()) {
            return getHiveSchema(table);
        }
        return getHiveSchema(partition.get(), table);
    }

    private static String getPartitionLocation(Table table, Optional<Partition> partition)
    {
        if (!partition.isPresent()) {
            return table.getStorage().getLocation();
        }
        return partition.get().getStorage().getLocation();
    }

    public static class BucketSplitInfo
    {
        private final int bucketCount;
        private final IntPredicate bucketFilter;

        public static Optional<BucketSplitInfo> createBucketSplitInfo(Optional<HiveBucketHandle> bucketHandle, List<HiveBucket> buckets)
        {
            requireNonNull(bucketHandle, "bucketHandle is null");
            requireNonNull(buckets, "buckets is null");

            if (!buckets.isEmpty()) {
                int bucketCount = buckets.get(0).getBucketCount();
                Set<Integer> bucketNumbers = buckets.stream()
                        .map(HiveBucket::getBucketNumber)
                        .collect(toImmutableSet());
                return Optional.of(new BucketSplitInfo(bucketCount, bucketNumbers::contains));
            }

            return bucketHandle.map(handle -> new BucketSplitInfo(handle.getBucketCount(), bucketNumber -> true));
        }

        private BucketSplitInfo(int bucketCount, IntPredicate bucketFilter)
        {
            this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
            this.bucketFilter = requireNonNull(bucketFilter, "bucketFilter is null");
        }

        public int getBucketCount()
        {
            return bucketCount;
        }

        public boolean isBucketEnabled(int value)
        {
            return bucketFilter.test(value);
        }
    }
}
