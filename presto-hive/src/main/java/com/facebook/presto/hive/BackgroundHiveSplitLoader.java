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
import com.facebook.presto.hive.HiveBucketing.HiveBucketFilter;
import com.facebook.presto.hive.HiveSplit.BucketConversion;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.util.HiveFileIterator.NestedDirectoryNotAllowedException;
import com.facebook.presto.hive.util.InternalHiveSplitFactory;
import com.facebook.presto.hive.util.ResumableTask;
import com.facebook.presto.hive.util.ResumableTasks;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.Domain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
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
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.IntPredicate;

import static com.facebook.presto.hive.HiveBucketing.getVirtualBucketNumber;
import static com.facebook.presto.hive.HiveColumnHandle.pathColumnHandle;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static com.facebook.presto.hive.HiveSessionProperties.isForceLocalScheduling;
import static com.facebook.presto.hive.HiveUtil.checkCondition;
import static com.facebook.presto.hive.HiveUtil.getFooterCount;
import static com.facebook.presto.hive.HiveUtil.getHeaderCount;
import static com.facebook.presto.hive.HiveUtil.getInputFormat;
import static com.facebook.presto.hive.S3SelectPushdown.shouldEnablePushdownForTable;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveSchema;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getPartitionLocation;
import static com.facebook.presto.hive.util.ConfigurationUtils.toJobConf;
import static com.facebook.presto.hive.util.HiveFileIterator.NestedDirectoryPolicy.FAIL;
import static com.facebook.presto.hive.util.HiveFileIterator.NestedDirectoryPolicy.IGNORED;
import static com.facebook.presto.hive.util.HiveFileIterator.NestedDirectoryPolicy.RECURSE;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.common.FileUtils.HIDDEN_FILES_PATH_FILTER;

public class BackgroundHiveSplitLoader
        implements HiveSplitLoader
{
    private static final ListenableFuture<?> COMPLETED_FUTURE = immediateFuture(null);

    private final Table table;
    private final Optional<Domain> pathDomain;
    private final Optional<BucketSplitInfo> tableBucketInfo;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final NamenodeStats namenodeStats;
    private final DirectoryLister directoryLister;
    private final int loaderConcurrency;
    private final boolean recursiveDirWalkerEnabled;
    private final Executor executor;
    private final ConnectorSession session;
    private final ConcurrentLazyQueue<HivePartitionMetadata> partitions;
    private final Deque<Iterator<InternalHiveSplit>> fileIterators = new ConcurrentLinkedDeque<>();
    private final boolean schedulerUsesHostAddresses;

    // Purpose of this lock:
    // * Write lock: when you need a consistent view across partitions, fileIterators, and hiveSplitSource.
    // * Read lock: when you need to modify any of the above.
    //   Make sure the lock is held throughout the period during which they may not be consistent with each other.
    // Details:
    // * When write lock is acquired, except the holder, no one can do any of the following:
    // ** poll from (or check empty) partitions
    // ** poll from (or check empty) or push to fileIterators
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
            Optional<Domain> pathDomain,
            Optional<BucketSplitInfo> tableBucketInfo,
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            NamenodeStats namenodeStats,
            DirectoryLister directoryLister,
            Executor executor,
            int loaderConcurrency,
            boolean recursiveDirWalkerEnabled,
            boolean schedulerUsesHostAddresses)
    {
        this.table = requireNonNull(table, "table is null");
        this.pathDomain = requireNonNull(pathDomain, "pathDomain is null");
        this.tableBucketInfo = requireNonNull(tableBucketInfo, "tableBucketInfo is null");
        this.loaderConcurrency = requireNonNull(loaderConcurrency, "loaderConcurrency is null");
        this.session = requireNonNull(session, "session is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.namenodeStats = requireNonNull(namenodeStats, "namenodeStats is null");
        this.directoryLister = requireNonNull(directoryLister, "directoryLister is null");
        this.recursiveDirWalkerEnabled = recursiveDirWalkerEnabled;
        this.executor = requireNonNull(executor, "executor is null");
        this.partitions = new ConcurrentLazyQueue<>(requireNonNull(partitions, "partitions is null"));
        this.hdfsContext = new HdfsContext(session, table.getDatabaseName(), table.getTableName());
        this.schedulerUsesHostAddresses = schedulerUsesHostAddresses;
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
                    if (e instanceof IOException) {
                        e = new PrestoException(HIVE_FILESYSTEM_ERROR, e);
                    }
                    else if (!(e instanceof PrestoException)) {
                        e = new PrestoException(HIVE_UNKNOWN_ERROR, e);
                    }
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
        taskExecutionLock.readLock().lock();
        try {
            // This is an opportunistic check to avoid getting the write lock unnecessarily
            if (!partitions.isEmpty() || !fileIterators.isEmpty()) {
                return;
            }
        }
        catch (Exception e) {
            hiveSplitSource.fail(e);
            checkState(stopped, "Task is not marked as stopped even though it failed");
            return;
        }
        finally {
            taskExecutionLock.readLock().unlock();
        }

        taskExecutionLock.writeLock().lock();
        try {
            // the write lock guarantees that no one is operating on the partitions, fileIterators, or hiveSplitSource, or half way through doing so.
            if (partitions.isEmpty() && fileIterators.isEmpty()) {
                // It is legal to call `noMoreSplits` multiple times or after `stop` was called.
                // Nothing bad will happen if `noMoreSplits` implementation calls methods that will try to obtain a read lock because the lock is re-entrant.
                hiveSplitSource.noMoreSplits();
            }
        }
        catch (Exception e) {
            hiveSplitSource.fail(e);
            checkState(stopped, "Task is not marked as stopped even though it failed");
        }
        finally {
            taskExecutionLock.writeLock().unlock();
        }
    }

    private ListenableFuture<?> loadSplits()
            throws IOException
    {
        Iterator<InternalHiveSplit> splits = fileIterators.poll();
        if (splits == null) {
            HivePartitionMetadata partition = partitions.poll();
            if (partition == null) {
                return COMPLETED_FUTURE;
            }
            return loadPartition(partition);
        }

        while (splits.hasNext() && !stopped) {
            ListenableFuture<?> future = hiveSplitSource.addToQueue(splits.next());
            if (!future.isDone()) {
                fileIterators.addFirst(splits);
                return future;
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

        Path path = new Path(getPartitionLocation(table, partition.getPartition()));
        Configuration configuration = hdfsEnvironment.getConfiguration(hdfsContext, path);
        InputFormat<?, ?> inputFormat = getInputFormat(configuration, schema, false);
        FileSystem fs = hdfsEnvironment.getFileSystem(hdfsContext, path);
        boolean s3SelectPushdownEnabled = shouldEnablePushdownForTable(session, table, path.toString(), partition.getPartition());

        if (inputFormat instanceof SymlinkTextInputFormat) {
            if (tableBucketInfo.isPresent()) {
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

                InternalHiveSplitFactory splitFactory = new InternalHiveSplitFactory(
                        targetFilesystem,
                        inputFormat,
                        pathDomain,
                        isForceLocalScheduling(session),
                        s3SelectPushdownEnabled,
                        new HiveSplitPartitionInfo(schema, path.toUri(), partitionKeys, partitionName, partition.getColumnCoercions(), Optional.empty()),
                        schedulerUsesHostAddresses);
                lastResult = addSplitsToSource(targetSplits, splitFactory);
                if (stopped) {
                    return COMPLETED_FUTURE;
                }
            }
            return lastResult;
        }

        Optional<BucketConversion> bucketConversion = Optional.empty();
        boolean bucketConversionRequiresWorkerParticipation = false;
        if (partition.getPartition().isPresent()) {
            Optional<HiveBucketProperty> partitionBucketProperty = partition.getPartition().get().getStorage().getBucketProperty();
            if (tableBucketInfo.isPresent() && partitionBucketProperty.isPresent()) {
                int tableBucketCount = tableBucketInfo.get().getTableBucketCount();
                int partitionBucketCount = partitionBucketProperty.get().getBucketCount();
                // Validation was done in HiveSplitManager#getPartitionMetadata.
                // Here, it's just trying to see if its needs the BucketConversion.
                if (tableBucketCount != partitionBucketCount) {
                    bucketConversion = Optional.of(new BucketConversion(tableBucketCount, partitionBucketCount, tableBucketInfo.get().getBucketColumns()));
                    if (tableBucketCount > partitionBucketCount) {
                        bucketConversionRequiresWorkerParticipation = true;
                    }
                }
            }
        }
        InternalHiveSplitFactory splitFactory = new InternalHiveSplitFactory(
                fs,
                inputFormat,
                pathDomain,
                isForceLocalScheduling(session),
                s3SelectPushdownEnabled,
                new HiveSplitPartitionInfo(
                        schema,
                        path.toUri(),
                        partitionKeys,
                        partitionName,
                        partition.getColumnCoercions(),
                        bucketConversionRequiresWorkerParticipation ? bucketConversion : Optional.empty()),
                schedulerUsesHostAddresses);

        // To support custom input formats, we want to call getSplits()
        // on the input format to obtain file splits.
        if (shouldUseFileSplitsFromInputFormat(inputFormat)) {
            if (tableBucketInfo.isPresent()) {
                throw new PrestoException(NOT_SUPPORTED, "Presto cannot read bucketed partition in an input format with UseFileSplitsFromInputFormat annotation: " + inputFormat.getClass().getSimpleName());
            }
            JobConf jobConf = toJobConf(configuration);
            FileInputFormat.setInputPaths(jobConf, path);
            InputSplit[] splits = inputFormat.getSplits(jobConf, 0);

            return addSplitsToSource(splits, splitFactory);
        }

        // S3 Select pushdown works at the granularity of individual S3 objects,
        // therefore we must not split files when it is enabled.
        boolean splittable = getHeaderCount(schema) == 0 && getFooterCount(schema) == 0 && !s3SelectPushdownEnabled;

        // Bucketed partitions are fully loaded immediately since all files must be loaded to determine the file to bucket mapping
        if (tableBucketInfo.isPresent()) {
            if (tableBucketInfo.get().isVirtuallyBucketed()) {
                // For virtual bucket, bucket conversion must not be present because there is no physical partition bucket count
                checkState(!bucketConversion.isPresent(), "Virtually bucketed table must not have partitions that are physically bucketed");
                checkState(
                        tableBucketInfo.get().getTableBucketCount() == tableBucketInfo.get().getReadBucketCount(),
                        "Table and read bucket count should be the same for virtual bucket");
                return hiveSplitSource.addToQueue(getVirtuallyBucketedSplits(path, fs, splitFactory, tableBucketInfo.get().getReadBucketCount(), splittable));
            }
            return hiveSplitSource.addToQueue(getBucketedSplits(path, fs, splitFactory, tableBucketInfo.get(), bucketConversion, partitionName, splittable));
        }

        fileIterators.addLast(createInternalHiveSplitIterator(path, fs, splitFactory, splittable));
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

    private Iterator<InternalHiveSplit> createInternalHiveSplitIterator(Path path, FileSystem fileSystem, InternalHiveSplitFactory splitFactory, boolean splittable)
    {
        return stream(directoryLister.list(fileSystem, path, namenodeStats, recursiveDirWalkerEnabled ? RECURSE : IGNORED))
                .map(status -> splitFactory.createInternalHiveSplit(status, splittable))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .iterator();
    }

    private List<InternalHiveSplit> getBucketedSplits(
            Path path,
            FileSystem fileSystem,
            InternalHiveSplitFactory splitFactory,
            BucketSplitInfo bucketSplitInfo,
            Optional<BucketConversion> bucketConversion,
            String partitionName,
            boolean splittable)
    {
        int readBucketCount = bucketSplitInfo.getReadBucketCount();
        int tableBucketCount = bucketSplitInfo.getTableBucketCount();
        int partitionBucketCount = bucketConversion.isPresent() ? bucketConversion.get().getPartitionBucketCount() : tableBucketCount;

        checkState(readBucketCount <= tableBucketCount, "readBucketCount(%s) should be less than or equal to tableBucketCount(%s)", readBucketCount, tableBucketCount);

        // list all files in the partition
        ArrayList<LocatedFileStatus> files = new ArrayList<>(partitionBucketCount);
        try {
            Iterators.addAll(files, directoryLister.list(fileSystem, path, namenodeStats, FAIL));
        }
        catch (NestedDirectoryNotAllowedException e) {
            // Fail here to be on the safe side. This seems to be the same as what Hive does
            throw new PrestoException(
                    HIVE_INVALID_BUCKET_FILES,
                    format("Hive table '%s' is corrupt. Found sub-directory in bucket directory for partition: %s",
                            new SchemaTableName(table.getDatabaseName(), table.getTableName()),
                            partitionName));
        }

        // verify we found one file per bucket
        if (files.size() != partitionBucketCount) {
            throw new PrestoException(
                    HIVE_INVALID_BUCKET_FILES,
                    format("Hive table '%s' is corrupt. The number of files in the directory (%s) does not match the declared bucket count (%s) for partition: %s",
                            new SchemaTableName(table.getDatabaseName(), table.getTableName()),
                            files.size(),
                            partitionBucketCount,
                            partitionName));
        }

        // Sort FileStatus objects (instead of, e.g., fileStatus.getPath().toString). This matches org.apache.hadoop.hive.ql.metadata.Table.getSortedPaths
        files.sort(null);

        // convert files internal splits
        List<InternalHiveSplit> splitList = new ArrayList<>();
        for (int bucketNumber = 0; bucketNumber < max(readBucketCount, partitionBucketCount); bucketNumber++) {
            // Physical bucket #. This determine file name. It also determines the order of splits in the result.
            int partitionBucketNumber = bucketNumber % partitionBucketCount;
            // Logical bucket #. Each logical bucket corresponds to a "bucket" from engine's perspective.
            int readBucketNumber = bucketNumber % readBucketCount;

            boolean containsIneligibleTableBucket = false;
            List<Integer> eligibleTableBucketNumbers = new ArrayList<>();
            for (int tableBucketNumber = bucketNumber % tableBucketCount; tableBucketNumber < tableBucketCount; tableBucketNumber += max(readBucketCount, partitionBucketCount)) {
                // table bucket number: this is used for evaluating "$bucket" filters.
                if (bucketSplitInfo.isTableBucketEnabled(tableBucketNumber)) {
                    eligibleTableBucketNumbers.add(tableBucketNumber);
                }
                else {
                    containsIneligibleTableBucket = true;
                }
            }

            if (!eligibleTableBucketNumbers.isEmpty() && containsIneligibleTableBucket) {
                throw new PrestoException(
                        NOT_SUPPORTED,
                        "The bucket filter cannot be satisfied. There are restrictions on the bucket filter when all the following is true: " +
                                "1. a table has a different buckets count as at least one of its partitions that is read in this query; " +
                                "2. the table has a different but compatible bucket number with another table in the query; " +
                                "3. some buckets of the table is filtered out from the query, most likely using a filter on \"$bucket\". " +
                                "(table name: " + table.getTableName() + ", table bucket count: " + tableBucketCount + ", " +
                                "partition bucket count: " + partitionBucketCount + ", effective reading bucket count: " + readBucketCount + ")");
            }
            if (!eligibleTableBucketNumbers.isEmpty()) {
                LocatedFileStatus file = files.get(partitionBucketNumber);
                eligibleTableBucketNumbers.stream()
                        .map(tableBucketNumber -> splitFactory.createInternalHiveSplit(file, readBucketNumber, tableBucketNumber, splittable))
                        .forEach(optionalSplit -> optionalSplit.ifPresent(splitList::add));
            }
        }
        return splitList;
    }

    private List<InternalHiveSplit> getVirtuallyBucketedSplits(Path path, FileSystem fileSystem, InternalHiveSplitFactory splitFactory, int bucketCount, boolean splittable)
    {
        // List all files recursively in the partition and assign virtual bucket number to each of them
        return stream(directoryLister.list(fileSystem, path, namenodeStats, RECURSE))
                .map(file -> {
                    int virtualBucketNumber = getVirtualBucketNumber(bucketCount, file.getPath());
                    return splitFactory.createInternalHiveSplit(file, virtualBucketNumber, virtualBucketNumber, splittable);
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
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

    public static class BucketSplitInfo
    {
        private final List<HiveColumnHandle> bucketColumns;
        private final int tableBucketCount;
        private final int readBucketCount;
        private final IntPredicate bucketFilter;

        public static Optional<BucketSplitInfo> createBucketSplitInfo(Optional<HiveBucketHandle> bucketHandle, Optional<HiveBucketFilter> bucketFilter)
        {
            requireNonNull(bucketHandle, "bucketHandle is null");
            requireNonNull(bucketFilter, "buckets is null");

            if (!bucketHandle.isPresent()) {
                checkArgument(!bucketFilter.isPresent(), "bucketHandle must be present if bucketFilter is present");
                return Optional.empty();
            }

            int tableBucketCount = bucketHandle.get().getTableBucketCount();
            int readBucketCount = bucketHandle.get().getReadBucketCount();

            List<HiveColumnHandle> bucketColumns = bucketHandle.get().getColumns();
            IntPredicate predicate = bucketFilter
                    .<IntPredicate>map(filter -> filter.getBucketsToKeep()::contains)
                    .orElse(bucket -> true);
            return Optional.of(new BucketSplitInfo(bucketColumns, tableBucketCount, readBucketCount, predicate));
        }

        private BucketSplitInfo(List<HiveColumnHandle> bucketColumns, int tableBucketCount, int readBucketCount, IntPredicate bucketFilter)
        {
            this.bucketColumns = ImmutableList.copyOf(requireNonNull(bucketColumns, "bucketColumns is null"));
            this.tableBucketCount = tableBucketCount;
            this.readBucketCount = readBucketCount;
            this.bucketFilter = requireNonNull(bucketFilter, "bucketFilter is null");
        }

        public List<HiveColumnHandle> getBucketColumns()
        {
            return bucketColumns;
        }

        public int getTableBucketCount()
        {
            return tableBucketCount;
        }

        public int getReadBucketCount()
        {
            return readBucketCount;
        }

        public boolean isVirtuallyBucketed()
        {
            return bucketColumns.size() == 1 && bucketColumns.get(0).equals(pathColumnHandle());
        }

        /**
         * Evaluates whether the provided table bucket number passes the bucket predicate.
         * A bucket predicate can be present in two cases:
         * <ul>
         * <li>Filter on "$bucket" column. e.g. {@code "$bucket" between 0 and 100}
         * <li>Single-value equality filter on all bucket columns. e.g. for a table with two bucketing columns,
         * {@code bucketCol1 = 'a' AND bucketCol2 = 123}
         * </ul>
         */
        public boolean isTableBucketEnabled(int tableBucketNumber)
        {
            return bucketFilter.test(tableBucketNumber);
        }
    }
}
