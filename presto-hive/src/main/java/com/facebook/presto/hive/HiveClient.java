package com.facebook.presto.hive;

import com.facebook.presto.hive.util.AsyncRecursiveWalker;
import com.facebook.presto.hive.util.BoundedExecutor;
import com.facebook.presto.hive.util.FileStatusCallback;
import com.facebook.presto.hive.util.SuspendingExecutor;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat.SymlinkTextInputSplit;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.hive.HiveColumnHandle.columnMetadataGetter;
import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;
import static com.facebook.presto.hive.HivePartitionChunk.makeLastChunk;
import static com.facebook.presto.hive.HiveType.getHiveType;
import static com.facebook.presto.hive.HiveUtil.convertNativeHiveType;
import static com.facebook.presto.hive.HiveUtil.getInputFormat;
import static com.facebook.presto.hive.UnpartitionedPartition.UNPARTITIONED_PARTITION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.partition;
import static com.google.common.collect.Iterables.transform;

@SuppressWarnings("deprecation")
public class HiveClient
        implements ImportClient
{
    public static final String HIVE_VIEWS_NOT_SUPPORTED = "Hive views are not supported";

    private static final Logger log = Logger.get(HiveClient.class);

    private final long maxChunkBytes;
    private final int maxOutstandingChunks;
    private final int maxChunkIteratorThreads;
    private final int partitionBatchSize;
    private final HiveChunkEncoder hiveChunkEncoder;
    private final HiveChunkReader hiveChunkReader;
    private final CachingHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final ExecutorService executor;

    public HiveClient(
            long maxChunkBytes,
            int maxOutstandingChunks,
            int maxChunkIteratorThreads,
            int partitionBatchSize,
            HiveChunkEncoder hiveChunkEncoder,
            HiveChunkReader hiveChunkReader,
            CachingHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            ExecutorService executor)
    {
        this.maxChunkBytes = maxChunkBytes;
        this.maxOutstandingChunks = maxOutstandingChunks;
        this.maxChunkIteratorThreads = maxChunkIteratorThreads;
        this.partitionBatchSize = partitionBatchSize;
        this.hiveChunkEncoder = hiveChunkEncoder;
        this.hiveChunkReader = hiveChunkReader;
        this.metastore = metastore;
        this.hdfsEnvironment = hdfsEnvironment;
        this.executor = executor;
    }

    @Override
    public List<String> listSchemaNames()
    {
        return metastore.getAllDatabases();
    }

    @Override
    public TableHandle getTableHandle(SchemaTableName tableName)
    {
        try {
            metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            return new HiveTableHandle(tableName);
        }
        catch (NoSuchObjectException e) {
            // table was not found
            return null;
        }
    }

    @Override
    public SchemaTableName getTableName(TableHandle tableHandle)
    {
        checkArgument(tableHandle instanceof HiveTableHandle, "tableHandle is not an instance of HiveTableHandle");
        return ((HiveTableHandle) tableHandle).getTableName();
    }

    @Override
    public SchemaTableMetadata getTableMetadata(TableHandle tableHandle)
    {
        SchemaTableName tableName = getTableName(tableHandle);
        return getTableMetadata(tableName);
    }

    private SchemaTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        try {
            Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            List<ColumnMetadata> columns = ImmutableList.copyOf(transform(getColumnHandles(table), columnMetadataGetter()));

            ImmutableList.Builder<String> keys = ImmutableList.builder();
            for (FieldSchema field : table.getPartitionKeys()) {
                keys.add(field.getName());
            }

            return new SchemaTableMetadata(tableName, columns, keys.build());
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }
    }

    @Override
    public List<SchemaTableName> listTables(String schemaNameOrNull)
    {
        List<String> schemaNames;
        if (schemaNameOrNull == null) {
            schemaNames = listSchemaNames();
        }
        else {
            schemaNames = Collections.singletonList(schemaNameOrNull);
        }

        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            try {
                for (String tableName : metastore.getAllTables(schemaName)) {
                    tableNames.add(new SchemaTableName(schemaName, tableName));
                }
            }
            catch (NoSuchObjectException e) {
                throw new SchemaNotFoundException(schemaName);
            }
        }
        return tableNames.build();
    }

    private List<SchemaTableName> listTables(SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tableNames;
        if (prefix.getSchemaName() == null) {
            tableNames = listTables(prefix.getSchemaName());
        } else {
            tableNames = Collections.singletonList(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        return tableNames;
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
    {
        return getColumnHandles(tableHandle).get(columnName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        SchemaTableName tableName = getTableName(tableHandle);
        try {
            Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
            for (HiveColumnHandle columnHandle : getColumnHandles(table)) {
                columnHandles.put(columnHandle.getName(), columnHandle);
            }
            return columnHandles.build();
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }
    }

    private List<HiveColumnHandle> getColumnHandles(Table table)
    {
        try {
            Deserializer deserializer = MetaStoreUtils.getDeserializer(null, MetaStoreUtils.getSchema(table));
            ObjectInspector inspector = deserializer.getObjectInspector();
            checkArgument(inspector.getCategory() == ObjectInspector.Category.STRUCT, "expected STRUCT: %s", inspector.getCategory());
            StructObjectInspector structObjectInspector = (StructObjectInspector) inspector;

            ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();

            // add the partition keys
            List<FieldSchema> partitionKeys = table.getPartitionKeys();
            for (int i = 0; i < partitionKeys.size(); i++) {
                FieldSchema field = partitionKeys.get(i);

                HiveType hiveType = getHiveType(field.getType());
                columns.add(new HiveColumnHandle(field.getName(), i, hiveType, -1, true));
            }

            // add the data fields
            int hiveColumnIndex = 0;
            for (StructField field : structObjectInspector.getAllStructFieldRefs()) {
                // ignore unsupported types rather than failing
                HiveType hiveType = getHiveType(field.getFieldObjectInspector());
                if (hiveType != null) {
                    columns.add(new HiveColumnHandle(field.getFieldName(), partitionKeys.size() + hiveColumnIndex, hiveType, hiveColumnIndex, false));
                }
                hiveColumnIndex++;
            }
            return columns.build();
        }
        catch (MetaException | SerDeException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(prefix)) {
            columns.put(tableName, getTableMetadata(tableName).getColumns());
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkArgument(tableHandle instanceof HiveTableHandle, "tableHandle is not an instance of HiveTableHandle");
        checkArgument(columnHandle instanceof HiveColumnHandle, "columnHandle is not an instance of HiveColumnHandle");
        return ((HiveColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public List<Partition> getPartitions(TableHandle tableHandle, Map<ColumnHandle, Object> bindings)
    {
        SchemaTableName tableName = getTableName(tableHandle);

        List<FieldSchema> partitionKeys;
        try {
            partitionKeys = metastore.getTable(tableName.getSchemaName(), tableName.getTableName()).getPartitionKeys();
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }

        LinkedHashMap<String, ColumnHandle> partitionKeysByName = new LinkedHashMap<>();
        List<String> filterPrefix = new ArrayList<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            FieldSchema field = partitionKeys.get(i);

            HiveColumnHandle columnHandle = new HiveColumnHandle(field.getName(), i, getHiveType(field.getType()), -1, true);
            partitionKeysByName.put(field.getName(), columnHandle);

            // only add to prefix if all previous keys have a value
            if (filterPrefix.size() == i) {
                Object value = bindings.get(columnHandle);
                if (value != null) {
                    Preconditions.checkArgument(value instanceof String || value instanceof Double || value instanceof Long,
                            "Only String, Double and Long partition keys are supported");
                    filterPrefix.add(value.toString());
                }
            }
        }

        // fetch the partition names
        List<String> partitionNames;
        try {
            if (filterPrefix.isEmpty()) {
                partitionNames = metastore.getPartitionNames(tableName.getSchemaName(), tableName.getTableName());
            }
            else {
                partitionNames = metastore.getPartitionNamesByParts(tableName.getSchemaName(), tableName.getTableName(), filterPrefix);
            }
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }

        // do a final pass to filter based on fields that could not be used to build the prefix
        Iterable<Partition> partitions = transform(partitionNames, toPartition(tableName, partitionKeysByName));
        return ImmutableList.copyOf(Iterables.filter(partitions, partitionMatches(bindings)));
    }

    @Override
    public Iterable<PartitionChunk> getPartitionChunks(List<Partition> partitions, List<ColumnHandle> columns)
    {
        checkNotNull(partitions, "partitions is null");
        checkNotNull(columns, "columns is null");

        Partition partition = Iterables.getFirst(partitions, null);
        if (partition == null) {
            return ImmutableList.of();
        }
        checkArgument(partition instanceof HivePartition, "Partition must be a hive partition");
        SchemaTableName tableName = ((HivePartition) partition).getTableName();

        List<String> partitionNames = Lists.transform(partitions, new Function<Partition, String>()
        {
            @Override
            public String apply(Partition input)
            {
                return input.getPartitionId();
            }
        });

        Table table;
        Iterable<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions;
        try {
            table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            hivePartitions = getPartitions(tableName, partitionNames);
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }

        ImmutableList.Builder<HiveColumnHandle> hiveColumns = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            hiveColumns.add((HiveColumnHandle) column);
        }
        return getPartitionChunks(table, ImmutableList.copyOf(partitionNames), hivePartitions, hiveColumns.build());
    }

    private Iterable<org.apache.hadoop.hive.metastore.api.Partition> getPartitions(final SchemaTableName tableName, final List<String> partitionNames)
            throws NoSuchObjectException
    {
        if (partitionNames.equals(ImmutableList.of(UNPARTITIONED_ID))) {
            return ImmutableList.of(UNPARTITIONED_PARTITION);
        }

        Iterable<List<String>> partitionNameBatches = partitionExponentially(partitionNames, partitionBatchSize);
        Iterable<List<org.apache.hadoop.hive.metastore.api.Partition>> partitionBatches = transform(partitionNameBatches, new Function<List<String>, List<org.apache.hadoop.hive.metastore.api.Partition>>()
        {
            @Override
            public List<org.apache.hadoop.hive.metastore.api.Partition> apply(List<String> partitionNameBatch)
            {
                Exception exception = null;
                for (int attempt = 0; attempt < 10; attempt++) {
                    try {
                        List<org.apache.hadoop.hive.metastore.api.Partition> partitions = metastore.getPartitionsByNames(tableName.getSchemaName(), tableName.getTableName(), partitionNameBatch);
                        checkState(partitionNameBatch.size() == partitions.size(), "expected %s partitions but found %s", partitionNameBatch.size(), partitions.size());
                        return partitions;
                    }
                    catch (NoSuchObjectException | NullPointerException | IllegalStateException | IllegalArgumentException e) {
                        throw Throwables.propagate(e);
                    }
                    catch (Exception e) {
                        exception = e;
                        log.debug("getPartitions attempt %s failed, will retry. Exception: %s", attempt, e.getMessage());
                    }

                    try {
                        TimeUnit.SECONDS.sleep(1);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw Throwables.propagate(e);
                    }
                }
                throw Throwables.propagate(exception);
            }
        });
        return concat(partitionBatches);
    }

    @Override
    public RecordCursor getRecords(PartitionChunk partitionChunk)
    {
        checkArgument(partitionChunk instanceof HivePartitionChunk,
                "expected instance of %s: %s", HivePartitionChunk.class, partitionChunk.getClass());
        assert partitionChunk instanceof HivePartitionChunk; // IDEA-60343
        return hiveChunkReader.getRecords((HivePartitionChunk) partitionChunk);
    }

    @Override
    public byte[] serializePartitionChunk(PartitionChunk partitionChunk)
    {
        checkArgument(partitionChunk instanceof HivePartitionChunk,
                "expected instance of %s: %s", HivePartitionChunk.class, partitionChunk.getClass());
        assert partitionChunk instanceof HivePartitionChunk; // IDEA-60343
        return hiveChunkEncoder.serialize((HivePartitionChunk) partitionChunk);
    }

    @Override
    public PartitionChunk deserializePartitionChunk(byte[] bytes)
    {
        return hiveChunkEncoder.deserialize(bytes);
    }

    private Iterable<PartitionChunk> getPartitionChunks(Table table, Iterable<String> partitionNames, Iterable<org.apache.hadoop.hive.metastore.api.Partition> partitions, List<HiveColumnHandle> columns)
    {
        return new PartitionChunkIterable(table, partitionNames, partitions, columns, maxChunkBytes, maxOutstandingChunks, maxChunkIteratorThreads, hdfsEnvironment, executor, partitionBatchSize);
    }

    private static class PartitionChunkIterable
            implements Iterable<PartitionChunk>
    {
        private static final PartitionChunk FINISHED_MARKER = new PartitionChunk()
        {
            @Override
            public String getPartitionName()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getLength()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<InetAddress> getHosts()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object getInfo()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isLastChunk()
            {
                throw new UnsupportedOperationException();
            }
        };

        private final Table table;
        private final Iterable<String> partitionNames;
        private final Iterable<org.apache.hadoop.hive.metastore.api.Partition> partitions;
        private final List<HiveColumnHandle> columns;
        private final long maxChunkBytes;
        private final int maxOutstandingChunks;
        private final int maxThreads;
        private final HdfsEnvironment hdfsEnvironment;
        private final ExecutorService executor;
        private final ClassLoader classLoader;
        private final int partitionBatchSize;

        private PartitionChunkIterable(Table table,
                Iterable<String> partitionNames,
                Iterable<org.apache.hadoop.hive.metastore.api.Partition> partitions,
                List<HiveColumnHandle> columns,
                long maxChunkBytes,
                int maxOutstandingChunks,
                int maxThreads,
                HdfsEnvironment hdfsEnvironment,
                ExecutorService executor,
                int partitionBatchSize)
        {
            this.table = table;
            this.partitionNames = partitionNames;
            this.partitions = partitions;
            this.partitionBatchSize = partitionBatchSize;
            this.columns = ImmutableList.copyOf(columns);
            this.maxChunkBytes = maxChunkBytes;
            this.maxOutstandingChunks = maxOutstandingChunks;
            this.maxThreads = maxThreads;
            this.hdfsEnvironment = hdfsEnvironment;
            this.executor = executor;
            this.classLoader = Thread.currentThread().getContextClassLoader();
        }

        @Override
        public Iterator<PartitionChunk> iterator()
        {
            // Each iterator has its own bounded executor and can be independently suspended
            final SuspendingExecutor suspendingExecutor = new SuspendingExecutor(new BoundedExecutor(executor, maxThreads));
            final PartitionChunkQueue partitionChunkQueue = new PartitionChunkQueue(maxOutstandingChunks, suspendingExecutor);
            executor.submit(new Callable<Void>()
            {
                @Override
                public Void call()
                        throws Exception
                {
                    loadPartitionChunks(partitionChunkQueue, suspendingExecutor);
                    return null;
                }
            });
            return partitionChunkQueue;
        }

        private void loadPartitionChunks(final PartitionChunkQueue partitionChunkQueue, final SuspendingExecutor suspendingExecutor)
                throws InterruptedException
        {
            final Semaphore semaphore = new Semaphore(partitionBatchSize);
            try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
                ImmutableList.Builder<ListenableFuture<Void>> futureBuilder = ImmutableList.builder();

                Iterator<String> nameIterator = partitionNames.iterator();
                for (org.apache.hadoop.hive.metastore.api.Partition partition : partitions) {
                    checkState(nameIterator.hasNext(), "different number of partitions and partition names!");
                    semaphore.acquire();
                    final String partitionName = nameIterator.next();
                    final Properties schema = getPartitionSchema(table, partition);
                    final List<HivePartitionKey> partitionKeys = getPartitionKeys(table, partition);
                    final InputFormat<?, ?> inputFormat = getInputFormat(hdfsEnvironment.getConfiguration(), schema, false);
                    Path partitionPath = hdfsEnvironment.getFileSystemWrapper().wrap(new Path(getPartitionLocation(table, partition)));

                    final FileSystem fs = partitionPath.getFileSystem(hdfsEnvironment.getConfiguration());
                    final PartitionChunkPoisoner chunkPoisoner = new PartitionChunkPoisoner(partitionChunkQueue);

                    if (inputFormat instanceof SymlinkTextInputFormat) {
                        JobConf jobConf = new JobConf(hdfsEnvironment.getConfiguration());
                        FileInputFormat.setInputPaths(jobConf, partitionPath);
                        InputSplit[] splits = inputFormat.getSplits(jobConf, 0);
                        for (InputSplit rawSplit : splits) {
                            FileSplit split = ((SymlinkTextInputSplit) rawSplit).getTargetSplit();

                            // get the filesystem for the target path -- it may be a different hdfs instance
                            FileSystem targetFilesystem = split.getPath().getFileSystem(hdfsEnvironment.getConfiguration());
                            chunkPoisoner.writeChunks(createHivePartitionChunks(partitionName, targetFilesystem.getFileStatus(split.getPath()),
                                    split.getStart(),
                                    split.getLength(),
                                    schema,
                                    partitionKeys,
                                    targetFilesystem,
                                    false));
                        }
                        chunkPoisoner.finish();
                        continue;
                    }

                    ListenableFuture<Void> partitionFuture = new AsyncRecursiveWalker(fs, suspendingExecutor).beginWalk(partitionPath, new FileStatusCallback()
                    {
                        @Override
                        public void process(FileStatus file)
                        {
                            try {
                                boolean splittable = isSplittable(inputFormat,
                                        file.getPath().getFileSystem(hdfsEnvironment.getConfiguration()),
                                        file.getPath());

                                chunkPoisoner.writeChunks(createHivePartitionChunks(partitionName, file, 0, file.getLen(), schema, partitionKeys, fs, splittable));
                            }
                            catch (IOException e) {
                                partitionChunkQueue.fail(e);
                            }
                        }
                    });

                    // release the semaphore when the partition finishes
                    Futures.addCallback(partitionFuture, new FutureCallback<Void>()
                    {
                        @Override
                        public void onSuccess(Void result)
                        {
                            chunkPoisoner.finish();
                            semaphore.release();
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                            chunkPoisoner.finish();
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
                        partitionChunkQueue.finished();
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        partitionChunkQueue.fail(t);
                    }
                });
            }
            catch (Throwable e) {
                partitionChunkQueue.fail(e);
                Throwables.propagateIfInstanceOf(e, Error.class);
            }
        }

        private List<HivePartitionChunk> createHivePartitionChunks(
                String partitionName,
                FileStatus file,
                long start,
                long length,
                Properties schema,
                List<HivePartitionKey> partitionKeys,
                FileSystem fs,
                boolean splittable)
                throws IOException
        {
            BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(file, start, length);

            ImmutableList.Builder<HivePartitionChunk> builder = ImmutableList.builder();
            if (splittable) {
                for (BlockLocation blockLocation : fileBlockLocations) {
                    builder.add(new HivePartitionChunk(partitionName,
                            false,
                            file.getPath(),
                            blockLocation.getOffset(),
                            blockLocation.getLength(),
                            schema,
                            partitionKeys,
                            columns,
                            toInetAddress(blockLocation.getHosts())));
                }
            } else {
                // not splittable, use the hosts from the first block
                builder.add(new HivePartitionChunk(
                        partitionName,
                        false,
                        file.getPath(),
                        start,
                        length,
                        schema,
                        partitionKeys,
                        columns,
                        toInetAddress(fileBlockLocations[0].getHosts())));
            }
            return builder.build();
        }

        private List<InetAddress> toInetAddress(String[] hosts)
        {
            ImmutableList.Builder<InetAddress> builder = ImmutableList.builder();
            for (String host : hosts) {
                try {
                    builder.add(InetAddress.getByName(host));
                }
                catch (UnknownHostException e) {
                }
            }
            return builder.build();
        }

        /**
         * Holds onto the last seen partition chunk for a given partition and
         * will "poison" it with an end marker when a partition is finished.
         */
        @ThreadSafe
        private static class PartitionChunkPoisoner
        {
            private final PartitionChunkQueue partitionChunkQueue;

            private final AtomicReference<HivePartitionChunk> chunkHolder = new AtomicReference<>();
            private final AtomicBoolean done = new AtomicBoolean();

            private PartitionChunkPoisoner(PartitionChunkQueue partitionChunkQueue)
            {
                this.partitionChunkQueue = checkNotNull(partitionChunkQueue, "partitionChunkQueue is null");
            }

            public synchronized void writeChunks(Iterable<HivePartitionChunk> chunks)
            {
                checkNotNull(chunks, "chunks is null");
                checkState(!done.get(), "already done");

                for (HivePartitionChunk chunk : chunks) {
                    HivePartitionChunk previousChunk = chunkHolder.getAndSet(chunk);
                    if (previousChunk != null) {
                        partitionChunkQueue.addToQueue(previousChunk);
                    }
                }
            }

            private synchronized void finish()
            {
                checkState(!done.getAndSet(true), "already done");
                HivePartitionChunk finalChunk = chunkHolder.getAndSet(null);
                if (finalChunk != null) {
                    partitionChunkQueue.addToQueue(makeLastChunk(finalChunk));
                }
            }
        }

        private static class PartitionChunkQueue
                extends AbstractIterator<PartitionChunk>
        {
            private final BlockingQueue<PartitionChunk> queue = new LinkedBlockingQueue<>();
            private final AtomicInteger outstandingChunkCount = new AtomicInteger();
            private final AtomicReference<Throwable> throwable = new AtomicReference<>();
            private final int maxOutstandingChunks;
            private final SuspendingExecutor suspendingExecutor;

            private PartitionChunkQueue(int maxOutstandingChunks, SuspendingExecutor suspendingExecutor)
            {
                this.maxOutstandingChunks = maxOutstandingChunks;
                this.suspendingExecutor = suspendingExecutor;
            }

            private void addToQueue(PartitionChunk chunk)
            {
                queue.add(chunk);
                if (outstandingChunkCount.incrementAndGet() == maxOutstandingChunks) {
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
            protected PartitionChunk computeNext()
            {
                try {
                    PartitionChunk chunk = queue.take();
                    if (chunk == FINISHED_MARKER) {
                        if (throwable.get() != null) {
                            throw Throwables.propagate(throwable.get());
                        }
                        return endOfData();
                    }
                    if (outstandingChunkCount.getAndDecrement() == maxOutstandingChunks) {
                        suspendingExecutor.resume();
                    }
                    return chunk;
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    private static boolean isSplittable(InputFormat<?,?> inputFormat, FileSystem fileSystem, Path path)
    {
        // use reflection to get isSplittable method on InputFormat
        Method method = null;
        for (Class<?> clazz = inputFormat.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
            try {
                method = clazz.getDeclaredMethod("isSplitable", FileSystem.class, Path.class);
                break;
            }
            catch (NoSuchMethodException e) {
            }
        }

        if (method == null) {
            return false;
        }
        try {
            method.setAccessible(true);
            return (boolean) method.invoke(inputFormat, fileSystem, path);
        }
        catch (InvocationTargetException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    private static Function<String, Partition> toPartition(final SchemaTableName tableName, final Map<String, ColumnHandle> columnsByName)
    {
        return new Function<String, Partition>()
        {
            @Override
            public Partition apply(String partitionId)
            {
                try {
                    if (partitionId.equals(UNPARTITIONED_ID)) {
                        return new HivePartition(tableName);
                    }

                    LinkedHashMap<String, String> keys = Warehouse.makeSpecFromName(partitionId);
                    ImmutableMap.Builder<ColumnHandle, String> builder = ImmutableMap.builder();
                    for (Entry<String, String> entry : keys.entrySet()) {
                        ColumnHandle columnHandle = columnsByName.get(entry.getKey());
                        checkArgument(columnHandle != null, "Invalid partition key %s in partition %s", entry.getKey(), partitionId);
                        builder.put(columnHandle, entry.getValue());
                    }

                    return new HivePartition(tableName, partitionId, builder.build());
                }
                catch (MetaException e) {
                    // invalid partition id
                    throw Throwables.propagate(e);
                }
            }
        };
    }

    public static Predicate<Partition> partitionMatches(final Map<ColumnHandle, Object> filters)
    {
        return new Predicate<Partition>()
        {
            @Override
            public boolean apply(Partition partition)
            {
                for (Map.Entry<ColumnHandle, String> entry : partition.getKeys().entrySet()) {
                    Object filterValue = filters.get(entry.getKey());
                    if (filterValue != null && !entry.getValue().equals(filterValue)) {
                        return false;
                    }
                }
                return true;
            }
        };
    }

    private static List<HivePartitionKey> getPartitionKeys(Table table, org.apache.hadoop.hive.metastore.api.Partition partition)
    {
        if (partition == UNPARTITIONED_PARTITION) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<HivePartitionKey> partitionKeys = ImmutableList.builder();
        List<FieldSchema> keys = table.getPartitionKeys();
        List<String> values = partition.getValues();
        checkArgument(keys.size() == values.size(), "Expected %s partition key values, but got %s", keys.size(), values.size());
        for (int i = 0; i < keys.size(); i++) {
            String name = keys.get(i).getName();
            PrimitiveCategory primitiveCategory = convertNativeHiveType(keys.get(i).getType());
            HiveType hiveType = HiveType.getSupportedHiveType(primitiveCategory);
            String value = values.get(i);
            checkNotNull(value, "partition key value cannot be null for field: %s", name);
            partitionKeys.add(new HivePartitionKey(name, hiveType, value));
        }
        return partitionKeys.build();
    }

    private static Properties getPartitionSchema(Table table, org.apache.hadoop.hive.metastore.api.Partition partition)
    {
        if (partition == UNPARTITIONED_PARTITION) {
            return MetaStoreUtils.getSchema(table);
        }
        return MetaStoreUtils.getSchema(partition, table);
    }

    private static String getPartitionLocation(Table table, org.apache.hadoop.hive.metastore.api.Partition partition)
    {
        if (partition == UNPARTITIONED_PARTITION) {
            return table.getSd().getLocation();
        }
        return partition.getSd().getLocation();
    }

    /**
     * Partition the given list in exponentially (power of 2) increasing batch sizes starting at 1 up to maxBatchSize
     */
    private static <T> Iterable<List<T>> partitionExponentially(final List<T> values, final int maxBatchSize)
    {
        return new Iterable<List<T>>()
        {
            @Override
            public Iterator<List<T>> iterator()
            {
                return new AbstractIterator<List<T>>()
                {
                    private int currentSize = 1;
                    private final Iterator<T> iterator = values.iterator();

                    @Override
                    protected List<T> computeNext()
                    {
                        if (!iterator.hasNext()) {
                            return endOfData();
                        }

                        int count = 0;
                        ImmutableList.Builder<T> builder = ImmutableList.builder();
                        while (iterator.hasNext() && count < currentSize) {
                            builder.add(iterator.next());
                            ++count;
                        }

                        currentSize = Math.min(maxBatchSize, currentSize * 2);
                        return builder.build();
                    }
                };
            }
        };
    }

    private static class ThreadContextClassLoader
            implements Closeable
    {
        private final ClassLoader originalThreadContextClassLoader;

        private ThreadContextClassLoader(ClassLoader newThreadContextClassLoader)
        {
            this.originalThreadContextClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(newThreadContextClassLoader);
        }

        @Override
        public void close()
        {
            Thread.currentThread().setContextClassLoader(originalThreadContextClassLoader);
        }
    }
}
