package com.facebook.presto.hive;

import com.facebook.presto.hive.util.AsyncRecursiveWalker;
import com.facebook.presto.hive.util.BoundedExecutor;
import com.facebook.presto.hive.util.FileStatusCallback;
import com.facebook.presto.hive.util.SuspendingExecutor;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ObjectNotFoundException;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.spi.PartitionInfo;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.spi.SchemaField.Type;
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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.json.JsonCodec;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.hive.HadoopConfiguration.HADOOP_CONFIGURATION;
import static com.facebook.presto.hive.HiveColumn.indexGetter;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.transform;
import static java.lang.Math.min;
import static java.lang.String.format;
import static org.apache.hadoop.hive.metastore.api.Constants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat.SymlinkTextInputSplit;

@SuppressWarnings("deprecation")
public class HiveClient
        implements ImportClient
{
    private static final int PARTITION_BATCH_SIZE = 1000;

    private static final String UNPARTITIONED_NAME = "<UNPARTITIONED>";

    // TODO: consider injecting this static instance
    private static final ExecutorService HIVE_CLIENT_EXECUTOR = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                    .setNameFormat("hive-client-%d")
                    .setDaemon(true)
                    .build()
    );

    private final String metastoreHost;
    private final int metastorePort;
    private final long maxChunkBytes;
    private final int maxOutstandingChunks;
    private final int maxChunkIteratorThreads;
    private final JsonCodec<HivePartitionChunk> partitionChunkCodec;

    public HiveClient(String metastoreHost, int metastorePort, long maxChunkBytes, int maxOutstandingChunks, int maxChunkIteratorThreads, JsonCodec<HivePartitionChunk> partitionChunkCodec)
    {
        this.metastoreHost = metastoreHost;
        this.metastorePort = metastorePort;
        this.maxChunkBytes = maxChunkBytes;
        this.maxOutstandingChunks = maxOutstandingChunks;
        this.maxChunkIteratorThreads = maxChunkIteratorThreads;
        this.partitionChunkCodec = partitionChunkCodec;

        HadoopNative.requireHadoopNative();
    }

    @Override
    public List<String> getDatabaseNames()
    {
        try (HiveMetastoreClient metastore = getClient()) {
            return metastore.get_all_databases();
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<String> getTableNames(String databaseName)
            throws ObjectNotFoundException
    {
        try (HiveMetastoreClient metastore = getClient()) {
            List<String> tables = metastore.get_all_tables(databaseName);
            if (tables.isEmpty()) {
                // Check to see if the database exists
                metastore.get_database(databaseName);
            }
            return tables;
        }
        catch (NoSuchObjectException e) {
            throw new ObjectNotFoundException(e.getMessage());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<SchemaField> getTableSchema(String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        try (HiveMetastoreClient metastore = getClient()) {
            Table table = metastore.get_table(databaseName, tableName);
            List<FieldSchema> partitionKeys = table.getPartitionKeys();
            Properties schema = MetaStoreUtils.getSchema(table);
            return getSchemaFields(schema, partitionKeys);
        }
        catch (NoSuchObjectException e) {
            throw new ObjectNotFoundException(e.getMessage());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<SchemaField> getPartitionKeys(String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        try (HiveMetastoreClient metastore = getClient()) {
            Table table = metastore.get_table(databaseName, tableName);
            List<FieldSchema> partitionKeys = table.getPartitionKeys();

            ImmutableList.Builder<SchemaField> schemaFields = ImmutableList.builder();
            for (int i = 0; i < partitionKeys.size(); i++) {
                FieldSchema field = partitionKeys.get(i);
                Type type = convertHiveType(field.getType());

                // partition keys are always the first fields in the table
                schemaFields.add(SchemaField.createPrimitive(field.getName(), i, type));
            }

            return schemaFields.build();
        }
        catch (NoSuchObjectException e) {
            throw new ObjectNotFoundException(e.getMessage());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<String> getPartitionNames(String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        try (HiveMetastoreClient metastore = getClient()) {
            List<String> partitionNames = metastore.get_partition_names(databaseName, tableName, (short) 0);
            if (partitionNames.isEmpty()) {
                // Check to see if the table exists
                metastore.get_table(databaseName, tableName);
                return ImmutableList.of(UNPARTITIONED_NAME);
            }
            return partitionNames;
        }
        catch (NoSuchObjectException e) {
            throw new ObjectNotFoundException(e.getMessage());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<PartitionInfo> getPartitions(String databaseName, String tableName, final Map<String, Object> filters)
            throws ObjectNotFoundException
    {
        // build the filtering prefix
        List<String> parts = new ArrayList<>();
        List<SchemaField> partitionKeys = getPartitionKeys(databaseName, tableName);
        for (SchemaField key : partitionKeys) {
            Object value = filters.get(key.getFieldName());

            if (value == null) {
                // we're building a partition prefix, so stop at the first missing binding
                break;
            }

            Preconditions.checkArgument(value instanceof String || value instanceof Double || value instanceof Long,
                    "Only String, Double and Long partition keys are supported");

            parts.add(value.toString());
        }

        // fetch the partition names
        List<PartitionInfo> partitions;
        if (parts.isEmpty()) {
            partitions = getPartitions(databaseName, tableName);
        }
        else {
            try (HiveMetastoreClient metastore = getClient()) {
                List<String> names = metastore.get_partition_names_ps(databaseName, tableName, parts, (short) -1);
                partitions = Lists.transform(names, toPartitionInfo(partitionKeys));
            }
            catch (NoSuchObjectException e) {
                throw new ObjectNotFoundException(e.getMessage());
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        // do a final pass to filter based on fields that could not be used to build the prefix
        return ImmutableList.copyOf(Iterables.filter(partitions, partitionMatches(filters)));
    }

    @Override
    public List<PartitionInfo> getPartitions(String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        List<SchemaField> partitionKeys = getPartitionKeys(databaseName, tableName);
        return Lists.transform(getPartitionNames(databaseName, tableName), toPartitionInfo(partitionKeys));
    }

    @Override
    public Iterable<PartitionChunk> getPartitionChunks(String databaseName, String tableName, String partitionName, List<String> columns)
            throws ObjectNotFoundException
    {
        return getPartitionChunks(databaseName, tableName, ImmutableList.of(partitionName), columns);
    }

    @Override
    public Iterable<PartitionChunk> getPartitionChunks(String databaseName, String tableName, List<String> partitionNames, List<String> columns)
            throws ObjectNotFoundException
    {
        Table table = getTable(databaseName, tableName);

        List<Partition> partitions = getPartitions(databaseName, tableName, partitionNames);


        if (partitionNames.size() != partitions.size()) {
            throw new ObjectNotFoundException(format("expected %s partitions but found %s", partitionNames.size(), partitions.size()));
        }

        return getPartitionChunks(table, partitions, getHiveColumns(table, columns));
    }

    @Override
    public RecordCursor getRecords(PartitionChunk partitionChunk)
    {
        checkArgument(partitionChunk instanceof HivePartitionChunk,
                "expected instance of %s: %s", HivePartitionChunk.class, partitionChunk.getClass());
        assert partitionChunk instanceof HivePartitionChunk; // // IDEA-60343
        HivePartitionChunk chunk = (HivePartitionChunk) partitionChunk;

        try {
            // Clone schema since we modify it below
            Properties schema = (Properties) chunk.getSchema().clone();

            // We are handling parsing directly since the hive code is slow
            // In order to do this, remove column types entry so that hive treats all columns as type "string"
            String typeSpecification = (String) schema.remove(Constants.LIST_COLUMN_TYPES);
            Preconditions.checkNotNull(typeSpecification, "Partition column type specification is null");

            String nullSequence = (String) schema.get(Constants.SERIALIZATION_NULL_FORMAT);
            checkState(nullSequence == null || nullSequence.equals("\\N"), "Only '\\N' supported as null specifier, was '%s'", nullSequence);

            // Tell hive the columns we would like to read, this lets hive optimize reading column oriented files
            List<HiveColumn> columns = chunk.getColumns();
            if (columns.isEmpty()) {
                // for count(*) queries we will have "no" columns we want to read, but since hive doesn't
                // support no columns (it will read all columns instead), we must choose a single column
                columns = ImmutableList.of(getFirstPrimitiveColumn(schema));
            }
            ColumnProjectionUtils.setReadColumnIDs(HADOOP_CONFIGURATION.get(), new ArrayList<>(transform(columns, indexGetter())));

            RecordReader<?, ?> recordReader = createRecordReader(chunk);
            if (recordReader.createValue() instanceof BytesRefArrayWritable) {
                return new BytesHiveRecordCursor<>((RecordReader<?, BytesRefArrayWritable>) recordReader, chunk.getLength(), chunk.getPartitionKeys(), columns);
            }
            else {
                return new GenericHiveRecordCursor<>((RecordReader<?, ? extends Writable>) recordReader, chunk.getLength(), chunk.getSchema(), chunk.getPartitionKeys(), columns);
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private RecordReader<?, ?> createRecordReader(HivePartitionChunk chunk)
    {
        InputFormat inputFormat = getInputFormat(chunk.getSchema(), true);
        FileSplit split = new FileSplit(chunk.getPath(), chunk.getStart(), chunk.getLength(), (String[]) null);
        JobConf jobConf = new JobConf(HADOOP_CONFIGURATION.get());

        try {
            return inputFormat.getRecordReader(split, jobConf, Reporter.NULL);
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to create record reader for input format " + getInputFormatName(chunk.getSchema()), e);
        }
    }

    private static InputFormat getInputFormat(Properties schema, boolean symlinkTarget)
    {
        String inputFormatName = getInputFormatName(schema);
        try {
            JobConf jobConf = new JobConf(HADOOP_CONFIGURATION.get());

            // This code should be equivalent to jobConf.getInputFormat()
            Class<? extends InputFormat> inputFormatClass = jobConf.getClassByName(inputFormatName).asSubclass(InputFormat.class);
            if (inputFormatClass == null) {
                // default file format in Hadoop is TextInputFormat
                inputFormatClass = TextInputFormat.class;
            }
            else if (symlinkTarget && (inputFormatClass == SymlinkTextInputFormat.class)) {
                // symlink targets are always TextInputFormat
                inputFormatClass = TextInputFormat.class;
            }
            return ReflectionUtils.newInstance(inputFormatClass, jobConf);
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to create input format " + inputFormatName, e);
        }
    }

    @Override
    public byte[] serializePartitionChunk(PartitionChunk partitionChunk)
    {
        checkArgument(partitionChunk instanceof HivePartitionChunk,
                "expected instance of %s: %s", HivePartitionChunk.class, partitionChunk.getClass());
        return partitionChunkCodec.toJson((HivePartitionChunk) partitionChunk).getBytes(UTF_8);
    }

    @Override
    public PartitionChunk deserializePartitionChunk(byte[] bytes)
    {
        return partitionChunkCodec.fromJson(new String(bytes, UTF_8));
    }

    private HiveMetastoreClient getClient()
    {
        try {
            return HiveMetastoreClient.create(metastoreHost, metastorePort);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private HiveColumn getFirstPrimitiveColumn(Properties schema)
    {
        try {
            Deserializer deserializer = MetaStoreUtils.getDeserializer(null, schema);
            StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

            int index = 0;
            for (StructField field : rowInspector.getAllStructFieldRefs()) {
                if (field.getFieldObjectInspector().getCategory() == ObjectInspector.Category.PRIMITIVE) {
                    PrimitiveObjectInspector inspector = (PrimitiveObjectInspector) field.getFieldObjectInspector();
                    SchemaField.Type type = getSupportedPrimitiveType(inspector.getPrimitiveCategory());
                    return new HiveColumn(field.getFieldName(), index, type, inspector.getPrimitiveCategory());
                }
                index++;
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        throw new IllegalStateException("Table doesn't have any PRIMITIVE columns");
    }

    private static List<HiveColumn> getHiveColumns(Table table, List<String> columns)
    {
        HashSet<String> columnNames = new HashSet<>(columns);

        // remove primary keys
        for (FieldSchema fieldSchema : table.getPartitionKeys()) {
            columnNames.remove(fieldSchema.getName());
        }

        try {
            Properties schema = MetaStoreUtils.getSchema(table);
            Deserializer deserializer = MetaStoreUtils.getDeserializer(null, schema);
            StructObjectInspector tableInspector = (StructObjectInspector) deserializer.getObjectInspector();

            ImmutableList.Builder<HiveColumn> hiveColumns = ImmutableList.builder();
            int index = 0;
            for (StructField field : tableInspector.getAllStructFieldRefs()) {
                // ignore unused columns
                // remove the columns as we find them so we can know if all columns were found
                if (columnNames.remove(field.getFieldName())) {

                    ObjectInspector fieldInspector = field.getFieldObjectInspector();
                    Preconditions.checkArgument(fieldInspector.getCategory() == Category.PRIMITIVE, "Column %s is not a primitive type", field.getFieldName());
                    PrimitiveObjectInspector inspector = (PrimitiveObjectInspector) fieldInspector;
                    Type type = getSupportedPrimitiveType(inspector.getPrimitiveCategory());
                    PrimitiveCategory hiveType = inspector.getPrimitiveCategory();

                    hiveColumns.add(new HiveColumn(field.getFieldName(), index, type, hiveType));
                }
                index++;
            }

            Preconditions.checkArgument(columnNames.isEmpty(), "Table %s does not contain the columns %s", table.getTableName(), columnNames);

            return hiveColumns.build();
        }
        catch (MetaException | SerDeException e) {
            throw Throwables.propagate(e);
        }
    }

    private Table getTable(String databaseName, String tableName)
            throws ObjectNotFoundException
    {
        try (HiveMetastoreClient metastore = getClient()) {
            return metastore.get_table(databaseName, tableName);
        }
        catch (NoSuchObjectException e) {
            throw new ObjectNotFoundException(e.getMessage());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private List<Partition> getPartitions(String databaseName, String tableName, List<String> partitionNames)
            throws ObjectNotFoundException
    {
        if (partitionNames.equals(ImmutableList.of(UNPARTITIONED_NAME))) {
            return ImmutableList.<Partition>of(UnpartitionedPartition.INSTANCE);
        }

        try (HiveMetastoreClient metastore = getClient()) {
            ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
            for (List<String> batchedPartitionNames : Lists.partition(partitionNames, PARTITION_BATCH_SIZE)) {
                partitions.addAll(metastore.get_partitions_by_names(databaseName, tableName, batchedPartitionNames));
            }
            return partitions.build();
        }
        catch (NoSuchObjectException e) {
            throw new ObjectNotFoundException(e.getMessage());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private Iterable<PartitionChunk> getPartitionChunks(Table table, List<Partition> partitions, List<HiveColumn> columns)
    {
        return new PartitionChunkIterable(table, partitions, columns, maxChunkBytes, maxOutstandingChunks, maxChunkIteratorThreads, HIVE_CLIENT_EXECUTOR);
    }

    private static class PartitionChunkIterable
            implements Iterable<PartitionChunk>
    {
        private static final PartitionChunk FINISHED_MARKER = new PartitionChunk()
        {
            @Override
            public long getLength()
            {
                throw new UnsupportedOperationException();
            }
        };

        private final Table table;
        private final List<Partition> partitions;
        private final List<HiveColumn> columns;
        private final long maxChunkBytes;
        private final int maxOutstandingChunks;
        private final int maxThreads;
        private final Executor executor;

        private PartitionChunkIterable(Table table, List<Partition> partitions, List<HiveColumn> columns, long maxChunkBytes, int maxOutstandingChunks, int maxThreads, Executor executor)
        {
            this.table = table;
            this.partitions = ImmutableList.copyOf(partitions);
            this.columns = ImmutableList.copyOf(columns);
            this.maxChunkBytes = maxChunkBytes;
            this.maxOutstandingChunks = maxOutstandingChunks;
            this.maxThreads = maxThreads;
            this.executor = executor;
        }

        @Override
        public Iterator<PartitionChunk> iterator()
        {
            // Each iterator has its own bounded executor and can be independently suspended
            SuspendingExecutor suspendingExecutor = new SuspendingExecutor(new BoundedExecutor(executor, maxThreads));
            final PartitionChunkQueue partitionChunkQueue = new PartitionChunkQueue(maxOutstandingChunks, suspendingExecutor);
            ImmutableList.Builder<ListenableFuture<Void>> futureBuilder = ImmutableList.builder();
            try {
                for (Partition partition : partitions) {
                    final Properties schema = getPartitionSchema(table, partition);
                    final List<HivePartitionKey> partitionKeys = getPartitionKeys(table, partition);
                    final InputFormat inputFormat = getInputFormat(schema, false);
                    Path partitionPath = new CachingPath(getPartitionLocation(table, partition));

                    if (inputFormat instanceof SymlinkTextInputFormat) {
                        JobConf jobConf = new JobConf(HADOOP_CONFIGURATION.get());
                        FileInputFormat.setInputPaths(jobConf, partitionPath);
                        InputSplit[] splits = inputFormat.getSplits(jobConf, 0);
                        for (InputSplit rawSplit : splits) {
                            FileSplit split = ((SymlinkTextInputSplit) rawSplit).getTargetSplit();
                            partitionChunkQueue.addToQueue(new HivePartitionChunk(
                                    split.getPath(), split.getStart(), split.getLength(), schema, partitionKeys, columns));
                        }
                        continue;
                    }

                    FileSystem fs = partitionPath.getFileSystem(HADOOP_CONFIGURATION.get());

                    futureBuilder.add(new AsyncRecursiveWalker(fs, suspendingExecutor).beginWalk(partitionPath, new FileStatusCallback()
                    {
                        @Override
                        public void process(FileStatus file)
                        {
                            try {
                                boolean splittable = isSplittable(inputFormat,
                                        file.getPath().getFileSystem(HADOOP_CONFIGURATION.get()),
                                        file.getPath());

                                long splitSize = splittable ? maxChunkBytes : file.getLen();
                                for (long start = 0; start < file.getLen(); start += splitSize) {
                                    long length = min(splitSize, file.getLen() - start);
                                    partitionChunkQueue.addToQueue(new HivePartitionChunk(file.getPath(), start, length, schema, partitionKeys, columns));
                                }
                            }
                            catch (IOException e) {
                                partitionChunkQueue.fail(e);
                            }
                        }
                    }));
                }

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
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            return partitionChunkQueue;
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
            }

            @Override
            protected PartitionChunk computeNext()
            {
                if (throwable.get() != null) {
                    throw Throwables.propagate(throwable.get());
                }

                try {
                    PartitionChunk chunk = queue.take();
                    if (chunk == FINISHED_MARKER) {
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

    private static boolean isSplittable(InputFormat inputFormat, FileSystem fileSystem, Path path)
    {
        // use reflection to get isSplitable method on InputFormat
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
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private static Function<String, PartitionInfo> toPartitionInfo(final List<SchemaField> keys)
    {
        return new Function<String, PartitionInfo>()
        {
            @Override
            public PartitionInfo apply(String partitionName)
            {
                if (partitionName.equals(UNPARTITIONED_NAME)) {
                    return new PartitionInfo(UNPARTITIONED_NAME);
                }

                try {
                    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                    List<String> parts = Warehouse.getPartValuesFromPartName(partitionName);
                    for (int i = 0; i < parts.size(); i++) {
                        builder.put(keys.get(i).getFieldName(), parts.get(i));
                    }

                    return new PartitionInfo(partitionName, builder.build());
                }
                catch (MetaException e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }

    public static final Predicate<PartitionInfo> partitionMatches(final Map<String, Object> filters)
    {
        return new Predicate<PartitionInfo>()
        {
            @Override
            public boolean apply(PartitionInfo partition)
            {
                for (Map.Entry<String, String> entry : partition.getKeyFields().entrySet()) {
                    String partitionKey = entry.getKey();
                    Object filterValue = filters.get(partitionKey);
                    if (filterValue != null && !entry.getValue().equals(filterValue)) {
                        return false;
                    }
                }
                return true;
            }
        };
    }

    private static String getInputFormatName(Properties schema)
    {
        String name = schema.getProperty(FILE_INPUT_FORMAT);
        checkArgument(name != null, "missing property: %s", FILE_INPUT_FORMAT);
        return name;
    }

    private static List<SchemaField> getSchemaFields(Properties schema, List<FieldSchema> partitionKeys)
            throws MetaException, SerDeException
    {
        Deserializer deserializer = MetaStoreUtils.getDeserializer(null, schema);
        ObjectInspector inspector = deserializer.getObjectInspector();
        checkArgument(inspector.getCategory() == ObjectInspector.Category.STRUCT, "expected STRUCT: %s", inspector.getCategory());
        StructObjectInspector structObjectInspector = (StructObjectInspector) inspector;

        ImmutableList.Builder<SchemaField> schemaFields = ImmutableList.builder();

        // add the partition keys
        for (int i = 0; i < partitionKeys.size(); i++) {
            FieldSchema field = partitionKeys.get(i);
            SchemaField.Type type = convertHiveType(field.getType());
            schemaFields.add(SchemaField.createPrimitive(field.getName(), i, type));
        }

        // add the data fields
        List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
        int columnIndex = partitionKeys.size();
        for (StructField field : fields) {
            ObjectInspector fieldInspector = field.getFieldObjectInspector();

            // ignore containers rather than failing
            if (fieldInspector.getCategory() == Category.PRIMITIVE) {
                Type type = getPrimitiveType(((PrimitiveObjectInspector) fieldInspector).getPrimitiveCategory());
                if (type != null) { // ignore unsupported primitive types
                    schemaFields.add(SchemaField.createPrimitive(field.getFieldName(), columnIndex, type));
                }
            }

            columnIndex++;
        }

        return schemaFields.build();
    }

    private static SchemaField.Type getSupportedPrimitiveType(PrimitiveCategory category)
    {
        Type type = getPrimitiveType(category);
        if (type == null) {
            throw new IllegalArgumentException("Hive type not supported: " + category);
        }
        return type;
    }

    private static SchemaField.Type getPrimitiveType(PrimitiveCategory category)
    {
        switch (category) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return SchemaField.Type.LONG;
            case FLOAT:
            case DOUBLE:
                return SchemaField.Type.DOUBLE;
            case STRING:
                return SchemaField.Type.STRING;
            case BOOLEAN:
                return SchemaField.Type.LONG;
            default:
                return null;
        }
    }

    private static SchemaField.Type convertHiveType(String type)
    {
        return getSupportedPrimitiveType(convertNativeHiveType(type));
    }

    private static PrimitiveCategory convertNativeHiveType(String type)
    {
        return PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName(type).primitiveCategory;
    }

    @SuppressWarnings("CloneableClassWithoutClone")
    private static class UnpartitionedPartition
            extends Partition
    {
        public static final UnpartitionedPartition INSTANCE = new UnpartitionedPartition();
    }

    private static List<HivePartitionKey> getPartitionKeys(Table table, Partition partition)
    {
        if (partition instanceof UnpartitionedPartition) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<HivePartitionKey> partitionKeys = ImmutableList.builder();
        List<FieldSchema> keys = table.getPartitionKeys();
        List<String> values = partition.getValues();
        checkArgument(keys.size() == values.size(), "Expected %s partition key values, but got %s", keys.size(), values.size());
        for (int i = 0; i < keys.size(); i++) {
            String name = keys.get(i).getName();
            PrimitiveCategory hiveType = convertNativeHiveType(keys.get(i).getType());
            Type type = getSupportedPrimitiveType(hiveType);
            String value = values.get(i);
            checkNotNull(value, "partition key value cannot be null for field: %s", name);
            partitionKeys.add(new HivePartitionKey(name, type, hiveType, value));
        }
        return partitionKeys.build();
    }

    private static Properties getPartitionSchema(Table table, Partition partition)
    {
        if (partition instanceof UnpartitionedPartition) {
            return MetaStoreUtils.getSchema(table);
        }
        return MetaStoreUtils.getSchema(partition, table);
    }

    private static String getPartitionLocation(Table table, Partition partition)
    {
        if (partition instanceof UnpartitionedPartition) {
            return table.getSd().getLocation();
        }
        return partition.getSd().getLocation();
    }
}
