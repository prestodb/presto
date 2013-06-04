package com.facebook.presto.hive;

import com.facebook.presto.hadoop.HadoopNative;
import com.facebook.presto.hive.util.AsyncRecursiveWalker;
import com.facebook.presto.hive.util.BoundedExecutor;
import com.facebook.presto.hive.util.FileStatusCallback;
import com.facebook.presto.hive.util.SuspendingExecutor;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.ProtectMode;
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
import static com.facebook.presto.hive.HiveColumnHandle.hiveColumnHandle;
import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;
import static com.facebook.presto.hive.HiveSplit.markAsLastSplit;
import static com.facebook.presto.hive.HiveType.getHiveType;
import static com.facebook.presto.hive.HiveType.getSupportedHiveType;
import static com.facebook.presto.hive.HiveUtil.HIVE_TIMESTAMP_PARSER;
import static com.facebook.presto.hive.HiveUtil.convertNativeHiveType;
import static com.facebook.presto.hive.HiveUtil.getInputFormat;
import static com.facebook.presto.hive.UnpartitionedPartition.UNPARTITIONED_PARTITION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.hadoop.hive.metastore.ProtectMode.getProtectModeFromString;
import static org.apache.hadoop.hive.metastore.Warehouse.makePartName;

@SuppressWarnings("deprecation")
public class HiveClient
        implements ConnectorMetadata, ConnectorSplitManager, ConnectorRecordSetProvider, ConnectorHandleResolver
{
    static {
        HadoopNative.requireHadoopNative();
    }

    private static final Logger log = Logger.get(HiveClient.class);

    private final String connectorId;
    private final int maxOutstandingSplits;
    private final int maxSplitIteratorThreads;
    private final int partitionBatchSize;
    private final CachingHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final ExecutorService executor;
    private final DataSize maxSplitSize;

    @Inject
    public HiveClient(HiveConnectorId connectorId,
            HiveClientConfig hiveClientConfig,
            CachingHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            @ForHiveClient ExecutorService executor)
    {
        this(connectorId,
                metastore,
                hdfsEnvironment,
                executor,
                hiveClientConfig.getMaxSplitSize(),
                hiveClientConfig.getMaxOutstandingSplits(),
                hiveClientConfig.getMaxSplitIteratorThreads(),
                hiveClientConfig.getPartitionBatchSize());
    }

    public HiveClient(HiveConnectorId connectorId,
            CachingHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            ExecutorService executor,
            DataSize maxSplitSize,
            int maxOutstandingSplits,
            int maxSplitIteratorThreads,
            int partitionBatchSize)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();

        this.maxSplitSize = checkNotNull(maxSplitSize, "maxSplitSize is null");
        this.maxOutstandingSplits = maxOutstandingSplits;
        this.maxSplitIteratorThreads = maxSplitIteratorThreads;
        this.partitionBatchSize = partitionBatchSize;

        this.metastore = checkNotNull(metastore, "metastore is null");
        this.hdfsEnvironment = checkNotNull(hdfsEnvironment, "hdfsEnvironment is null");

        this.executor = checkNotNull(executor, "executor is null");
    }

    public CachingHiveMetastore getMetastore()
    {
        return metastore;
    }

    @Override
    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public List<String> listSchemaNames()
    {
        return metastore.getAllDatabases();
    }

    @Override
    public HiveTableHandle getTableHandle(SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        try {
            metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            return new HiveTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
        }
        catch (NoSuchObjectException e) {
            // table was not found
            return null;
        }
    }

    private SchemaTableName getTableName(TableHandle tableHandle)
    {
        checkArgument(tableHandle instanceof HiveTableHandle, "tableHandle is not an instance of HiveTableHandle");
        return ((HiveTableHandle) tableHandle).getSchemaTableName();
    }

    @Override
    public TableMetadata getTableMetadata(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = getTableName(tableHandle);
        return getTableMetadata(tableName);
    }

    private TableMetadata getTableMetadata(SchemaTableName tableName)
    {
        try {
            Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            List<ColumnMetadata> columns = ImmutableList.copyOf(transform(getColumnHandles(table), columnMetadataGetter()));

            return new TableMetadata(tableName, columns);
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
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnName, "columnName is null");
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
            Deserializer deserializer = MetaStoreUtils.getDeserializer(null, MetaStoreUtils.getTableMetadata(table));
            ObjectInspector inspector = deserializer.getObjectInspector();
            checkArgument(inspector.getCategory() == ObjectInspector.Category.STRUCT, "expected STRUCT: %s", inspector.getCategory());
            StructObjectInspector structObjectInspector = (StructObjectInspector) inspector;

            ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();

            // add the partition keys
            List<FieldSchema> partitionKeys = table.getPartitionKeys();
            for (int i = 0; i < partitionKeys.size(); i++) {
                FieldSchema field = partitionKeys.get(i);

                HiveType hiveType = getSupportedHiveType(field.getType());
                columns.add(new HiveColumnHandle(connectorId, field.getName(), i, hiveType, -1, true));
            }

            // add the data fields
            int hiveColumnIndex = 0;
            for (StructField field : structObjectInspector.getAllStructFieldRefs()) {
                // ignore unsupported types rather than failing
                HiveType hiveType = getHiveType(field.getFieldObjectInspector());
                if (hiveType != null) {
                    columns.add(new HiveColumnHandle(connectorId, field.getFieldName(), partitionKeys.size() + hiveColumnIndex, hiveType, hiveColumnIndex, false));
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
        checkNotNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(prefix)) {
            try {
                columns.put(tableName, getTableMetadata(tableName).getColumns());
            }
            catch (RuntimeException e) {
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");
        checkArgument(tableHandle instanceof HiveTableHandle, "tableHandle is not an instance of HiveTableHandle");
        checkArgument(columnHandle instanceof HiveColumnHandle, "columnHandle is not an instance of HiveColumnHandle");
        return ((HiveColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public TableHandle createTable(TableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Partition> getPartitions(TableHandle tableHandle, Map<ColumnHandle, Object> bindings)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(bindings, "bindings is null");
        SchemaTableName tableName = getTableName(tableHandle);

        List<FieldSchema> partitionKeys;
        try {
            Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());

            String protectMode = table.getParameters().get(ProtectMode.PARAMETER_NAME);
            if (protectMode != null && getProtectModeFromString(protectMode).offline) {
                throw new TableOfflineException(tableName);
            }

            partitionKeys = table.getPartitionKeys();
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }

        LinkedHashMap<String, ColumnHandle> partitionKeysByName = new LinkedHashMap<>();
        List<String> filterPrefix = new ArrayList<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            FieldSchema field = partitionKeys.get(i);

            HiveColumnHandle columnHandle = new HiveColumnHandle(connectorId, field.getName(), i, getSupportedHiveType(field.getType()), -1, true);
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
            if (partitionKeys.isEmpty()) {
                partitionNames = ImmutableList.of(UNPARTITIONED_ID);
            }
            else if (filterPrefix.isEmpty()) {
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
    public Iterable<Split> getPartitionSplits(List<Partition> partitions)
    {
        checkNotNull(partitions, "partitions is null");

        Partition partition = Iterables.getFirst(partitions, null);
        if (partition == null) {
            return ImmutableList.of();
        }
        checkArgument(partition instanceof HivePartition, "Partition must be a hive partition");
        SchemaTableName tableName = ((HivePartition) partition).getTableName();

        List<String> partitionNames = new ArrayList<>(Lists.transform(partitions, HiveUtil.partitionIdGetter()));
        Collections.sort(partitionNames, Ordering.natural().reverse());

        Table table;
        Iterable<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions;
        try {
            table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            hivePartitions = getPartitions(table, tableName, partitionNames);
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }

        return new HiveSplitIterable(connectorId,
                table,
                partitionNames,
                hivePartitions,
                maxSplitSize,
                maxOutstandingSplits,
                maxSplitIteratorThreads,
                hdfsEnvironment,
                executor,
                partitionBatchSize);
    }

    private Iterable<org.apache.hadoop.hive.metastore.api.Partition> getPartitions(final Table table, final SchemaTableName tableName, final List<String> partitionNames)
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
                        List<org.apache.hadoop.hive.metastore.api.Partition> partitions = metastore.getPartitionsByNames(table, tableName.getSchemaName(), tableName.getTableName(), partitionNameBatch);
                        checkState(partitionNameBatch.size() == partitions.size(), "expected %s partitions but found %s", partitionNameBatch.size(), partitions.size());

                        // verify all partitions are online
                        for (org.apache.hadoop.hive.metastore.api.Partition partition : partitions) {
                            String protectMode = partition.getParameters().get(ProtectMode.PARAMETER_NAME);
                            if (protectMode != null && getProtectModeFromString(protectMode).offline) {
                                throw new PartitionOfflineException(tableName, makePartName(table.getPartitionKeys(), partition.getValues()));
                            }
                        }

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
    public RecordSet getRecordSet(Split split, List<? extends ColumnHandle> columns)
    {
        checkNotNull(split, "split is null");
        checkNotNull(columns, "columns is null");
        checkArgument(split instanceof HiveSplit, "expected instance of %s: %s", HiveSplit.class, split.getClass());

        List<HiveColumnHandle> hiveColumns = ImmutableList.copyOf(transform(columns, hiveColumnHandle()));
        return new HiveRecordSet(hdfsEnvironment, (HiveSplit) split, hiveColumns);
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof HiveTableHandle && ((HiveTableHandle) tableHandle).getClientId().equals(connectorId);
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return columnHandle instanceof HiveColumnHandle && ((HiveColumnHandle) columnHandle).getClientId().equals(connectorId);
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof HiveSplit && ((HiveSplit) split).getClientId().equals(connectorId);
    }

    @Override
    public Class<? extends TableHandle> getTableHandleClass()
    {
        return HiveTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return HiveColumnHandle.class;
    }

    @Override
    public Class<? extends Split> getSplitClass()
    {
        return HiveSplit.class;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("clientId", connectorId)
                .toString();
    }

    private static class HiveSplitIterable
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

        private final String clientId;
        private final Table table;
        private final Iterable<String> partitionNames;
        private final Iterable<org.apache.hadoop.hive.metastore.api.Partition> partitions;
        private final int maxOutstandingSplits;
        private final int maxThreads;
        private final HdfsEnvironment hdfsEnvironment;
        private final ExecutorService executor;
        private final ClassLoader classLoader;
        private final DataSize maxSplitSize;
        private final int partitionBatchSize;

        private HiveSplitIterable(String clientId,
                Table table,
                Iterable<String> partitionNames,
                Iterable<org.apache.hadoop.hive.metastore.api.Partition> partitions,
                DataSize maxSplitSize,
                int maxOutstandingSplits,
                int maxThreads,
                HdfsEnvironment hdfsEnvironment,
                ExecutorService executor,
                int partitionBatchSize)
        {
            this.clientId = clientId;
            this.table = table;
            this.partitionNames = partitionNames;
            this.partitions = partitions;
            this.maxSplitSize = maxSplitSize;
            this.partitionBatchSize = partitionBatchSize;
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

        private void loadPartitionSplits(final HiveSplitQueue hiveSplitQueue, final SuspendingExecutor suspendingExecutor)
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
                    final LastSplitMarkingQueue markerQueue = new LastSplitMarkingQueue(hiveSplitQueue);

                    if (inputFormat instanceof SymlinkTextInputFormat) {
                        JobConf jobConf = new JobConf(hdfsEnvironment.getConfiguration());
                        FileInputFormat.setInputPaths(jobConf, partitionPath);
                        InputSplit[] splits = inputFormat.getSplits(jobConf, 0);
                        for (InputSplit rawSplit : splits) {
                            FileSplit split = ((SymlinkTextInputSplit) rawSplit).getTargetSplit();

                            // get the filesystem for the target path -- it may be a different hdfs instance
                            FileSystem targetFilesystem = split.getPath().getFileSystem(hdfsEnvironment.getConfiguration());
                            markerQueue.addToQueue(createHiveSplits(partitionName,
                                    targetFilesystem.getFileStatus(split.getPath()),
                                    split.getStart(),
                                    split.getLength(),
                                    schema,
                                    partitionKeys,
                                    targetFilesystem,
                                    false));
                        }
                        markerQueue.finish();
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

                                markerQueue.addToQueue(createHiveSplits(partitionName, file, 0, file.getLen(), schema, partitionKeys, fs, splittable));
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

        private List<HiveSplit> createHiveSplits(
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

            ImmutableList.Builder<HiveSplit> builder = ImmutableList.builder();
            if (splittable) {
                for (BlockLocation blockLocation : fileBlockLocations) {
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
            } else {
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
                        toHostAddress(fileBlockLocations[0].getHosts())));
            }
            return builder.build();
        }

        private List<HostAddress> toHostAddress(String[] hosts)
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
                    if (split == FINISHED_MARKER) {
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
                    ImmutableMap.Builder<ColumnHandle, Object> builder = ImmutableMap.builder();
                    for (Entry<String, String> entry : keys.entrySet()) {
                        ColumnHandle columnHandle = columnsByName.get(entry.getKey());
                        checkArgument(columnHandle != null, "Invalid partition key %s in partition %s", entry.getKey(), partitionId);
                        checkArgument(columnHandle instanceof HiveColumnHandle, "columnHandle is not an instance of HiveColumnHandle");
                        HiveColumnHandle hiveColumnHandle = (HiveColumnHandle) columnHandle;

                        String value = entry.getValue();
                        switch (hiveColumnHandle.getType()) {
                            case LONG:
                                if (value.length() == 0) {
                                    builder.put(columnHandle, 0L);
                                }
                                else if (hiveColumnHandle.getHiveType() == HiveType.BOOLEAN) {
                                    builder.put(columnHandle, parseBoolean(value));
                                }
                                else if (hiveColumnHandle.getHiveType() == HiveType.TIMESTAMP) {
                                    builder.put(columnHandle, MILLISECONDS.toSeconds(HIVE_TIMESTAMP_PARSER.parseMillis(value)));
                                }
                                else {
                                    builder.put(columnHandle, parseLong(value));
                                }
                                break;
                            case DOUBLE:
                                if (value.length() == 0) {
                                    builder.put(columnHandle, 0L);
                                }
                                else {
                                    builder.put(columnHandle, parseDouble(value));
                                }
                                break;
                            case STRING:
                                builder.put(columnHandle, value);
                                break;
                        }
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
                for (Map.Entry<ColumnHandle, Object> entry : partition.getKeys().entrySet()) {
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
            HiveType hiveType = getSupportedHiveType(primitiveCategory);
            String value = values.get(i);
            checkNotNull(value, "partition key value cannot be null for field: %s", name);
            partitionKeys.add(new HivePartitionKey(name, hiveType, value));
        }
        return partitionKeys.build();
    }

    private static Properties getPartitionSchema(Table table, org.apache.hadoop.hive.metastore.api.Partition partition)
    {
        if (partition == UNPARTITIONED_PARTITION) {
            return MetaStoreUtils.getTableMetadata(table);
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
