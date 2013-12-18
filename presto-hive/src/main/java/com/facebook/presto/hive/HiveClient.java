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

import com.facebook.presto.hadoop.HadoopNative;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.OutputTableHandle;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import org.apache.hadoop.hive.metastore.ProtectMode;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveBucketing.HiveBucket;
import static com.facebook.presto.hive.HiveBucketing.getHiveBucket;
import static com.facebook.presto.hive.HiveColumnHandle.columnMetadataGetter;
import static com.facebook.presto.hive.HiveColumnHandle.hiveColumnHandle;
import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;
import static com.facebook.presto.hive.HiveType.getHiveType;
import static com.facebook.presto.hive.HiveType.getSupportedHiveType;
import static com.facebook.presto.hive.HiveUtil.getTableStructFields;
import static com.facebook.presto.hive.HiveUtil.parseHiveTimestamp;
import static com.facebook.presto.hive.UnpartitionedPartition.UNPARTITIONED_PARTITION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
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
    private final int minPartitionBatchSize;
    private final int maxPartitionBatchSize;
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
                hiveClientConfig.getMinPartitionBatchSize(),
                hiveClientConfig.getMaxPartitionBatchSize());
    }

    public HiveClient(HiveConnectorId connectorId,
            CachingHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            ExecutorService executor,
            DataSize maxSplitSize,
            int maxOutstandingSplits,
            int maxSplitIteratorThreads,
            int minPartitionBatchSize,
            int maxPartitionBatchSize)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();

        this.maxSplitSize = checkNotNull(maxSplitSize, "maxSplitSize is null");
        this.maxOutstandingSplits = maxOutstandingSplits;
        this.maxSplitIteratorThreads = maxSplitIteratorThreads;
        this.minPartitionBatchSize = minPartitionBatchSize;
        this.maxPartitionBatchSize = maxPartitionBatchSize;

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

    private static SchemaTableName getTableName(TableHandle tableHandle)
    {
        checkArgument(tableHandle instanceof HiveTableHandle, "tableHandle is not an instance of HiveTableHandle");
        return ((HiveTableHandle) tableHandle).getSchemaTableName();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = getTableName(tableHandle);
        return getTableMetadata(tableName);
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        try {
            Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            List<ColumnMetadata> columns = ImmutableList.copyOf(transform(getColumnHandles(table), columnMetadataGetter()));
            return new ConnectorTableMetadata(tableName, columns);
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }
    }

    @Override
    public List<SchemaTableName> listTables(String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(schemaNameOrNull)) {
            try {
                for (String tableName : metastore.getAllTables(schemaName)) {
                    tableNames.add(new SchemaTableName(schemaName, tableName));
                }
            }
            catch (NoSuchObjectException e) {
                // schema disappeared during listing operation
            }
        }
        return tableNames.build();
    }

    private List<String> listSchemas(String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return listSchemaNames();
        }
        return ImmutableList.of(schemaNameOrNull);
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
            ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();

            // add the data fields first
            int hiveColumnIndex = 0;
            for (StructField field : getTableStructFields(table)) {
                // ignore unsupported types rather than failing
                HiveType hiveType = getHiveType(field.getFieldObjectInspector());
                if (hiveType != null) {
                    columns.add(new HiveColumnHandle(connectorId, field.getFieldName(), hiveColumnIndex, hiveType, hiveColumnIndex, false));
                }
                hiveColumnIndex++;
            }

            // add the partition keys last (like Hive does)
            List<FieldSchema> partitionKeys = table.getPartitionKeys();
            for (int i = 0; i < partitionKeys.size(); i++) {
                FieldSchema field = partitionKeys.get(i);

                HiveType hiveType = getSupportedHiveType(field.getType());
                columns.add(new HiveColumnHandle(connectorId, field.getName(), hiveColumnIndex + i, hiveType, -1, true));
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
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    private List<SchemaTableName> listTables(SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
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
    public TableHandle createTable(ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean canHandle(OutputTableHandle tableHandle)
    {
        return false;
    }

    @Override
    public OutputTableHandle beginCreateTable(ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitCreateTable(OutputTableHandle tableHandle, Collection<String> fragments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionResult getPartitions(TableHandle tableHandle, TupleDomain tupleDomain)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(tupleDomain, "tupleDomain is null");
        SchemaTableName tableName = getTableName(tableHandle);

        List<FieldSchema> partitionKeys;
        Optional<HiveBucket> bucket;

        try {
            Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());

            String protectMode = table.getParameters().get(ProtectMode.PARAMETER_NAME);
            if (protectMode != null && getProtectModeFromString(protectMode).offline) {
                throw new TableOfflineException(tableName);
            }

            partitionKeys = table.getPartitionKeys();
            bucket = getHiveBucket(table, tupleDomain.extractFixedValues());
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }

        ImmutableMap.Builder<String, ColumnHandle> partitionKeysByNameBuilder = ImmutableMap.builder();
        List<String> filterPrefix = new ArrayList<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            FieldSchema field = partitionKeys.get(i);

            HiveColumnHandle columnHandle = new HiveColumnHandle(connectorId, field.getName(), i, getSupportedHiveType(field.getType()), -1, true);
            partitionKeysByNameBuilder.put(field.getName(), columnHandle);

            // only add to prefix if all previous keys have a value
            if (filterPrefix.size() == i && !tupleDomain.isNone()) {
                Domain domain = tupleDomain.getDomains().get(columnHandle);
                if (domain != null && domain.getRanges().getRangeCount() == 1) {
                    // We intentionally ignore whether NULL is in the domain since partition keys can never be NULL
                    Range range = Iterables.getOnlyElement(domain.getRanges());
                    if (range.isSingleValue()) {
                        Comparable<?> value = range.getLow().getValue();
                        checkArgument(value instanceof Boolean || value instanceof String || value instanceof Double || value instanceof Long,
                                "Only Boolean, String, Double and Long partition keys are supported");
                        filterPrefix.add(value.toString());
                    }
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
        Map<String, ColumnHandle> partitionKeysByName = partitionKeysByNameBuilder.build();
        List<Partition> partitions = FluentIterable.from(partitionNames)
                .transform(toPartition(tableName, partitionKeysByName, bucket))
                .filter(partitionMatches(tupleDomain))
                .filter(Partition.class)
                .toList();

        // All partition key domains will be fully evaluated, so we don't need to include those
        TupleDomain remainingTupleDomain = TupleDomain.none();
        if (!tupleDomain.isNone()) {
            remainingTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(tupleDomain.getDomains(), not(in(partitionKeysByName.values()))));
        }

        return new PartitionResult(partitions, remainingTupleDomain);
    }

    @Override
    public Iterable<Split> getPartitionSplits(TableHandle tableHandle, List<Partition> partitions)
    {
        checkNotNull(partitions, "partitions is null");

        Partition partition = Iterables.getFirst(partitions, null);
        if (partition == null) {
            return ImmutableList.of();
        }
        checkArgument(partition instanceof HivePartition, "Partition must be a hive partition");
        SchemaTableName tableName = ((HivePartition) partition).getTableName();
        Optional<HiveBucket> bucket = ((HivePartition) partition).getBucket();

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
                bucket,
                maxSplitSize,
                maxOutstandingSplits,
                maxSplitIteratorThreads,
                hdfsEnvironment,
                executor,
                maxPartitionBatchSize);
    }

    private Iterable<org.apache.hadoop.hive.metastore.api.Partition> getPartitions(final Table table, final SchemaTableName tableName, List<String> partitionNames)
            throws NoSuchObjectException
    {
        if (partitionNames.equals(ImmutableList.of(UNPARTITIONED_ID))) {
            return ImmutableList.of(UNPARTITIONED_PARTITION);
        }

        Iterable<List<String>> partitionNameBatches = partitionExponentially(partitionNames, minPartitionBatchSize, maxPartitionBatchSize);
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
                    catch (MetaException | RuntimeException e) {
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
        return new HiveRecordSet(hdfsEnvironment, (HiveSplit) split, hiveColumns, HiveRecordCursorProviders.getDefaultProviders());
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

    private static Function<String, HivePartition> toPartition(
            final SchemaTableName tableName,
            final Map<String, ColumnHandle> columnsByName,
            final Optional<HiveBucket> bucket)
    {
        return new Function<String, HivePartition>()
        {
            @Override
            public HivePartition apply(String partitionId)
            {
                try {
                    if (partitionId.equals(UNPARTITIONED_ID)) {
                        return new HivePartition(tableName);
                    }

                    LinkedHashMap<String, String> keys = Warehouse.makeSpecFromName(partitionId);
                    ImmutableMap.Builder<ColumnHandle, Comparable<?>> builder = ImmutableMap.builder();
                    for (Entry<String, String> entry : keys.entrySet()) {
                        ColumnHandle columnHandle = columnsByName.get(entry.getKey());
                        checkArgument(columnHandle != null, "Invalid partition key %s in partition %s", entry.getKey(), partitionId);
                        checkArgument(columnHandle instanceof HiveColumnHandle, "columnHandle is not an instance of HiveColumnHandle");
                        HiveColumnHandle hiveColumnHandle = (HiveColumnHandle) columnHandle;

                        String value = entry.getValue();
                        switch (hiveColumnHandle.getType()) {
                            case BOOLEAN:
                                if (value.isEmpty()) {
                                    builder.put(columnHandle, false);
                                }
                                else {
                                    builder.put(columnHandle, parseBoolean(value));
                                }
                                break;
                            case LONG:
                                if (value.isEmpty()) {
                                    builder.put(columnHandle, 0L);
                                }
                                else if (hiveColumnHandle.getHiveType() == HiveType.TIMESTAMP) {
                                    builder.put(columnHandle, parseHiveTimestamp(value));
                                }
                                else {
                                    builder.put(columnHandle, parseLong(value));
                                }
                                break;
                            case DOUBLE:
                                if (value.isEmpty()) {
                                    builder.put(columnHandle, 0.0);
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

                    return new HivePartition(tableName, partitionId, builder.build(), bucket);
                }
                catch (MetaException e) {
                    // invalid partition id
                    throw Throwables.propagate(e);
                }
            }
        };
    }

    public static Predicate<HivePartition> partitionMatches(final TupleDomain tupleDomain)
    {
        return new Predicate<HivePartition>()
        {
            @Override
            public boolean apply(HivePartition partition)
            {
                if (tupleDomain.isNone()) {
                    return false;
                }
                for (Entry<ColumnHandle, Comparable<?>> entry : partition.getKeys().entrySet()) {
                    Domain allowedDomain = tupleDomain.getDomains().get(entry.getKey());
                    if (allowedDomain != null && !allowedDomain.includesValue(entry.getValue())) {
                        return false;
                    }
                }
                return true;
            }
        };
    }

    /**
     * Partition the given list in exponentially (power of 2) increasing batch sizes starting at 1 up to maxBatchSize
     */
    private static <T> Iterable<List<T>> partitionExponentially(final List<T> values, final int minBatchSize, final int maxBatchSize)
    {
        return new Iterable<List<T>>()
        {
            @Override
            public Iterator<List<T>> iterator()
            {
                return new AbstractIterator<List<T>>()
                {
                    private int currentSize = minBatchSize;
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
}
