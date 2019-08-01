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
import com.facebook.presto.hive.HiveSplit.BucketConversion;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveCoercer.createCoercer;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.hive.HivePageSourceProvider.ColumnMapping.toColumnHandles;
import static com.facebook.presto.hive.HiveUtil.getPrefilledColumnValue;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveSchema;
import static com.facebook.presto.hive.metastore.MetastoreUtil.reconstructPartitionSchema;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HivePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final DateTimeZone hiveStorageTimeZone;
    private final HdfsEnvironment hdfsEnvironment;
    private final Set<HiveRecordCursorProvider> cursorProviders;
    private final Set<HiveBatchPageSourceFactory> pageSourceFactories;
    private final Set<HiveSelectivePageSourceFactory> selectivePageSourceFactories;
    private final TypeManager typeManager;
    private final RowExpressionService rowExpressionService;

    @Inject
    public HivePageSourceProvider(
            HiveClientConfig hiveClientConfig,
            HdfsEnvironment hdfsEnvironment,
            Set<HiveRecordCursorProvider> cursorProviders,
            Set<HiveBatchPageSourceFactory> pageSourceFactories,
            Set<HiveSelectivePageSourceFactory> selectivePageSourceFactories,
            TypeManager typeManager,
            RowExpressionService rowExpressionService)
    {
        requireNonNull(hiveClientConfig, "hiveClientConfig is null");
        this.hiveStorageTimeZone = hiveClientConfig.getDateTimeZone();
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.cursorProviders = ImmutableSet.copyOf(requireNonNull(cursorProviders, "cursorProviders is null"));
        this.pageSourceFactories = ImmutableSet.copyOf(requireNonNull(pageSourceFactories, "pageSourceFactories is null"));
        this.selectivePageSourceFactories = ImmutableSet.copyOf(requireNonNull(selectivePageSourceFactories, "selectivePageSourceFactories is null"));
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableLayoutHandle layout, List<ColumnHandle> columns)
    {
        HiveTableLayoutHandle hiveLayout = (HiveTableLayoutHandle) layout;

        List<HiveColumnHandle> selectedColumns = columns.stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toList());

        HiveSplit hiveSplit = (HiveSplit) split;
        Path path = new Path(hiveSplit.getPath());

        Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsContext(session, hiveSplit.getDatabase(), hiveSplit.getTable()), path);

        if (hiveLayout.isPushdownFilterEnabled()) {
            Optional<ConnectorPageSource> selectivePageSource = createSelectivePageSource(selectivePageSourceFactories, configuration, session, hiveSplit, hiveLayout, selectedColumns, hiveStorageTimeZone, rowExpressionService, typeManager);
            if (selectivePageSource.isPresent()) {
                return selectivePageSource.get();
            }
        }

        Optional<ConnectorPageSource> pageSource = createHivePageSource(
                cursorProviders,
                pageSourceFactories,
                configuration,
                session,
                path,
                hiveSplit.getTableBucketNumber(),
                hiveSplit.getStart(),
                hiveSplit.getLength(),
                hiveSplit.getFileSize(),
                hiveSplit.getStorage(),
                hiveLayout.getDomainPredicate()
                        .transform(Subfield::getRootName)
                        .transform(hiveLayout.getPredicateColumns()::get),
                selectedColumns,
                hiveLayout.getPredicateColumns(),
                hiveSplit.getPartitionKeys(),
                hiveStorageTimeZone,
                typeManager,
                hiveLayout.getSchemaTableName(),
                hiveLayout.getPartitionColumns(),
                hiveLayout.getDataColumns(),
                hiveLayout.getTableParameters(),
                hiveSplit.getPartitionDataColumnCount(),
                hiveSplit.getPartitionSchemaDifference(),
                hiveSplit.getBucketConversion(),
                hiveSplit.isS3SelectPushdownEnabled(),
                hiveSplit.getExtraFileInfo(),
                hiveLayout.getRemainingPredicate(),
                hiveLayout.isPushdownFilterEnabled(),
                rowExpressionService);
        if (pageSource.isPresent()) {
            return pageSource.get();
        }
        throw new IllegalStateException("Could not find a file reader for split " + hiveSplit);
    }

    private static Optional<ConnectorPageSource> createSelectivePageSource(
            Set<HiveSelectivePageSourceFactory> selectivePageSourceFactories,
            Configuration configuration,
            ConnectorSession session,
            HiveSplit split,
            HiveTableLayoutHandle layout,
            List<HiveColumnHandle> columns,
            DateTimeZone hiveStorageTimeZone,
            RowExpressionService rowExpressionService,
            TypeManager typeManager)
    {
        Set<HiveColumnHandle> interimColumns = ImmutableSet.<HiveColumnHandle>builder()
                .addAll(layout.getPredicateColumns().values())
                .addAll(split.getBucketConversion().map(BucketConversion::getBucketColumnHandles).orElse(ImmutableList.of()))
                .build();

        Set<String> columnNames = columns.stream().map(HiveColumnHandle::getName).collect(toImmutableSet());

        List<HiveColumnHandle> allColumns = ImmutableList.<HiveColumnHandle>builder()
                .addAll(columns)
                .addAll(interimColumns.stream().filter(column -> !columnNames.contains(column.getName())).collect(toImmutableList()))
                .build();

        Path path = new Path(split.getPath());
        List<ColumnMapping> columnMappings = ColumnMapping.buildColumnMappings(
                split.getPartitionKeys(),
                allColumns,
                ImmutableList.of(),
                split.getPartitionSchemaDifference(),
                path,
                split.getTableBucketNumber());

        Optional<BucketAdaptation> bucketAdaptation = split.getBucketConversion().map(conversion -> toBucketAdaptation(conversion, columnMappings, split.getTableBucketNumber(), mapping -> mapping.getHiveColumnHandle().getHiveColumnIndex()));

        Map<Integer, String> prefilledValues = columnMappings.stream()
                .filter(mapping -> mapping.getKind() == ColumnMappingKind.PREFILLED)
                .collect(toImmutableMap(mapping -> mapping.getHiveColumnHandle().getHiveColumnIndex(), ColumnMapping::getPrefilledValue));

        Map<Integer, HiveCoercer> coercers = columnMappings.stream()
                .filter(mapping -> mapping.getCoercionFrom().isPresent())
                .collect(toImmutableMap(
                        mapping -> mapping.getHiveColumnHandle().getHiveColumnIndex(),
                        mapping -> createCoercer(typeManager, mapping.getCoercionFrom().get(), mapping.getHiveColumnHandle().getHiveType())));

        List<Integer> outputColumns = columns.stream()
                .map(HiveColumnHandle::getHiveColumnIndex)
                .collect(toImmutableList());

        RowExpression optimizedRemainingPredicate = rowExpressionService.getExpressionOptimizer().optimize(layout.getRemainingPredicate(), OPTIMIZED, session);

        for (HiveSelectivePageSourceFactory pageSourceFactory : selectivePageSourceFactories) {
            Optional<? extends ConnectorPageSource> pageSource = pageSourceFactory.createPageSource(
                    configuration,
                    session,
                    path,
                    split.getStart(),
                    split.getLength(),
                    split.getFileSize(),
                    split.getStorage(),
                    toColumnHandles(columnMappings, true),
                    prefilledValues,
                    coercers,
                    bucketAdaptation,
                    outputColumns,
                    layout.getDomainPredicate(),
                    optimizedRemainingPredicate,
                    hiveStorageTimeZone,
                    split.getExtraFileInfo());
            if (pageSource.isPresent()) {
                return Optional.of(pageSource.get());
            }
        }

        return Optional.empty();
    }

    public static Optional<ConnectorPageSource> createHivePageSource(
            Set<HiveRecordCursorProvider> cursorProviders,
            Set<HiveBatchPageSourceFactory> pageSourceFactories,
            Configuration configuration,
            ConnectorSession session,
            Path path,
            OptionalInt tableBucketNumber,
            long start,
            long length,
            long fileSize,
            Storage storage,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            List<HiveColumnHandle> hiveColumns,
            Map<String, HiveColumnHandle> predicateColumns,
            List<HivePartitionKey> partitionKeys,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            SchemaTableName tableName,
            List<HiveColumnHandle> partitionKeyColumnHandles,
            List<Column> tableDataColumns,
            Map<String, String> tableParameters,
            int partitionDataColumnCount,
            Map<Integer, Column> partitionSchemaDifference,
            Optional<BucketConversion> bucketConversion,
            boolean s3SelectPushdownEnabled,
            Optional<byte[]> extraFileInfo,
            RowExpression remainingPredicate,
            boolean isPushdownFilterEnabled,
            RowExpressionService rowExpressionService)
    {
        List<HiveColumnHandle> allColumns;

        if (isPushdownFilterEnabled) {
            Set<String> columnNames = hiveColumns.stream().map(HiveColumnHandle::getName).collect(toImmutableSet());
            List<HiveColumnHandle> additionalColumns = predicateColumns.values().stream()
                    .filter(column -> !columnNames.contains(column.getName()))
                    .collect(toImmutableList());

            allColumns = ImmutableList.<HiveColumnHandle>builder()
                    .addAll(hiveColumns)
                    .addAll(additionalColumns)
                    .build();
        }
        else {
            allColumns = hiveColumns;
        }

        List<ColumnMapping> columnMappings = ColumnMapping.buildColumnMappings(
                partitionKeys,
                allColumns,
                bucketConversion.map(BucketConversion::getBucketColumnHandles).orElse(ImmutableList.of()),
                partitionSchemaDifference,
                path,
                tableBucketNumber);

        Set<Integer> outputIndices = hiveColumns.stream()
                .map(HiveColumnHandle::getHiveColumnIndex)
                .collect(toImmutableSet());

        List<ColumnMapping> regularAndInterimColumnMappings = ColumnMapping.extractRegularAndInterimColumnMappings(columnMappings);

        Optional<BucketAdaptation> bucketAdaptation = bucketConversion.map(conversion -> toBucketAdaptation(conversion, regularAndInterimColumnMappings, tableBucketNumber, ColumnMapping::getIndex));

        for (HiveBatchPageSourceFactory pageSourceFactory : pageSourceFactories) {
            Optional<? extends ConnectorPageSource> pageSource = pageSourceFactory.createPageSource(
                    configuration,
                    session,
                    path,
                    start,
                    length,
                    fileSize,
                    storage,
                    tableParameters,
                    toColumnHandles(regularAndInterimColumnMappings, true),
                    effectivePredicate,
                    hiveStorageTimeZone,
                    extraFileInfo);
            if (pageSource.isPresent()) {
                HivePageSource hivePageSource = new HivePageSource(
                        columnMappings,
                        bucketAdaptation,
                        hiveStorageTimeZone,
                        typeManager,
                        pageSource.get());

                if (isPushdownFilterEnabled) {
                    return Optional.of(new FilteringPageSource(
                            columnMappings,
                            effectivePredicate,
                            remainingPredicate,
                            typeManager,
                            rowExpressionService,
                            session,
                            outputIndices,
                            hivePageSource));
                }
                return Optional.of(hivePageSource);
            }
        }

        for (HiveRecordCursorProvider provider : cursorProviders) {
            // GenericHiveRecordCursor will automatically do the coercion without HiveCoercionRecordCursor
            boolean doCoercion = !(provider instanceof GenericHiveRecordCursorProvider);

            List<Column> partitionDataColumns = reconstructPartitionSchema(tableDataColumns, partitionDataColumnCount, partitionSchemaDifference);
            List<Column> partitionKeyColumns = partitionKeyColumnHandles.stream()
                    .map(handle -> new Column(handle.getName(), handle.getHiveType(), handle.getComment()))
                    .collect(toImmutableList());

            Properties schema = getHiveSchema(
                    storage,
                    partitionDataColumns,
                    tableDataColumns,
                    tableParameters,
                    tableName.getSchemaName(),
                    tableName.getTableName(),
                    partitionKeyColumns);

            Optional<RecordCursor> cursor = provider.createRecordCursor(
                    configuration,
                    session,
                    path,
                    start,
                    length,
                    fileSize,
                    schema,
                    toColumnHandles(regularAndInterimColumnMappings, doCoercion),
                    effectivePredicate,
                    hiveStorageTimeZone,
                    typeManager,
                    s3SelectPushdownEnabled);

            if (cursor.isPresent()) {
                RecordCursor delegate = cursor.get();

                if (bucketAdaptation.isPresent()) {
                    delegate = new HiveBucketAdapterRecordCursor(
                            bucketAdaptation.get().getBucketColumnIndices(),
                            bucketAdaptation.get().getBucketColumnHiveTypes(),
                            bucketAdaptation.get().getTableBucketCount(),
                            bucketAdaptation.get().getPartitionBucketCount(),
                            bucketAdaptation.get().getBucketToKeep(),
                            typeManager,
                            delegate);
                }

                // Need to wrap RcText and RcBinary into a wrapper, which will do the coercion for mismatch columns
                if (doCoercion) {
                    delegate = new HiveCoercionRecordCursor(regularAndInterimColumnMappings, typeManager, delegate);
                }

                HiveRecordCursor hiveRecordCursor = new HiveRecordCursor(
                        columnMappings,
                        hiveStorageTimeZone,
                        typeManager,
                        delegate);
                List<Type> columnTypes = allColumns.stream()
                        .map(input -> typeManager.getType(input.getTypeSignature()))
                        .collect(toList());

                RecordPageSource recordPageSource = new RecordPageSource(columnTypes, hiveRecordCursor);
                if (isPushdownFilterEnabled) {
                    return Optional.of(new FilteringPageSource(
                            columnMappings,
                            effectivePredicate,
                            remainingPredicate,
                            typeManager,
                            rowExpressionService,
                            session,
                            outputIndices,
                            recordPageSource));
                }
                return Optional.of(recordPageSource);
            }
        }

        return Optional.empty();
    }

    private static BucketAdaptation toBucketAdaptation(BucketConversion conversion, List<ColumnMapping> columnMappings, OptionalInt tableBucketNumber, Function<ColumnMapping, Integer> bucketColumnIndexProducer)
    {
        Map<Integer, ColumnMapping> hiveIndexToBlockIndex = uniqueIndex(columnMappings, columnMapping -> columnMapping.getHiveColumnHandle().getHiveColumnIndex());
        int[] bucketColumnIndices = conversion.getBucketColumnHandles().stream()
                .map(HiveColumnHandle::getHiveColumnIndex)
                .map(hiveIndexToBlockIndex::get)
                .mapToInt(bucketColumnIndexProducer::apply)
                .toArray();

        List<HiveType> bucketColumnHiveTypes = conversion.getBucketColumnHandles().stream()
                .map(HiveColumnHandle::getHiveColumnIndex)
                .map(hiveIndexToBlockIndex::get)
                .map(ColumnMapping::getHiveColumnHandle)
                .map(HiveColumnHandle::getHiveType)
                .collect(toImmutableList());
        return new BucketAdaptation(bucketColumnIndices, bucketColumnHiveTypes, conversion.getTableBucketCount(), conversion.getPartitionBucketCount(), tableBucketNumber.getAsInt());
    }

    public static class ColumnMapping
    {
        private final ColumnMappingKind kind;
        private final HiveColumnHandle hiveColumnHandle;
        private final Optional<String> prefilledValue;
        /**
         * ordinal of this column in the underlying page source or record cursor
         */
        private final OptionalInt index;
        private final Optional<HiveType> coercionFrom;

        public static ColumnMapping regular(HiveColumnHandle hiveColumnHandle, int index, Optional<HiveType> coerceFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == REGULAR);
            return new ColumnMapping(ColumnMappingKind.REGULAR, hiveColumnHandle, Optional.empty(), OptionalInt.of(index), coerceFrom);
        }

        public static ColumnMapping prefilled(HiveColumnHandle hiveColumnHandle, String prefilledValue, Optional<HiveType> coerceFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == PARTITION_KEY || hiveColumnHandle.getColumnType() == SYNTHESIZED);
            return new ColumnMapping(ColumnMappingKind.PREFILLED, hiveColumnHandle, Optional.of(prefilledValue), OptionalInt.empty(), coerceFrom);
        }

        public static ColumnMapping interim(HiveColumnHandle hiveColumnHandle, int index)
        {
            checkArgument(hiveColumnHandle.getColumnType() == REGULAR);
            return new ColumnMapping(ColumnMappingKind.INTERIM, hiveColumnHandle, Optional.empty(), OptionalInt.of(index), Optional.empty());
        }

        private ColumnMapping(ColumnMappingKind kind, HiveColumnHandle hiveColumnHandle, Optional<String> prefilledValue, OptionalInt index, Optional<HiveType> coerceFrom)
        {
            this.kind = requireNonNull(kind, "kind is null");
            this.hiveColumnHandle = requireNonNull(hiveColumnHandle, "hiveColumnHandle is null");
            this.prefilledValue = requireNonNull(prefilledValue, "prefilledValue is null");
            this.index = requireNonNull(index, "index is null");
            this.coercionFrom = requireNonNull(coerceFrom, "coerceFrom is null");
        }

        public ColumnMappingKind getKind()
        {
            return kind;
        }

        public String getPrefilledValue()
        {
            checkState(kind == ColumnMappingKind.PREFILLED);
            return prefilledValue.get();
        }

        public HiveColumnHandle getHiveColumnHandle()
        {
            return hiveColumnHandle;
        }

        public int getIndex()
        {
            checkState(kind == ColumnMappingKind.REGULAR || kind == ColumnMappingKind.INTERIM);
            return index.getAsInt();
        }

        public Optional<HiveType> getCoercionFrom()
        {
            return coercionFrom;
        }

        /**
         * @param columns columns that need to be returned to engine
         * @param requiredInterimColumns columns that are needed for processing, but shouldn't be returned to engine (may overlaps with columns)
         * @param partitionSchemaDifference map from hive column index to hive type
         * @param bucketNumber empty if table is not bucketed, a number within [0, # bucket in table) otherwise
         */
        public static List<ColumnMapping> buildColumnMappings(
                List<HivePartitionKey> partitionKeys,
                List<HiveColumnHandle> columns,
                List<HiveColumnHandle> requiredInterimColumns,
                Map<Integer, Column> partitionSchemaDifference,
                Path path,
                OptionalInt bucketNumber)
        {
            Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(partitionKeys, HivePartitionKey::getName);
            int regularIndex = 0;
            Set<Integer> regularColumnIndices = new HashSet<>();
            ImmutableList.Builder<ColumnMapping> columnMappings = ImmutableList.builder();
            for (HiveColumnHandle column : columns) {
                // will be present if the partition has a different schema (column type, column name) for the column
                Optional<Column> partitionColumn = Optional.ofNullable(partitionSchemaDifference.get(column.getHiveColumnIndex()));
                Optional<HiveType> coercionFrom = Optional.empty();
                // we don't care if only the column name has changed
                if (partitionColumn.isPresent() && !partitionColumn.get().getType().equals(column.getHiveType())) {
                    coercionFrom = Optional.of(partitionColumn.get().getType());
                }

                if (column.getColumnType() == REGULAR) {
                    checkArgument(regularColumnIndices.add(column.getHiveColumnIndex()), "duplicate hiveColumnIndex in columns list");
                    columnMappings.add(regular(column, regularIndex, coercionFrom));
                    regularIndex++;
                }
                else {
                    columnMappings.add(prefilled(
                            column,
                            getPrefilledColumnValue(column, partitionKeysByName.get(column.getName()), path, bucketNumber),
                            coercionFrom));
                }
            }
            for (HiveColumnHandle column : requiredInterimColumns) {
                checkArgument(column.getColumnType() == REGULAR);
                if (regularColumnIndices.contains(column.getHiveColumnIndex())) {
                    continue; // This column exists in columns. Do not add it again.
                }
                // If coercion does not affect bucket number calculation, coercion doesn't need to be applied here.
                // Otherwise, read of this partition should not be allowed.
                // (Alternatively, the partition could be read as an unbucketed partition. This is not implemented.)
                columnMappings.add(interim(column, regularIndex));
                regularIndex++;
            }
            return columnMappings.build();
        }

        public static List<ColumnMapping> extractRegularAndInterimColumnMappings(List<ColumnMapping> columnMappings)
        {
            return columnMappings.stream()
                    .filter(columnMapping -> columnMapping.getKind() == ColumnMappingKind.REGULAR || columnMapping.getKind() == ColumnMappingKind.INTERIM)
                    .collect(toImmutableList());
        }

        public static List<HiveColumnHandle> toColumnHandles(List<ColumnMapping> regularColumnMappings, boolean doCoercion)
        {
            return regularColumnMappings.stream()
                    .map(columnMapping -> {
                        HiveColumnHandle columnHandle = columnMapping.getHiveColumnHandle();
                        if (!doCoercion || !columnMapping.getCoercionFrom().isPresent()) {
                            return columnHandle;
                        }
                        return new HiveColumnHandle(
                                columnHandle.getName(),
                                columnMapping.getCoercionFrom().get(),
                                columnMapping.getCoercionFrom().get().getTypeSignature(),
                                columnHandle.getHiveColumnIndex(),
                                columnHandle.getColumnType(),
                                Optional.empty(),
                                columnHandle.getRequiredSubfields());
                    })
                    .collect(toList());
        }
    }

    public enum ColumnMappingKind
    {
        REGULAR,
        PREFILLED,
        INTERIM,
    }
}
