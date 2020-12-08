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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.Subfield.NestedField;
import com.facebook.presto.common.Subfield.PathElement;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HiveSplit.BucketConversion;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
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

import static com.facebook.presto.hive.HiveBucketing.getHiveBucketFilter;
import static com.facebook.presto.hive.HiveCoercer.createCoercer;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.AGGREGATED;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.hive.HiveColumnHandle.isPushedDownSubfield;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static com.facebook.presto.hive.HivePageSourceProvider.ColumnMapping.toColumnHandles;
import static com.facebook.presto.hive.HiveUtil.getPrefilledColumnValue;
import static com.facebook.presto.hive.HiveUtil.parsePartitionValue;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveSchema;
import static com.facebook.presto.hive.metastore.MetastoreUtil.reconstructPartitionSchema;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.lang.String.format;
import static java.lang.System.identityHashCode;
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
    private final LoadingCache<RowExpressionCacheKey, RowExpression> optimizedRowExpressionCache;

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
        this.optimizedRowExpressionCache = CacheBuilder.newBuilder()
                .recordStats()
                .maximumSize(10_000)
                .build(CacheLoader.from(cacheKey -> rowExpressionService.getExpressionOptimizer().optimize(cacheKey.rowExpression, OPTIMIZED, cacheKey.session)));
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableLayoutHandle layout,
            List<ColumnHandle> columns,
            SplitContext splitContext)
    {
        HiveTableLayoutHandle hiveLayout = (HiveTableLayoutHandle) layout;

        List<HiveColumnHandle> selectedColumns = columns.stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toList());

        HiveSplit hiveSplit = (HiveSplit) split;
        Path path = new Path(hiveSplit.getPath());

        Configuration configuration = hdfsEnvironment.getConfiguration(
                new HdfsContext(
                        session,
                        hiveSplit.getDatabase(),
                        hiveSplit.getTable(),
                        hiveLayout.getTablePath(),
                        false),
                path);

        Optional<EncryptionInformation> encryptionInformation = hiveSplit.getEncryptionInformation();
        if (hiveLayout.isPushdownFilterEnabled()) {
            Optional<ConnectorPageSource> selectivePageSource = createSelectivePageSource(
                    selectivePageSourceFactories,
                    configuration,
                    session,
                    hiveSplit,
                    hiveLayout,
                    selectedColumns,
                    hiveStorageTimeZone,
                    typeManager,
                    optimizedRowExpressionCache,
                    splitContext,
                    encryptionInformation);
            if (selectivePageSource.isPresent()) {
                return selectivePageSource.get();
            }
        }

        TupleDomain<HiveColumnHandle> effectivePredicate = hiveLayout.getDomainPredicate()
                .transform(Subfield::getRootName)
                .transform(hiveLayout.getPredicateColumns()::get);

        if (shouldSkipBucket(hiveLayout, hiveSplit, splitContext)) {
            return new HiveEmptySplitPageSource();
        }

        if (shouldSkipPartition(typeManager, hiveLayout, hiveStorageTimeZone, hiveSplit, splitContext)) {
            return new HiveEmptySplitPageSource();
        }

        CacheQuota cacheQuota = generateCacheQuota(hiveSplit);
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
                splitContext.getDynamicFilterPredicate().map(filter -> filter.transform(handle -> (HiveColumnHandle) handle).intersect(effectivePredicate)).orElse(effectivePredicate),
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
                new HiveFileContext(splitContext.isCacheable(), cacheQuota, hiveSplit.getExtraFileInfo().map(BinaryExtraHiveFileInfo::new), Optional.of(hiveSplit.getFileSize())),
                hiveLayout.getRemainingPredicate(),
                hiveLayout.isPushdownFilterEnabled(),
                rowExpressionService,
                encryptionInformation,
                hiveSplit.getCustomSplitInfo());
        if (pageSource.isPresent()) {
            return pageSource.get();
        }
        throw new IllegalStateException("Could not find a file reader for split " + hiveSplit);
    }

    @VisibleForTesting
    protected static CacheQuota generateCacheQuota(HiveSplit hiveSplit)
    {
        Optional<DataSize> quota = hiveSplit.getCacheQuotaRequirement().getQuota();
        switch (hiveSplit.getCacheQuotaRequirement().getCacheQuotaScope()) {
            case GLOBAL:
                return new CacheQuota(".", quota);
            case SCHEMA:
                return new CacheQuota(hiveSplit.getDatabase(), quota);
            case TABLE:
                return new CacheQuota(hiveSplit.getDatabase() + "." + hiveSplit.getTable(), quota);
            case PARTITION:
                return new CacheQuota(hiveSplit.getDatabase() + "." + hiveSplit.getTable() + "." + hiveSplit.getPartitionName(), quota);
            default:
                throw new PrestoException(HIVE_UNKNOWN_ERROR, format("%s is not supported", quota));
        }
    }

    private static Optional<ConnectorPageSource> createSelectivePageSource(
            Set<HiveSelectivePageSourceFactory> selectivePageSourceFactories,
            Configuration configuration,
            ConnectorSession session,
            HiveSplit split,
            HiveTableLayoutHandle layout,
            List<HiveColumnHandle> columns,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            LoadingCache<RowExpressionCacheKey, RowExpression> rowExpressionCache,
            SplitContext splitContext,
            Optional<EncryptionInformation> encryptionInformation)
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

        RowExpression optimizedRemainingPredicate = rowExpressionCache.getUnchecked(new RowExpressionCacheKey(layout.getRemainingPredicate(), session));

        if (shouldSkipBucket(layout, split, splitContext)) {
            return Optional.of(new HiveEmptySplitPageSource());
        }

        if (shouldSkipPartition(typeManager, layout, hiveStorageTimeZone, split, splitContext)) {
            return Optional.of(new HiveEmptySplitPageSource());
        }

        CacheQuota cacheQuota = generateCacheQuota(split);
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
                    splitContext.getDynamicFilterPredicate().map(filter -> filter.transform(
                            handle -> new Subfield(((HiveColumnHandle) handle).getName())).intersect(layout.getDomainPredicate())).orElse(layout.getDomainPredicate()),
                    optimizedRemainingPredicate,
                    hiveStorageTimeZone,
                    new HiveFileContext(splitContext.isCacheable(), cacheQuota, split.getExtraFileInfo().map(BinaryExtraHiveFileInfo::new), Optional.of(split.getFileSize())),
                    encryptionInformation);
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
            HiveFileContext hiveFileContext,
            RowExpression remainingPredicate,
            boolean isPushdownFilterEnabled,
            RowExpressionService rowExpressionService,
            Optional<EncryptionInformation> encryptionInformation,
            Map<String, String> customSplitInfo)
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

        boolean useRecordReaderFromInputFormat = HiveUtil.shouldUseRecordReaderFromInputFormat(configuration, storage,
                customSplitInfo);
        if (!useRecordReaderFromInputFormat) {
            for (HiveBatchPageSourceFactory pageSourceFactory : pageSourceFactories) {
                Optional<? extends ConnectorPageSource> pageSource = pageSourceFactory.createPageSource(
                        configuration,
                        session,
                        path,
                        start,
                        length,
                        fileSize,
                        storage,
                        tableName,
                        tableParameters,
                        toColumnHandles(regularAndInterimColumnMappings, true),
                        effectivePredicate,
                        hiveStorageTimeZone,
                        hiveFileContext,
                        encryptionInformation);
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
        }

        if (!hiveColumns.isEmpty() && hiveColumns.stream().allMatch(hiveColumnHandle -> hiveColumnHandle.getColumnType() == AGGREGATED)) {
            throw new UnsupportedOperationException("Partial aggregation pushdown only supported for ORC/Parquet files. " +
                    "Table " + tableName.toString() + " has file (" + path.toString() + ") of format " + storage.getStorageFormat().getOutputFormat() +
                    ". Set session property hive.pushdown_partial_aggregations_into_scan=false and execute query again");
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
                    s3SelectPushdownEnabled,
                    customSplitInfo);

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

    private static boolean shouldSkipBucket(HiveTableLayoutHandle hiveLayout, HiveSplit hiveSplit, SplitContext splitContext)
    {
        if (!splitContext.getDynamicFilterPredicate().isPresent()
                || !hiveSplit.getReadBucketNumber().isPresent()
                || !hiveSplit.getStorage().getBucketProperty().isPresent()) {
            return false;
        }

        TupleDomain<ColumnHandle> dynamicFilter = splitContext.getDynamicFilterPredicate().get();
        Optional<HiveBucketing.HiveBucketFilter> hiveBucketFilter = getHiveBucketFilter(hiveSplit.getStorage().getBucketProperty(), hiveLayout.getDataColumns(), dynamicFilter);

        return hiveBucketFilter.map(filter -> !filter.getBucketsToKeep().contains(hiveSplit.getReadBucketNumber().getAsInt())).orElse(false);
    }

    private static boolean shouldSkipPartition(TypeManager typeManager, HiveTableLayoutHandle hiveLayout, DateTimeZone hiveStorageTimeZone, HiveSplit hiveSplit, SplitContext splitContext)
    {
        List<HiveColumnHandle> partitionColumns = hiveLayout.getPartitionColumns();
        List<Type> partitionTypes = partitionColumns.stream()
                .map(column -> typeManager.getType(column.getTypeSignature()))
                .collect(toList());
        List<HivePartitionKey> partitionKeys = hiveSplit.getPartitionKeys();

        if (!splitContext.getDynamicFilterPredicate().isPresent()
                || hiveSplit.getPartitionKeys().isEmpty()
                || partitionColumns.isEmpty()
                || partitionColumns.size() != partitionKeys.size()) {
            return false;
        }

        TupleDomain<ColumnHandle> dynamicFilter = splitContext.getDynamicFilterPredicate().get();
        Map<ColumnHandle, Domain> domains = dynamicFilter.getDomains().get();
        for (int i = 0; i < partitionKeys.size(); i++) {
            Type type = partitionTypes.get(i);
            HivePartitionKey hivePartitionKey = partitionKeys.get(i);
            HiveColumnHandle hiveColumnHandle = partitionColumns.get(i);
            Domain allowedDomain = domains.get(hiveColumnHandle);
            NullableValue value = parsePartitionValue(hivePartitionKey.getName(), hivePartitionKey.getValue(), type, hiveStorageTimeZone);
            if (allowedDomain != null && !allowedDomain.includesNullableValue(value.getValue())) {
                return true;
            }
        }
        return false;
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

        public static ColumnMapping aggregated(HiveColumnHandle hiveColumnHandle, int index)
        {
            checkArgument(hiveColumnHandle.getColumnType() == AGGREGATED);
            // Pretend that it is a regular column so that the split manager can process it as normal
            return new ColumnMapping(ColumnMappingKind.REGULAR, hiveColumnHandle, Optional.empty(), OptionalInt.of(index), Optional.empty());
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
                else if (column.getColumnType() == AGGREGATED) {
                    columnMappings.add(aggregated(column, regularIndex));
                    regularIndex++;
                }
                else if (isPushedDownSubfield(column)) {
                    Optional<HiveType> coercionFromType = getHiveType(coercionFrom, getOnlyElement(column.getRequiredSubfields()));
                    HiveType coercionToType = column.getHiveType();
                    if (coercionFromType.isPresent() && coercionFromType.get().equals(coercionToType)) {
                        // In nested columns, if the resolved type is same as requested type don't add the coercion mapping
                        coercionFromType = Optional.empty();
                    }
                    ColumnMapping columnMapping = new ColumnMapping(ColumnMappingKind.REGULAR, column, Optional.empty(), OptionalInt.of(regularIndex), coercionFromType);
                    columnMappings.add(columnMapping);
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

        private static Optional<HiveType> getHiveType(Optional<HiveType> baseType, Subfield subfield)
        {
            List<PathElement> pathElements = subfield.getPath();
            ImmutableList.Builder<String> nestedColumnPathBuilder = ImmutableList.builder();
            for (PathElement pathElement : pathElements) {
                checkArgument(pathElement instanceof NestedField, "Unsupported subfield. Expected only nested path elements. " + subfield);
                nestedColumnPathBuilder.add(((NestedField) pathElement).getName());
            }
            return baseType.flatMap(type -> type.findChildType(nestedColumnPathBuilder.build()));
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
                                columnHandle.getRequiredSubfields(),
                                columnHandle.getPartialAggregation());
                    })
                    .collect(toList());
        }
    }

    public enum ColumnMappingKind
    {
        REGULAR,
        PREFILLED,
        INTERIM,
        AGGREGATED
    }

    private static final class RowExpressionCacheKey
    {
        private final RowExpression rowExpression;
        private final ConnectorSession session;

        RowExpressionCacheKey(RowExpression rowExpression, ConnectorSession session)
        {
            this.rowExpression = rowExpression;
            this.session = session;
        }

        @Override
        public int hashCode()
        {
            return identityHashCode(rowExpression);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            RowExpressionCacheKey other = (RowExpressionCacheKey) obj;
            return this.rowExpression == other.rowExpression;
        }
    }
}
