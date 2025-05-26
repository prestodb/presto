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
package com.facebook.presto.hive.orc;

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.predicate.FilterFunction;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.TupleDomainFilter;
import com.facebook.presto.common.relation.Predicate;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.expressions.DynamicFilters.DynamicFilterExtractResult;
import com.facebook.presto.hive.BucketAdaptation;
import com.facebook.presto.hive.EncryptionInformation;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveCoercer;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.HiveFileSplit;
import com.facebook.presto.hive.HiveOrcAggregatedMemoryContext;
import com.facebook.presto.hive.HiveSelectivePageSourceFactory;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.SubfieldExtractor;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.orc.DwrfEncryptionProvider;
import com.facebook.presto.orc.OrcAggregatedMemoryContext;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcReaderOptions;
import com.facebook.presto.orc.OrcSelectiveRecordReader;
import com.facebook.presto.orc.StripeMetadataSourceFactory;
import com.facebook.presto.orc.TupleDomainOrcPredicate;
import com.facebook.presto.orc.cache.OrcFileTailSource;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import jakarta.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.expressions.DynamicFilters.extractDynamicFilters;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.binaryExpression;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.expressions.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveBucketing.getHiveBucket;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcMaxReadBlockSize;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcTinyStripeThreshold;
import static com.facebook.presto.hive.HiveCommonSessionProperties.isOrcBloomFiltersEnabled;
import static com.facebook.presto.hive.HiveCommonSessionProperties.isOrcZstdJniDecompressionEnabled;
import static com.facebook.presto.hive.HiveCommonSessionProperties.isUseOrcColumnNames;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static com.facebook.presto.hive.HiveSessionProperties.isAdaptiveFilterReorderingEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isLegacyTimestampBucketing;
import static com.facebook.presto.hive.HiveUtil.getPhysicalHiveColumnHandles;
import static com.facebook.presto.hive.HiveUtil.typedPartitionKey;
import static com.facebook.presto.hive.MetadataUtils.isEntireColumn;
import static com.facebook.presto.hive.orc.OrcPageSourceFactoryUtils.getOrcDataSource;
import static com.facebook.presto.hive.orc.OrcPageSourceFactoryUtils.getOrcReader;
import static com.facebook.presto.hive.orc.OrcPageSourceFactoryUtils.mapToPrestoException;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableBiMap.toImmutableBiMap;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OrcSelectivePageSourceFactory
        implements HiveSelectivePageSourceFactory
{
    private final TypeManager typeManager;
    private final StandardFunctionResolution functionResolution;
    private final RowExpressionService rowExpressionService;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final int domainCompactionThreshold;
    private final OrcFileTailSource orcFileTailSource;
    private final StripeMetadataSourceFactory stripeMetadataSourceFactory;
    private final TupleDomainFilterCache tupleDomainFilterCache;

    @Inject
    public OrcSelectivePageSourceFactory(
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            HiveClientConfig config,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSourceFactory stripeMetadataSourceFactory,
            TupleDomainFilterCache tupleDomainFilterCache)
    {
        this(
                typeManager,
                functionResolution,
                rowExpressionService,
                hdfsEnvironment,
                stats,
                config.getDomainCompactionThreshold(),
                orcFileTailSource,
                stripeMetadataSourceFactory,
                tupleDomainFilterCache);
    }

    public OrcSelectivePageSourceFactory(
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            int domainCompactionThreshold,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSourceFactory stripeMetadataSourceFactory,
            TupleDomainFilterCache tupleDomainFilterCache)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.domainCompactionThreshold = domainCompactionThreshold;
        this.orcFileTailSource = requireNonNull(orcFileTailSource, "orcFileTailCache is null");
        this.stripeMetadataSourceFactory = requireNonNull(stripeMetadataSourceFactory, "stripeMetadataSourceFactory is null");
        this.tupleDomainFilterCache = requireNonNull(tupleDomainFilterCache, "tupleDomainFilterCache is null");
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            HiveFileSplit fileSplit,
            Storage storage,
            List<HiveColumnHandle> selectedColumns,
            Map<Integer, String> prefilledValues,
            Map<Integer, HiveCoercer> coercers,
            Optional<BucketAdaptation> bucketAdaptation,
            List<Integer> outputColumns,
            TupleDomain<Subfield> domainPredicate,
            RowExpression remainingPredicate,
            DateTimeZone hiveStorageTimeZone,
            HiveFileContext hiveFileContext,
            Optional<EncryptionInformation> encryptionInformation,
            boolean appendRowNumberEnabled,
            Optional<byte[]> rowIDPartitionComponent)
    {
        if (!OrcSerde.class.getName().equals(storage.getStorageFormat().getSerDe())) {
            return Optional.empty();
        }

        // per HIVE-13040 and ORC-162, empty files are allowed
        if (fileSplit.getFileSize() == 0) {
            return Optional.of(new FixedPageSource(ImmutableList.of()));
        }

        return Optional.of(createOrcPageSource(
                session,
                ORC,
                hdfsEnvironment,
                configuration,
                fileSplit,
                selectedColumns,
                prefilledValues,
                coercers,
                bucketAdaptation,
                outputColumns,
                domainPredicate,
                remainingPredicate,
                isUseOrcColumnNames(session),
                hiveStorageTimeZone,
                typeManager,
                functionResolution,
                rowExpressionService,
                isOrcBloomFiltersEnabled(session),
                stats,
                domainCompactionThreshold,
                orcFileTailSource,
                stripeMetadataSourceFactory,
                hiveFileContext,
                tupleDomainFilterCache,
                encryptionInformation,
                NO_ENCRYPTION,
                appendRowNumberEnabled,
                rowIDPartitionComponent));
    }

    public static ConnectorPageSource createOrcPageSource(
            ConnectorSession session,
            OrcEncoding orcEncoding,
            HdfsEnvironment hdfsEnvironment,
            Configuration configuration,
            HiveFileSplit fileSplit,
            List<HiveColumnHandle> selectedColumns,
            Map<Integer, String> prefilledValues,
            Map<Integer, HiveCoercer> coercers,
            Optional<BucketAdaptation> bucketAdaptation,
            List<Integer> outputColumns,
            TupleDomain<Subfield> domainPredicate,
            RowExpression remainingPredicate,
            boolean useOrcColumnNames,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            boolean orcBloomFiltersEnabled,
            FileFormatDataSourceStats stats,
            int domainCompactionThreshold,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSourceFactory stripeMetadataSourceFactory,
            HiveFileContext hiveFileContext,
            TupleDomainFilterCache tupleDomainFilterCache,
            Optional<EncryptionInformation> encryptionInformation,
            DwrfEncryptionProvider dwrfEncryptionProvider,
            boolean appendRowNumberEnabled,
            Optional<byte[]> rowIDPartitionComponent)
    {
        checkArgument(domainCompactionThreshold >= 1, "domainCompactionThreshold must be at least 1");

        OrcDataSource orcDataSource = getOrcDataSource(session, fileSplit, hdfsEnvironment, configuration, hiveFileContext, stats);
        Path path = new Path(fileSplit.getPath());

        boolean supplyRowIDs = selectedColumns.stream().anyMatch(column -> HiveColumnHandle.isRowIdColumnHandle(column));
        checkArgument(!supplyRowIDs || rowIDPartitionComponent.isPresent(), "rowIDPartitionComponent required when supplying row IDs");
        byte[] partitionID = rowIDPartitionComponent.orElseGet(() -> new byte[0]);
        String rowGroupId = path.getName();

        DataSize maxMergeDistance = getOrcMaxMergeDistance(session);
        DataSize tinyStripeThreshold = getOrcTinyStripeThreshold(session);
        DataSize maxReadBlockSize = getOrcMaxReadBlockSize(session);
        OrcReaderOptions orcReaderOptions = OrcReaderOptions.builder()
                .withMaxMergeDistance(maxMergeDistance)
                .withTinyStripeThreshold(tinyStripeThreshold)
                .withMaxBlockSize(maxReadBlockSize)
                .withZstdJniDecompressionEnabled(isOrcZstdJniDecompressionEnabled(session))
                .withAppendRowNumber(appendRowNumberEnabled || supplyRowIDs)
                .build();
        OrcAggregatedMemoryContext systemMemoryUsage = new HiveOrcAggregatedMemoryContext();
        try {
            checkArgument(!domainPredicate.isNone(), "Unexpected NONE domain");

            OrcReader reader = getOrcReader(
                    orcEncoding,
                    selectedColumns,
                    useOrcColumnNames,
                    orcFileTailSource,
                    stripeMetadataSourceFactory,
                    hiveFileContext,
                    orcReaderOptions,
                    encryptionInformation,
                    dwrfEncryptionProvider,
                    orcDataSource,
                    path);

            List<HiveColumnHandle> physicalColumns = getPhysicalHiveColumnHandles(selectedColumns, useOrcColumnNames, reader.getTypes(), path);

            Map<Integer, Integer> indexMapping = IntStream.range(0, selectedColumns.size())
                    .boxed()
                    .collect(toImmutableMap(i -> selectedColumns.get(i).getHiveColumnIndex(), i -> physicalColumns.get(i).getHiveColumnIndex()));

            Map<Integer, String> columnNames = physicalColumns.stream()
                    .collect(toImmutableMap(HiveColumnHandle::getHiveColumnIndex, HiveColumnHandle::getName));

            Map<Integer, HiveCoercer> mappedCoercers = coercers.entrySet().stream().collect(toImmutableMap(entry -> indexMapping.get(entry.getKey()), Map.Entry::getValue));

            OrcPredicate orcPredicate = toOrcPredicate(domainPredicate, physicalColumns, mappedCoercers, typeManager, domainCompactionThreshold, orcBloomFiltersEnabled);

            Map<String, Integer> columnIndices = ImmutableBiMap.copyOf(columnNames).inverse();
            Map<Integer, Map<Subfield, TupleDomainFilter>> tupleDomainFilters = toTupleDomainFilters(domainPredicate, columnIndices, mappedCoercers, tupleDomainFilterCache);

            List<Integer> outputIndices = outputColumns.stream().map(indexMapping::get).collect(toImmutableList());
            Map<Integer, List<Subfield>> requiredSubfields = collectRequiredSubfields(physicalColumns, outputIndices, tupleDomainFilters, remainingPredicate, columnIndices, functionResolution, rowExpressionService, session);

            Map<Integer, Type> columnTypes = physicalColumns.stream()
                    .collect(toImmutableMap(HiveColumnHandle::getHiveColumnIndex, column -> typeManager.getType(column.getTypeSignature())));

            Map<Integer, Object> typedPrefilledValues = Maps.transformEntries(
                    prefilledValues.entrySet().stream()
                            .collect(toImmutableMap(entry -> indexMapping.get(entry.getKey()), Map.Entry::getValue)),
                    (hiveColumnIndex, value) -> typedPartitionKey(value, columnTypes.get(hiveColumnIndex), columnNames.get(hiveColumnIndex), hiveStorageTimeZone));

            BiMap<Integer, Integer> inputs = IntStream.range(0, physicalColumns.size())
                    .boxed()
                    .collect(toImmutableBiMap(i -> physicalColumns.get(i).getHiveColumnIndex(), Function.identity()));

            // use column types from the current table schema; these types might be different from this partition's schema
            Map<VariableReferenceExpression, InputReferenceExpression> variableToInput = columnNames.keySet().stream()
                    .collect(toImmutableMap(
                            hiveColumnIndex -> new VariableReferenceExpression(Optional.empty(), columnNames.get(hiveColumnIndex), getColumnTypeFromTableSchema(coercers, columnTypes, hiveColumnIndex)),
                            hiveColumnIndex -> new InputReferenceExpression(Optional.empty(), inputs.get(hiveColumnIndex), getColumnTypeFromTableSchema(coercers, columnTypes, hiveColumnIndex))));

            Optional<BucketAdapter> bucketAdapter = bucketAdaptation.map(adaptation -> new BucketAdapter(
                    Arrays.stream(adaptation.getBucketColumnIndices())
                            .map(indexMapping::get)
                            .map(inputs::get)
                            .toArray(),
                    adaptation.getBucketColumnHiveTypes(),
                    adaptation.getTableBucketCount(),
                    adaptation.getPartitionBucketCount(),
                    adaptation.getBucketToKeep(),
                    isLegacyTimestampBucketing(session)));

            List<FilterFunction> filterFunctions = toFilterFunctions(replaceExpression(remainingPredicate, variableToInput), bucketAdapter, session, rowExpressionService.getDeterminismEvaluator(), rowExpressionService.getPredicateCompiler());

            OrcSelectiveRecordReader recordReader = reader.createSelectiveRecordReader(
                    columnTypes,
                    outputIndices,
                    tupleDomainFilters,
                    filterFunctions,
                    inputs.inverse(),
                    requiredSubfields,
                    typedPrefilledValues,
                    Maps.transformValues(mappedCoercers, Function.class::cast),
                    orcPredicate,
                    fileSplit.getStart(),
                    fileSplit.getLength(),
                    hiveStorageTimeZone,
                    systemMemoryUsage,
                    Optional.empty(),
                    INITIAL_BATCH_SIZE);

            return new OrcSelectivePageSource(
                    recordReader,
                    reader.getOrcDataSource(),
                    systemMemoryUsage,
                    stats,
                    hiveFileContext.getStats(),
                    appendRowNumberEnabled,
                    partitionID,
                    rowGroupId,
                    supplyRowIDs);
        }
        catch (Exception e) {
            try {
                orcDataSource.close();
            }
            catch (IOException ignored) {
            }
            throw mapToPrestoException(e, path, fileSplit);
        }
    }

    private static Type getColumnTypeFromTableSchema(Map<Integer, HiveCoercer> coercers, Map<Integer, Type> columnTypes, int hiveColumnIndex)
    {
        return coercers.containsKey(hiveColumnIndex) ? coercers.get(hiveColumnIndex).getToType() : columnTypes.get(hiveColumnIndex);
    }

    private static Map<Integer, List<Subfield>> collectRequiredSubfields(List<HiveColumnHandle> physicalColumns, List<Integer> outputColumns, Map<Integer, Map<Subfield, TupleDomainFilter>> tupleDomainFilters, RowExpression remainingPredicate, Map<String, Integer> columnIndices, StandardFunctionResolution functionResolution, RowExpressionService rowExpressionService, ConnectorSession session)
    {
        /**
         * The logic is:
         *
         * - columns projected fully are not modified;
         * - columns projected partially are updated to include subfields used in the filters or
         *      to be read in full if entire column is used in a filter
         * - columns used for filtering only are updated to prune subfields if filters don't use full column
         */

        Map<Integer, Set<Subfield>> outputSubfields = new HashMap<>();
        physicalColumns.stream()
                .filter(column -> outputColumns.contains(column.getHiveColumnIndex()))
                .forEach(column -> outputSubfields.put(column.getHiveColumnIndex(), new HashSet<>(column.getRequiredSubfields())));

        Map<Integer, Set<Subfield>> predicateSubfields = new HashMap<>();
        SubfieldExtractor subfieldExtractor = new SubfieldExtractor(functionResolution, rowExpressionService.getExpressionOptimizer(session), session);
        remainingPredicate.accept(
                new RequiredSubfieldsExtractor(subfieldExtractor),
                subfield -> predicateSubfields.computeIfAbsent(columnIndices.get(subfield.getRootName()), v -> new HashSet<>()).add(subfield));

        for (Map.Entry<Integer, Map<Subfield, TupleDomainFilter>> entry : tupleDomainFilters.entrySet()) {
            predicateSubfields.computeIfAbsent(entry.getKey(), v -> new HashSet<>()).addAll(entry.getValue().keySet());
        }

        Map<Integer, List<Subfield>> allSubfields = new HashMap<>();
        for (Map.Entry<Integer, Set<Subfield>> entry : outputSubfields.entrySet()) {
            int columnIndex = entry.getKey();
            if (entry.getValue().isEmpty()) {
                // entire column is projected out
                continue;
            }

            if (!predicateSubfields.containsKey(columnIndex)) {
                // column is not used in filters
                allSubfields.put(columnIndex, ImmutableList.copyOf(entry.getValue()));
                continue;
            }

            List<Subfield> prunedSubfields = pruneSubfields(ImmutableSet.<Subfield>builder()
                    .addAll(entry.getValue())
                    .addAll(predicateSubfields.get(columnIndex)).build());

            if (prunedSubfields.size() == 1 && isEntireColumn(prunedSubfields.get(0))) {
                // entire column is used in a filter
                continue;
            }

            allSubfields.put(columnIndex, prunedSubfields);
        }

        for (Map.Entry<Integer, Set<Subfield>> entry : predicateSubfields.entrySet()) {
            int columnIndex = entry.getKey();
            if (outputSubfields.containsKey(columnIndex)) {
                // this column has been already processed (in the previous loop)
                continue;
            }

            List<Subfield> prunedSubfields = pruneSubfields(entry.getValue());
            if (prunedSubfields.size() == 1 && isEntireColumn(prunedSubfields.get(0))) {
                // entire column is used in a filter
                continue;
            }

            allSubfields.put(columnIndex, prunedSubfields);
        }

        return allSubfields;
    }

    // Prunes subfields: if one subfield is a prefix of another subfield, keeps the shortest one.
    // Example: {a.b.c, a.b} -> {a.b}
    private static List<Subfield> pruneSubfields(Set<Subfield> subfields)
    {
        verify(!subfields.isEmpty());

        return subfields.stream()
                .filter(subfield -> !prefixExists(subfield, subfields))
                .collect(toImmutableList());
    }

    private static boolean prefixExists(Subfield subfield, Collection<Subfield> subfields)
    {
        return subfields.stream().anyMatch(path -> path.isPrefix(subfield));
    }

    private static final class RequiredSubfieldsExtractor
            extends DefaultRowExpressionTraversalVisitor<Consumer<Subfield>>
    {
        private final SubfieldExtractor subfieldExtractor;

        public RequiredSubfieldsExtractor(SubfieldExtractor subfieldExtractor)
        {
            this.subfieldExtractor = requireNonNull(subfieldExtractor, "subfieldExtractor is null");
        }

        @Override
        public Void visitCall(CallExpression call, Consumer<Subfield> context)
        {
            Optional<Subfield> subfield = subfieldExtractor.extract(call);
            if (subfield.isPresent()) {
                context.accept(subfield.get());
                return null;
            }

            call.getArguments().forEach(argument -> argument.accept(this, context));
            return null;
        }

        @Override
        public Void visitSpecialForm(SpecialFormExpression specialForm, Consumer<Subfield> context)
        {
            Optional<Subfield> subfield = subfieldExtractor.extract(specialForm);
            if (subfield.isPresent()) {
                context.accept(subfield.get());
                return null;
            }

            specialForm.getArguments().forEach(argument -> argument.accept(this, context));
            return null;
        }

        @Override
        public Void visitVariableReference(VariableReferenceExpression reference, Consumer<Subfield> context)
        {
            Optional<Subfield> subfield = subfieldExtractor.extract(reference);
            if (subfield.isPresent()) {
                context.accept(subfield.get());
                return null;
            }

            return null;
        }
    }

    private static Map<Integer, Map<Subfield, TupleDomainFilter>> toTupleDomainFilters(TupleDomain<Subfield> domainPredicate, Map<String, Integer> columnIndices, Map<Integer, HiveCoercer> coercers, TupleDomainFilterCache tupleDomainFilterCache)
    {
        Map<Subfield, TupleDomainFilter> filtersBySubfield = Maps.transformValues(domainPredicate.getDomains().get(), tupleDomainFilterCache::getFilter);

        Map<Integer, Map<Subfield, TupleDomainFilter>> filtersByColumn = new HashMap<>();
        for (Map.Entry<Subfield, TupleDomainFilter> entry : filtersBySubfield.entrySet()) {
            Subfield subfield = entry.getKey();
            int columnIndex = columnIndices.get(subfield.getRootName());
            TupleDomainFilter filter = entry.getValue();
            if (coercers.containsKey(columnIndex)) {
                filter = coercers.get(columnIndex).toCoercingFilter(filter, subfield);
            }
            filtersByColumn.computeIfAbsent(columnIndex, k -> new HashMap<>()).put(subfield, filter);
        }

        return ImmutableMap.copyOf(filtersByColumn);
    }

    private static OrcPredicate toOrcPredicate(TupleDomain<Subfield> domainPredicate, List<HiveColumnHandle> physicalColumns, Map<Integer, HiveCoercer> coercers, TypeManager typeManager, int domainCompactionThreshold, boolean orcBloomFiltersEnabled)
    {
        ImmutableList.Builder<TupleDomainOrcPredicate.ColumnReference<HiveColumnHandle>> columnReferences = ImmutableList.builder();
        for (HiveColumnHandle column : physicalColumns) {
            if (column.getColumnType() == REGULAR) {
                Type type = typeManager.getType(column.getTypeSignature());
                columnReferences.add(new TupleDomainOrcPredicate.ColumnReference<>(column, column.getHiveColumnIndex(), type));
            }
        }

        Map<String, HiveColumnHandle> columnsByName = uniqueIndex(physicalColumns, HiveColumnHandle::getName);
        TupleDomain<HiveColumnHandle> entireColumnDomains = domainPredicate
                .transform(subfield -> isEntireColumn(subfield) ? columnsByName.get(subfield.getRootName()) : null)
                // filter out columns with coercions to avoid type mismatch errors between column stats in the file and values domain
                .transform(column -> coercers.containsKey(column.getHiveColumnIndex()) ? null : column);
        return new TupleDomainOrcPredicate<>(entireColumnDomains, columnReferences.build(), orcBloomFiltersEnabled, Optional.of(domainCompactionThreshold));
    }

    /**
     * Split filter expression into groups of conjuncts that depend on the same set of inputs,
     * then compile each group into FilterFunction.
     */
    private static List<FilterFunction> toFilterFunctions(RowExpression filter, Optional<BucketAdapter> bucketAdapter, ConnectorSession session, DeterminismEvaluator determinismEvaluator, PredicateCompiler predicateCompiler)
    {
        ImmutableList.Builder<FilterFunction> filterFunctions = ImmutableList.builder();

        bucketAdapter.map(predicate -> new FilterFunction(session.getSqlFunctionProperties(), true, predicate))
                .ifPresent(filterFunctions::add);

        if (TRUE_CONSTANT.equals(filter)) {
            return filterFunctions.build();
        }

        DynamicFilterExtractResult extractDynamicFilterResult = extractDynamicFilters(filter);

        // dynamic filter will be added through subfield pushdown
        filter = and(extractDynamicFilterResult.getStaticConjuncts());

        if (!isAdaptiveFilterReorderingEnabled(session)) {
            filterFunctions.add(new FilterFunction(session.getSqlFunctionProperties(), determinismEvaluator.isDeterministic(filter), predicateCompiler.compilePredicate(session.getSqlFunctionProperties(), session.getSessionFunctions(), filter).get()));
            return filterFunctions.build();
        }

        List<RowExpression> conjuncts = extractConjuncts(filter);
        if (conjuncts.size() == 1) {
            filterFunctions.add(new FilterFunction(session.getSqlFunctionProperties(), determinismEvaluator.isDeterministic(filter), predicateCompiler.compilePredicate(session.getSqlFunctionProperties(), session.getSessionFunctions(), filter).get()));
            return filterFunctions.build();
        }

        // Use LinkedHashMap to preserve user-specified order of conjuncts. This will be the initial order in which filters are applied.
        Map<Set<Integer>, List<RowExpression>> inputsToConjuncts = new LinkedHashMap<>();
        for (RowExpression conjunct : conjuncts) {
            inputsToConjuncts.computeIfAbsent(extractInputs(conjunct), k -> new ArrayList<>()).add(conjunct);
        }

        inputsToConjuncts.values().stream()
                .map(expressions -> binaryExpression(AND, expressions))
                .map(predicate -> new FilterFunction(session.getSqlFunctionProperties(), determinismEvaluator.isDeterministic(predicate), predicateCompiler.compilePredicate(session.getSqlFunctionProperties(), session.getSessionFunctions(), predicate).get()))
                .forEach(filterFunctions::add);

        return filterFunctions.build();
    }

    private static Set<Integer> extractInputs(RowExpression expression)
    {
        ImmutableSet.Builder<Integer> inputs = ImmutableSet.builder();
        expression.accept(new InputReferenceBuilderVisitor(), inputs);
        return inputs.build();
    }

    private static class InputReferenceBuilderVisitor
            extends DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<Integer>>
    {
        @Override
        public Void visitInputReference(InputReferenceExpression input, ImmutableSet.Builder<Integer> builder)
        {
            builder.add(input.getField());
            return null;
        }
    }

    private static class BucketAdapter
            implements Predicate
    {
        public final int[] bucketColumns;
        public final int bucketToKeep;
        public final int tableBucketCount;
        public final int partitionBucketCount; // for sanity check only
        private final List<TypeInfo> typeInfoList;
        private final boolean useLegacyTimestampBucketing;

        public BucketAdapter(int[] bucketColumnIndices, List<HiveType> bucketColumnHiveTypes, int tableBucketCount, int partitionBucketCount, int bucketToKeep, boolean useLegacyTimestampBucketing)
        {
            this.bucketColumns = requireNonNull(bucketColumnIndices, "bucketColumnIndices is null");
            this.bucketToKeep = bucketToKeep;
            this.typeInfoList = requireNonNull(bucketColumnHiveTypes, "bucketColumnHiveTypes is null").stream()
                    .map(HiveType::getTypeInfo)
                    .collect(toImmutableList());
            this.tableBucketCount = tableBucketCount;
            this.partitionBucketCount = partitionBucketCount;
            this.useLegacyTimestampBucketing = useLegacyTimestampBucketing;
        }

        @Override
        public int[] getInputChannels()
        {
            return bucketColumns;
        }

        @Override
        public boolean evaluate(SqlFunctionProperties properties, Page page, int position)
        {
            int bucket = getHiveBucket(tableBucketCount, typeInfoList, page, position, useLegacyTimestampBucketing);
            if ((bucket - bucketToKeep) % partitionBucketCount != 0) {
                throw new PrestoException(HIVE_INVALID_BUCKET_FILES, format(
                        "A row that is supposed to be in bucket %s is encountered. Only rows in bucket %s (modulo %s) are expected",
                        bucket, bucketToKeep % partitionBucketCount, partitionBucketCount));
            }

            return bucket == bucketToKeep;
        }
    }
}
