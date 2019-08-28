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

import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.FileOpener;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveSelectivePageSourceFactory;
import com.facebook.presto.hive.SubfieldExtractor;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.FilterFunction;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.OrcFileTailSource;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcSelectiveRecordReader;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.TupleDomainFilterUtils;
import com.facebook.presto.orc.TupleDomainOrcPredicate;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.binaryExpression;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.expressions.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcLazyReadSmallRanges;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxBufferSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxReadBlockSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcStreamBufferSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcTinyStripeThreshold;
import static com.facebook.presto.hive.HiveSessionProperties.isOrcBloomFiltersEnabled;
import static com.facebook.presto.hive.HiveUtil.getPhysicalHiveColumnHandles;
import static com.facebook.presto.hive.HiveUtil.typedPartitionKey;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
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
    private final boolean useOrcColumnNames;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final int domainCompactionThreshold;
    private final OrcFileTailSource orcFileTailSource;
    private final FileOpener fileOpener;

    @Inject
    public OrcSelectivePageSourceFactory(
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            HiveClientConfig config,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            OrcFileTailSource orcFileTailSource,
            FileOpener fileOpener)
    {
        this(
                typeManager,
                functionResolution,
                rowExpressionService,
                requireNonNull(config, "hiveClientConfig is null").isUseOrcColumnNames(),
                hdfsEnvironment,
                stats,
                config.getDomainCompactionThreshold(),
                orcFileTailSource,
                fileOpener);
    }

    public OrcSelectivePageSourceFactory(
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            boolean useOrcColumnNames,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            int domainCompactionThreshold,
            OrcFileTailSource orcFileTailSource,
            FileOpener fileOpener)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.useOrcColumnNames = useOrcColumnNames;
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.domainCompactionThreshold = domainCompactionThreshold;
        this.orcFileTailSource = requireNonNull(orcFileTailSource, "orcFileTailCache is null");
        this.fileOpener = requireNonNull(fileOpener, "fileOpener is null");
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Storage storage,
            List<HiveColumnHandle> columns,
            Map<Integer, String> prefilledValues,
            List<Integer> outputColumns,
            TupleDomain<Subfield> domainPredicate,
            RowExpression remainingPredicate,
            DateTimeZone hiveStorageTimeZone,
            Optional<byte[]> extraFileInfo)
    {
        if (!OrcSerde.class.getName().equals(storage.getStorageFormat().getSerDe())) {
            return Optional.empty();
        }

        // per HIVE-13040 and ORC-162, empty files are allowed
        if (fileSize == 0) {
            return Optional.of(new FixedPageSource(ImmutableList.of()));
        }

        return Optional.of(createOrcPageSource(
                session,
                ORC,
                hdfsEnvironment,
                configuration,
                path,
                start,
                length,
                fileSize,
                columns,
                prefilledValues,
                outputColumns,
                domainPredicate,
                remainingPredicate,
                useOrcColumnNames,
                hiveStorageTimeZone,
                typeManager,
                functionResolution,
                rowExpressionService,
                isOrcBloomFiltersEnabled(session),
                stats,
                domainCompactionThreshold,
                orcFileTailSource,
                extraFileInfo,
                fileOpener));
    }

    public static OrcSelectivePageSource createOrcPageSource(
            ConnectorSession session,
            OrcEncoding orcEncoding,
            HdfsEnvironment hdfsEnvironment,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            List<HiveColumnHandle> columns,
            Map<Integer, String> prefilledValues,
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
            Optional<byte[]> extraFileInfo,
            FileOpener fileOpener)
    {
        checkArgument(domainCompactionThreshold >= 1, "domainCompactionThreshold must be at least 1");

        DataSize maxMergeDistance = getOrcMaxMergeDistance(session);
        DataSize maxBufferSize = getOrcMaxBufferSize(session);
        DataSize streamBufferSize = getOrcStreamBufferSize(session);
        DataSize tinyStripeThreshold = getOrcTinyStripeThreshold(session);
        DataSize maxReadBlockSize = getOrcMaxReadBlockSize(session);
        boolean lazyReadSmallRanges = getOrcLazyReadSmallRanges(session);

        OrcDataSource orcDataSource;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), path, configuration);
            FSDataInputStream inputStream = fileOpener.open(fileSystem, path, extraFileInfo);
            orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(path.toString()),
                    fileSize,
                    maxMergeDistance,
                    maxBufferSize,
                    streamBufferSize,
                    lazyReadSmallRanges,
                    inputStream,
                    stats);
        }
        catch (Exception e) {
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }

        AggregatedMemoryContext systemMemoryUsage = newSimpleAggregatedMemoryContext();
        try {
            OrcReader reader = new OrcReader(orcDataSource, orcEncoding, maxMergeDistance, tinyStripeThreshold, maxReadBlockSize, orcFileTailSource);

            checkArgument(!domainPredicate.isNone(), "Unexpected NONE domain");

            List<HiveColumnHandle> physicalColumns = getPhysicalHiveColumnHandles(columns, useOrcColumnNames, reader, path);
            Map<Integer, Integer> indexMapping = IntStream.range(0, columns.size())
                    .boxed()
                    .collect(toImmutableMap(i -> columns.get(i).getHiveColumnIndex(), i -> physicalColumns.get(i).getHiveColumnIndex()));

            Map<Integer, String> columnNames = physicalColumns.stream()
                    .collect(toImmutableMap(HiveColumnHandle::getHiveColumnIndex, HiveColumnHandle::getName));

            OrcPredicate orcPredicate = toOrcPredicate(domainPredicate, physicalColumns, typeManager, domainCompactionThreshold, orcBloomFiltersEnabled);

            Map<String, Integer> columnIndices = ImmutableBiMap.copyOf(columnNames).inverse();
            Map<Integer, Map<Subfield, TupleDomainFilter>> tupleDomainFilters = toTupleDomainFilters(domainPredicate, columnIndices);

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

            Map<VariableReferenceExpression, InputReferenceExpression> variableToInput = columnNames.keySet().stream()
                    .collect(toImmutableMap(
                            hiveColumnIndex -> new VariableReferenceExpression(columnNames.get(hiveColumnIndex), columnTypes.get(hiveColumnIndex)),
                            hiveColumnIndex -> new InputReferenceExpression(inputs.get(hiveColumnIndex), columnTypes.get(hiveColumnIndex))));

            List<FilterFunction> filterFunctions = toFilterFunctions(replaceExpression(remainingPredicate, variableToInput), session, rowExpressionService.getDeterminismEvaluator(), rowExpressionService.getPredicateCompiler());

            OrcSelectiveRecordReader recordReader = reader.createSelectiveRecordReader(
                    columnTypes,
                    outputIndices,
                    tupleDomainFilters,
                    filterFunctions,
                    inputs.inverse(),
                    requiredSubfields,
                    typedPrefilledValues,
                    orcPredicate,
                    start,
                    length,
                    hiveStorageTimeZone,
                    systemMemoryUsage,
                    Optional.empty(),
                    INITIAL_BATCH_SIZE);

            return new OrcSelectivePageSource(
                    recordReader,
                    orcDataSource,
                    systemMemoryUsage.newAggregatedMemoryContext(),
                    stats);
        }
        catch (Exception e) {
            try {
                orcDataSource.close();
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = splitError(e, path, start, length);
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
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

        Map<Integer, Set<Subfield>> requiredSubfields = new HashMap<>();
        physicalColumns.stream()
                .filter(column -> outputColumns.contains(column.getHiveColumnIndex()))
                .forEach(column -> requiredSubfields.put(column.getHiveColumnIndex(), new HashSet<>(column.getRequiredSubfields())));

        for (int index : outputColumns) {
            requiredSubfields.computeIfAbsent(index, v -> new HashSet<>());
        }

        ImmutableSet.Builder<Subfield> predicateSubfields = ImmutableSet.builder();
        remainingPredicate.accept(new RequiredSubfieldsExtractor(new SubfieldExtractor(functionResolution, rowExpressionService.getExpressionOptimizer(), session)), predicateSubfields);

        for (Subfield subfield : predicateSubfields.build()) {
            int index = columnIndices.get(subfield.getRootName());
            updateRequiredSubfields(requiredSubfields, index, subfield);
        }

        for (Map.Entry<Integer, Map<Subfield, TupleDomainFilter>> entry : tupleDomainFilters.entrySet()) {
            int index = entry.getKey();
            for (Subfield subfield : entry.getValue().keySet()) {
                updateRequiredSubfields(requiredSubfields, index, subfield);
            }
        }

        return requiredSubfields.entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> (List) ImmutableList.copyOf(entry.getValue())));
    }

    private static void updateRequiredSubfields(Map<Integer, Set<Subfield>> requiredSubfields, int index, Subfield subfield)
    {
        if (isEntireColumn(subfield)) {
            if (requiredSubfields.containsKey(index)) {
                requiredSubfields.get(index).clear();
            }

            return;
        }

        if (requiredSubfields.containsKey(index)) {
            if (!requiredSubfields.get(index).isEmpty()) {
                requiredSubfields.get(index).add(subfield);
            }
            return;
        }

        requiredSubfields.computeIfAbsent(index, v -> new HashSet<>()).add(subfield);
    }

    private static final class RequiredSubfieldsExtractor
            extends DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<Subfield>>
    {
        private final SubfieldExtractor subfieldExtractor;

        public RequiredSubfieldsExtractor(SubfieldExtractor subfieldExtractor)
        {
            this.subfieldExtractor = requireNonNull(subfieldExtractor, "subfieldExtractor is null");
        }

        @Override
        public Void visitCall(CallExpression call, ImmutableSet.Builder<Subfield> context)
        {
            Optional<Subfield> subfield = subfieldExtractor.extract(call);
            if (subfield.isPresent()) {
                if (!isEntireColumn(subfield)) {
                    context.add(subfield.get());
                }
                return null;
            }

            call.getArguments().forEach(argument -> argument.accept(this, context));
            return null;
        }

        @Override
        public Void visitSpecialForm(SpecialFormExpression specialForm, ImmutableSet.Builder<Subfield> context)
        {
            Optional<Subfield> subfield = subfieldExtractor.extract(specialForm);
            if (subfield.isPresent()) {
                if (!isEntireColumn(subfield)) {
                    context.add(subfield.get());
                }
                return null;
            }

            specialForm.getArguments().forEach(argument -> argument.accept(this, context));
            return null;
        }
    }

    private static boolean isEntireColumn(Optional<Subfield> subfield)
    {
        return subfield.get().getPath().isEmpty();
    }

    private static Map<Integer, Map<Subfield, TupleDomainFilter>> toTupleDomainFilters(TupleDomain<Subfield> domainPredicate, Map<String, Integer> columnIndices)
    {
        Map<Subfield, TupleDomainFilter> filtersBySubfield = Maps.transformValues(domainPredicate.getDomains().get(), TupleDomainFilterUtils::toFilter);

        Map<Integer, Map<Subfield, TupleDomainFilter>> filtersByColumn = new HashMap<>();
        for (Map.Entry<Subfield, TupleDomainFilter> entry : filtersBySubfield.entrySet()) {
            int columnIndex = columnIndices.get(entry.getKey().getRootName());
            filtersByColumn.computeIfAbsent(columnIndex, k -> new HashMap<>()).put(entry.getKey(), entry.getValue());
        }

        return ImmutableMap.copyOf(filtersByColumn);
    }

    private static boolean isEntireColumn(Subfield subfield)
    {
        return subfield.getPath().isEmpty();
    }

    private static OrcPredicate toOrcPredicate(TupleDomain<Subfield> domainPredicate, List<HiveColumnHandle> physicalColumns, TypeManager typeManager, int domainCompactionThreshold, boolean orcBloomFiltersEnabled)
    {
        ImmutableList.Builder<TupleDomainOrcPredicate.ColumnReference<HiveColumnHandle>> columnReferences = ImmutableList.builder();
        for (HiveColumnHandle column : physicalColumns) {
            if (column.getColumnType() == REGULAR) {
                Type type = typeManager.getType(column.getTypeSignature());
                columnReferences.add(new TupleDomainOrcPredicate.ColumnReference<>(column, column.getHiveColumnIndex(), type));
            }
        }

        Map<String, HiveColumnHandle> columnsByName = uniqueIndex(physicalColumns, HiveColumnHandle::getName);
        TupleDomain<HiveColumnHandle> entireColumnDomains = domainPredicate.transform(subfield -> isEntireColumn(subfield) ? columnsByName.get(subfield.getRootName()) : null);
        return new TupleDomainOrcPredicate<>(entireColumnDomains, columnReferences.build(), orcBloomFiltersEnabled, Optional.of(domainCompactionThreshold));
    }

    /**
     * Split filter expression into groups of conjuncts that depend on the same set of inputs,
     * then compile each group into FilterFunction.
     */
    private static List<FilterFunction> toFilterFunctions(RowExpression filter, ConnectorSession session, DeterminismEvaluator determinismEvaluator, PredicateCompiler predicateCompiler)
    {
        if (TRUE_CONSTANT.equals(filter)) {
            return ImmutableList.of();
        }

        List<RowExpression> conjuncts = extractConjuncts(filter);
        if (conjuncts.size() == 1) {
            return ImmutableList.of(new FilterFunction(session, determinismEvaluator.isDeterministic(filter), predicateCompiler.compilePredicate(filter).get()));
        }

        // Use LinkedHashMap to preserve user-specified order of conjuncts. This will be the initial order in which filters are applied.
        Map<Set<Integer>, List<RowExpression>> inputsToConjuncts = new LinkedHashMap<>();
        for (RowExpression conjunct : conjuncts) {
            inputsToConjuncts.computeIfAbsent(extractInputs(conjunct), k -> new ArrayList<>()).add(conjunct);
        }

        return inputsToConjuncts.values().stream()
                .map(expressions -> binaryExpression(AND, expressions))
                .map(predicate -> new FilterFunction(session, determinismEvaluator.isDeterministic(predicate), predicateCompiler.compilePredicate(predicate).get()))
                .collect(toImmutableList());
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

    private static String splitError(Throwable t, Path path, long start, long length)
    {
        return format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, t.getMessage());
    }
}
