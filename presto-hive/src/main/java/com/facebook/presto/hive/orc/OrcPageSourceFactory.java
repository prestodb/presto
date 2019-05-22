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

import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePageSourceFactory;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.AbstractFilterFunction;
import com.facebook.presto.orc.ErrorSet;
import com.facebook.presto.orc.Filter;
import com.facebook.presto.orc.Filters;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.TupleDomainOrcPredicate;
import com.facebook.presto.orc.TupleDomainOrcPredicate.ColumnReference;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.Predicate;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILE_MISSING_COLUMN_NAMES;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcLazyReadSmallRanges;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxBufferSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxReadBlockSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcStreamBufferSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcTinyStripeThreshold;
import static com.facebook.presto.hive.HiveSessionProperties.isFilterReorderingEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isOrcBloomFiltersEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isReaderBudgetEnforcementEnabled;
import static com.facebook.presto.hive.HiveUtil.isDeserializerClass;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.TupleDomainFilters.toFilter;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.MOST_OPTIMIZED;
import static com.facebook.presto.spi.relation.LogicalRowExpressions.TRUE;
import static com.facebook.presto.spi.relation.LogicalRowExpressions.binaryExpression;
import static com.facebook.presto.spi.relation.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OrcPageSourceFactory
        implements HivePageSourceFactory
{
    private static final Pattern DEFAULT_HIVE_COLUMN_NAME_PATTERN = Pattern.compile("_col\\d+");

    private final TypeManager typeManager;
    private final DeterminismEvaluator determinismEvaluator;
    private final ExpressionOptimizer expressionOptimizer;
    private final PredicateCompiler predicateCompiler;
    private final boolean useOrcColumnNames;
    private final int domainCompactionThreshold;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;

    @Inject
    public OrcPageSourceFactory(
            TypeManager typeManager,
            RowExpressionService rowExpressionService,
            HiveClientConfig config,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats)
    {
        this(typeManager, rowExpressionService.getDeterminismEvaluator(), rowExpressionService.getExpressionOptimizer(), rowExpressionService.getPredicateCompiler(), requireNonNull(config, "hiveClientConfig is null").isUseOrcColumnNames(), config.getDomainCompactionThreshold(), hdfsEnvironment, stats);
    }

    public OrcPageSourceFactory(
            TypeManager typeManager,
            DeterminismEvaluator determinismEvaluator,
            ExpressionOptimizer expressionOptimizer,
            PredicateCompiler predicateCompiler,
            boolean useOrcColumnNames,
            int domainCompactionThreshold,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.determinismEvaluator = requireNonNull(determinismEvaluator, "determinismEvaluator is null");
        this.expressionOptimizer = requireNonNull(expressionOptimizer, "expressionOptimizer is null");
        this.predicateCompiler = requireNonNull(predicateCompiler, "predicateCompiler is null");
        this.useOrcColumnNames = useOrcColumnNames;
        this.domainCompactionThreshold = domainCompactionThreshold;
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<Subfield> domainPredicate,
            RowExpression remainingPredicate,
            Map<String, HiveColumnHandle> predicateColumns,
            DateTimeZone hiveStorageTimeZone)
    {
        if (!isDeserializerClass(schema, OrcSerde.class)) {
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
                session.getUser(),
                configuration,
                path,
                start,
                length,
                fileSize,
                columns,
                useOrcColumnNames,
                domainCompactionThreshold,
                domainPredicate,
                remainingPredicate,
                predicateColumns,
                hiveStorageTimeZone,
                typeManager,
                determinismEvaluator,
                expressionOptimizer,
                predicateCompiler,
                getOrcMaxMergeDistance(session),
                getOrcMaxBufferSize(session),
                getOrcStreamBufferSize(session),
                getOrcTinyStripeThreshold(session),
                getOrcMaxReadBlockSize(session),
                getOrcLazyReadSmallRanges(session),
                isOrcBloomFiltersEnabled(session),
                stats));
    }

    public static OrcPageSource createOrcPageSource(
            ConnectorSession session,
            OrcEncoding orcEncoding,
            HdfsEnvironment hdfsEnvironment,
            String sessionUser,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            List<HiveColumnHandle> columns,
            boolean useOrcColumnNames,
            int domainCompactionThreshold,
            TupleDomain<Subfield> domainPredicate,
            RowExpression remainingPredicate,
            Map<String, HiveColumnHandle> predicateColumns,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            DeterminismEvaluator determinismEvaluator,
            ExpressionOptimizer expressionOptimizer,
            PredicateCompiler predicateCompiler,
            DataSize maxMergeDistance,
            DataSize maxBufferSize,
            DataSize streamBufferSize,
            DataSize tinyStripeThreshold,
            DataSize maxReadBlockSize,
            boolean lazyReadSmallRanges,
            boolean orcBloomFiltersEnabled,
            FileFormatDataSourceStats stats)
    {
        OrcDataSource orcDataSource;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(sessionUser, path, configuration);
            FSDataInputStream inputStream = fileSystem.open(path);
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
            OrcReader reader = new OrcReader(orcDataSource, orcEncoding, maxMergeDistance, maxBufferSize, tinyStripeThreshold, maxReadBlockSize);

            List<HiveColumnHandle> physicalColumns = getPhysicalHiveColumnHandles(columns, useOrcColumnNames, reader, path);
            ImmutableMap.Builder<Integer, Type> includedColumns = ImmutableMap.builder();
            ImmutableMap.Builder<Integer, List<Subfield>> includedSubfields = ImmutableMap.builder();
            ImmutableList.Builder<ColumnReference<HiveColumnHandle>> columnReferences = ImmutableList.builder();
            for (HiveColumnHandle column : physicalColumns) {
                if (column.getColumnType() == REGULAR) {
                    Type type = typeManager.getType(column.getTypeSignature());
                    includedColumns.put(column.getHiveColumnIndex(), type);
                    if (column.getReferencedSubfields() != null) {
                        includedSubfields.put(column.getHiveColumnIndex(), column.getReferencedSubfields());
                    }
                    columnReferences.add(new ColumnReference<>(column, column.getHiveColumnIndex(), type));
                }
            }

            ImmutableMap.Builder<String, Integer> columnIndices = ImmutableMap.builder();
            for (int i = 0; i < physicalColumns.size(); i++) {
                columnIndices.put(physicalColumns.get(i).getName(), i);
            }

            TupleDomain<HiveColumnHandle> compactDomainPredicate = toCompactTupleDomain(
                    domainPredicate
                            .transform(subfield -> isEntireColumn(subfield) ? subfield.getRootName() : null)
                            .transform(predicateColumns::get),
                    domainCompactionThreshold);
            OrcPredicate predicate = new TupleDomainOrcPredicate<>(compactDomainPredicate, columnReferences.build(), orcBloomFiltersEnabled);
            Map<Integer, Filter> columnFilters = toFilters(domainPredicate, predicateColumns);
            RowExpression optimizedPredicate = expressionOptimizer.optimize(remainingPredicate, MOST_OPTIMIZED, session)
                    .accept(new ColumnNameToIndexTranslator(), columnIndices.build());
            List<AbstractFilterFunction> filterFunctions = toFilterFunctions(optimizedPredicate, session, determinismEvaluator, predicateCompiler);

            OrcRecordReader recordReader = reader.createRecordReader(
                    includedColumns.build(),
                    includedSubfields.build(),
                    predicate,
                    columnFilters,
                    filterFunctions,
                    start,
                    length,
                    hiveStorageTimeZone,
                    systemMemoryUsage,
                    INITIAL_BATCH_SIZE,
                    isFilterReorderingEnabled(session),
                    isReaderBudgetEnforcementEnabled(session));

            return new OrcPageSource(
                    recordReader,
                    orcDataSource,
                    physicalColumns,
                    typeManager,
                    systemMemoryUsage,
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

    private static boolean isEntireColumn(Subfield subfield)
    {
        return subfield.getPath().isEmpty();
    }

    private static <C> TupleDomain<C> toCompactTupleDomain(TupleDomain<C> effectivePredicate, int threshold)
    {
        ImmutableMap.Builder<C, Domain> builder = ImmutableMap.builder();
        effectivePredicate.getDomains().ifPresent(domains -> {
            for (Map.Entry<C, Domain> entry : domains.entrySet()) {
                C hiveColumnHandle = entry.getKey();

                ValueSet values = entry.getValue().getValues();
                ValueSet compactValueSet = values.getValuesProcessor().<Optional<ValueSet>>transform(
                        ranges -> ranges.getRangeCount() > threshold ? Optional.of(ValueSet.ofRanges(ranges.getSpan())) : Optional.empty(),
                        discreteValues -> discreteValues.getValues().size() > threshold ? Optional.of(ValueSet.all(values.getType())) : Optional.empty(),
                        allOrNone -> Optional.empty())
                        .orElse(values);
                builder.put(hiveColumnHandle, Domain.create(compactValueSet, entry.getValue().isNullAllowed()));
            }
        });
        return TupleDomain.withColumnDomains(builder.build());
    }

    private static final class FilterFunction
            extends AbstractFilterFunction
    {
        private final ConnectorSession session;
        private final boolean deterministic;
        private final Predicate predicate;

        private FilterFunction(ConnectorSession session, boolean deterministic, Predicate predicate)
        {
            super(requireNonNull(predicate, "predicate is null").getInputChannels(), 1);
            this.session = requireNonNull(session, "session is null");
            this.deterministic = deterministic;
            this.predicate = predicate;
        }

        @Override
        public boolean isDeterministic()
        {
            return deterministic;
        }

        @Override
        public int filter(Page page, int[] outputRows, ErrorSet errorSet)
        {
            int positionCount = page.getPositionCount();
            if (outputRows.length < positionCount) {
                throw new IllegalArgumentException("outputRows is too small");
            }

            int outputCount = 0;
            for (int i = 0; i < positionCount; i++) {
                try {
                    if (predicate.evaluate(session, page, i)) {
                        outputRows[outputCount++] = i;
                    }
                }
                catch (RuntimeException e) {
                    outputRows[outputCount++] = i;
                    errorSet.addError(i, positionCount, e);
                }
            }

            return outputCount;
        }
    }

    private static class ColumnNameToIndexTranslator
            implements RowExpressionVisitor<RowExpression, Map<String, Integer>>
    {
        @Override
        public RowExpression visitCall(CallExpression call, Map<String, Integer> columns)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
            call.getArguments().forEach(argument -> arguments.add(argument.accept(this, columns)));
            return new CallExpression(call.getDisplayName(), call.getFunctionHandle(), call.getType(), arguments.build());
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, Map<String, Integer> columns)
        {
            throw new UnsupportedOperationException("encountered already-translated reference");
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Map<String, Integer> columns)
        {
            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Map<String, Integer> columns)
        {
            return new LambdaDefinitionExpression(lambda.getArgumentTypes(), lambda.getArguments(), lambda.getBody().accept(this, columns));
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Map<String, Integer> columns)
        {
            String name = reference.getName();
            if (columns.containsKey(name)) {
                return new InputReferenceExpression(columns.get(name), reference.getType());
            }
            // this is possible only for lambda
            return reference;
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Map<String, Integer> columns)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
            specialForm.getArguments().forEach(argument -> arguments.add(argument.accept(this, columns)));
            return new SpecialFormExpression(specialForm.getForm(), specialForm.getType(), arguments.build());
        }
    }

    // Splits filter into groups of conjuncts that depend on the same set of inputs, then
    // compiles each group into an instance of AbstractFilterFunction.
    private static List<AbstractFilterFunction> toFilterFunctions(RowExpression filter, ConnectorSession session, DeterminismEvaluator determinismEvaluator, PredicateCompiler predicateCompiler)
    {
        if (TRUE.equals(filter)) {
            return ImmutableList.of();
        }

        List<RowExpression> conjuncts = extractConjuncts(filter);
        if (conjuncts.size() == 1) {
            return ImmutableList.of(new FilterFunction(session, determinismEvaluator.isDeterministic(filter), predicateCompiler.compilePredicate(filter).get()));
        }

        Map<Set<Integer>, List<RowExpression>> inputsToConjuncts = new HashMap();
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

    private static Map<Integer, Filter> toFilters(TupleDomain<Subfield> predicate, Map<String, HiveColumnHandle> columns)
    {
        checkArgument(!predicate.isNone(), "predicate is none");

        if (predicate.isAll()) {
            return ImmutableMap.of();
        }

        Map<Integer, Filter> filters = new HashMap<>();
        for (Map.Entry<Subfield, Domain> entry : predicate.getDomains().get().entrySet()) {
            addFilter(entry.getKey(), toFilter(entry.getValue()), columns, filters);
        }
        return ImmutableMap.copyOf(filters);
    }

    private static void addFilter(Subfield subfield, Filter filter, Map<String, HiveColumnHandle> columns, Map<Integer, Filter> filters)
    {
        String columnName = subfield.getRootName();
        int columnIndex = columns.get(columnName).getHiveColumnIndex();

        if (isEntireColumn(subfield)) {
            filters.put(columnIndex, filter);
            return;
        }

        Filter topFilter = filters.get(columnIndex);
        verify(topFilter == null || topFilter instanceof Filters.StructFilter);
        Filters.StructFilter structFilter = (Filters.StructFilter) topFilter;
        if (structFilter == null) {
            structFilter = new Filters.StructFilter();
            filters.put(columnIndex, structFilter);
        }
        int depth = subfield.getPath().size();
        for (int i = 0; i < depth; i++) {
            Subfield.PathElement pathElement = subfield.getPath().get(i);
            Filter memberFilter = structFilter.getMember(pathElement);
            if (i == depth - 1) {
                verify(memberFilter == null);
                structFilter.addMember(pathElement, filter);
                return;
            }
            if (memberFilter == null) {
                memberFilter = new Filters.StructFilter();
                structFilter.addMember(pathElement, memberFilter);
                structFilter = (Filters.StructFilter) memberFilter;
            }
            else {
                verify(memberFilter instanceof Filters.StructFilter);
                structFilter = (Filters.StructFilter) memberFilter;
            }
        }
    }

    private static String splitError(Throwable t, Path path, long start, long length)
    {
        return format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, t.getMessage());
    }

    private static List<HiveColumnHandle> getPhysicalHiveColumnHandles(List<HiveColumnHandle> columns, boolean useOrcColumnNames, OrcReader reader, Path path)
    {
        if (!useOrcColumnNames) {
            return columns;
        }

        verifyFileHasColumnNames(reader.getColumnNames(), path);

        Map<String, Integer> physicalNameOrdinalMap = buildPhysicalNameOrdinalMap(reader);
        int nextMissingColumnIndex = physicalNameOrdinalMap.size();

        ImmutableList.Builder<HiveColumnHandle> physicalColumns = ImmutableList.builder();
        for (HiveColumnHandle column : columns) {
            Integer physicalOrdinal = physicalNameOrdinalMap.get(column.getName());
            if (physicalOrdinal == null) {
                // if the column is missing from the file, assign it a column number larger
                // than the number of columns in the file so the reader will fill it with nulls
                physicalOrdinal = nextMissingColumnIndex;
                nextMissingColumnIndex++;
            }
            physicalColumns.add(new HiveColumnHandle(column.getName(), column.getHiveType(), column.getTypeSignature(), physicalOrdinal, column.getColumnType(), column.getComment()));
        }
        return physicalColumns.build();
    }

    private static void verifyFileHasColumnNames(List<String> physicalColumnNames, Path path)
    {
        if (!physicalColumnNames.isEmpty() && physicalColumnNames.stream().allMatch(physicalColumnName -> DEFAULT_HIVE_COLUMN_NAME_PATTERN.matcher(physicalColumnName).matches())) {
            throw new PrestoException(
                    HIVE_FILE_MISSING_COLUMN_NAMES,
                    "ORC file does not contain column names in the footer: " + path);
        }
    }

    private static Map<String, Integer> buildPhysicalNameOrdinalMap(OrcReader reader)
    {
        ImmutableMap.Builder<String, Integer> physicalNameOrdinalMap = ImmutableMap.builder();

        int ordinal = 0;
        for (String physicalColumnName : reader.getColumnNames()) {
            physicalNameOrdinalMap.put(physicalColumnName, ordinal);
            ordinal++;
        }

        return physicalNameOrdinalMap.build();
    }
}
