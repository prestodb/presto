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
package com.facebook.presto.orc;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockLease;
import com.facebook.presto.common.block.LazyBlock;
import com.facebook.presto.common.block.LazyBlockLoader;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.TupleDomainFilter.BigintMultiRange;
import com.facebook.presto.orc.TupleDomainFilter.BigintRange;
import com.facebook.presto.orc.TupleDomainFilter.BigintValuesUsingBitmask;
import com.facebook.presto.orc.TupleDomainFilter.BigintValuesUsingHashTable;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.PostScript;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.StripeStatistics;
import com.facebook.presto.orc.reader.SelectiveStreamReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.orc.reader.SelectiveStreamReaders.createStreamReader;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class OrcSelectiveRecordReader
        extends AbstractOrcRecordReader<SelectiveStreamReader>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(OrcSelectiveRecordReader.class).instanceSize();

    // Marks a SQL null when occurring in constantValues.
    private static final byte[] NULL_MARKER = new byte[0];
    private static final Page EMPTY_PAGE = new Page(0);

    private final int[] hiveColumnIndices;                            // elements are hive column indices
    private final List<Integer> outputColumns;                        // elements are hive column indices
    private final Map<Integer, Type> columnTypes;                     // key: index into hiveColumnIndices array
    private final Object[] constantValues;                            // aligned with hiveColumnIndices array
    private final Function<Block, Block>[] coercers;                   // aligned with hiveColumnIndices array

    // non-deterministic filter function with no inputs (rand() < 0.1); evaluated before any column is read
    private final Optional<FilterFunction> filterFunctionWithoutInput;
    private final Map<Integer, Integer> filterFunctionInputMapping;   // channel-to-index-into-hiveColumnIndices-array mapping
    private final Map<Integer, Integer> columnsWithFilterScores;      // keys are indices into hiveColumnIndices array; values are filter scores

    private final OrcLocalMemoryContext localMemoryContext;

    // Optimal order of stream readers
    private int[] streamReaderOrder;                                  // elements are indices into hiveColumnIndices array

    // aligned with streamReaderOrder order; each filter function is placed
    // into a list positioned at the last necessary input
    private List<FilterFunctionWithStats>[] filterFunctionsOrder;

    private Set<Integer>[] filterFunctionInputs;                      // aligned with filterFunctionsOrder
    private boolean reorderFilters;
    private int[] reorderableColumns;                                 // values are hiveColumnIndices

    // non-deterministic filter functions with only constant inputs; evaluated before any column is read
    private List<FilterFunctionWithStats> filterFunctionsWithConstantInputs;
    private Set<Integer> filterFunctionConstantInputs;

    // An immutable list of initial positions; includes all positions: 0,1,2,3,4,..
    // This array may grow, but cannot shrink. The values don't change.
    private int[] positions;

    // Used in applyFilterFunctions; mutable
    private int[] outputPositions;

    // errors encountered while evaluating filter functions; indices are positions in the batch
    // of rows being processed by getNextPage (errors[outputPositions[i]] is valid)
    private RuntimeException[] errors;

    // temporary array to be used in applyFilterFunctions only; exists solely for the purpose of re-using memory
    // indices are positions in a page provided to the filter filters (it contains a subset of rows that passed earlier filters)
    private RuntimeException[] tmpErrors;

    // flag indicating whether range filter on a constant column is false; no data is read in that case
    private boolean constantFilterIsFalse;

    // an error occurred while evaluating deterministic filter function with only constant
    // inputs; thrown unless other filters eliminate all rows
    @Nullable
    private RuntimeException constantFilterError;

    private int readPositions;

    public OrcSelectiveRecordReader(
            Map<Integer, Type> includedColumns,                 // key: hiveColumnIndex
            List<Integer> outputColumns,                        // elements are hive column indices
            Map<Integer, Map<Subfield, TupleDomainFilter>> filters, // key: hiveColumnIndex
            List<FilterFunction> filterFunctions,
            Map<Integer, Integer> filterFunctionInputMapping,   // channel-to-hiveColumnIndex mapping for all filter function inputs
            Map<Integer, List<Subfield>> requiredSubfields,     // key: hiveColumnIndex
            Map<Integer, Object> constantValues,                // key: hiveColumnIndex
            Map<Integer, Function<Block, Block>> coercers,      // key: hiveColumnIndex
            OrcPredicate predicate,
            long numberOfRows,
            List<StripeInformation> fileStripes,
            List<ColumnStatistics> fileStats,
            List<StripeStatistics> stripeStats,
            OrcDataSource orcDataSource,
            long offset,
            long length,
            List<OrcType> types,
            Optional<OrcDecompressor> decompressor,
            Optional<EncryptionLibrary> encryptionLibrary,
            Map<Integer, Integer> dwrfEncryptionGroupMap,
            Map<Integer, Slice> intermediateKeyMetadata,
            int rowsInRowGroup,
            DateTimeZone hiveStorageTimeZone,
            OrcRecordReaderOptions options,
            boolean legacyMapSubscript,
            PostScript.HiveWriterVersion hiveWriterVersion,
            MetadataReader metadataReader,
            Map<String, Slice> userMetadata,
            OrcAggregatedMemoryContext systemMemoryUsage,
            Optional<OrcWriteValidation> writeValidation,
            int initialBatchSize,
            StripeMetadataSource stripeMetadataSource,
            boolean cacheable,
            RuntimeStats runtimeStats)
    {
        super(includedColumns,
                requiredSubfields,
                createStreamReaders(
                        orcDataSource,
                        types,
                        hiveStorageTimeZone,
                        options,
                        legacyMapSubscript,
                        includedColumns,
                        outputColumns,
                        filters,
                        filterFunctions,
                        filterFunctionInputMapping,
                        requiredSubfields,
                        systemMemoryUsage.newOrcAggregatedMemoryContext()),
                predicate,
                numberOfRows,
                fileStripes,
                fileStats,
                stripeStats,
                orcDataSource,
                offset,
                length,
                types,
                decompressor,
                encryptionLibrary,
                dwrfEncryptionGroupMap,
                intermediateKeyMetadata,
                rowsInRowGroup,
                hiveStorageTimeZone,
                hiveWriterVersion,
                metadataReader,
                options.getMaxMergeDistance(),
                options.getTinyStripeThreshold(),
                options.getMaxBlockSize(),
                userMetadata,
                systemMemoryUsage,
                writeValidation,
                initialBatchSize,
                stripeMetadataSource,
                cacheable,
                runtimeStats);

        // Hive column indices can't be used to index into arrays because they are negative
        // for partition and hidden columns. Hence, we create synthetic zero-based indices.

        List<Integer> hiveColumnIndices = ImmutableList.copyOf(includedColumns.keySet());
        Map<Integer, Integer> zeroBasedIndices = IntStream.range(0, hiveColumnIndices.size())
                .boxed()
                .collect(toImmutableMap(hiveColumnIndices::get, Function.identity()));

        this.hiveColumnIndices = hiveColumnIndices.stream().mapToInt(i -> i).toArray();
        this.outputColumns = outputColumns.stream().map(zeroBasedIndices::get).collect(toImmutableList());
        this.columnTypes = includedColumns.entrySet().stream().collect(toImmutableMap(entry -> zeroBasedIndices.get(entry.getKey()), Map.Entry::getValue));
        this.filterFunctionWithoutInput = getFilterFunctionWithoutInputs(filterFunctions);

        Set<Integer> usedInputChannels = filterFunctions.stream()
                .flatMapToInt(function -> Arrays.stream(function.getInputChannels()))
                .boxed()
                .collect(toImmutableSet());
        this.filterFunctionInputMapping = Maps.transformValues(Maps.filterKeys(filterFunctionInputMapping, usedInputChannels::contains), zeroBasedIndices::get);
        this.columnsWithFilterScores = filters
                .entrySet()
                .stream()
                .collect(toImmutableMap(entry -> zeroBasedIndices.get(entry.getKey()), entry -> scoreFilter(entry.getValue())));

        this.localMemoryContext = systemMemoryUsage.newOrcLocalMemoryContext(OrcSelectiveRecordReader.class.getSimpleName());

        requireNonNull(coercers, "coercers is null");
        this.coercers = new Function[this.hiveColumnIndices.length];
        for (Map.Entry<Integer, Function<Block, Block>> entry : coercers.entrySet()) {
            this.coercers[zeroBasedIndices.get(entry.getKey())] = entry.getValue();
        }

        requireNonNull(constantValues, "constantValues is null");
        this.constantValues = new Object[this.hiveColumnIndices.length];
        for (int columnIndex : includedColumns.keySet()) {
            if (!isColumnPresent(columnIndex)) {
                // Any filter not true of null on a missing column
                // fails the whole split. Filters on prefilled columns
                // are already evaluated, hence we only check filters
                // for missing columns here.
                if (columnIndex >= 0 && containsNonNullFilter(filters.get(columnIndex))) {
                    constantFilterIsFalse = true;
                    // No further initialization needed.
                    return;
                }
                this.constantValues[zeroBasedIndices.get(columnIndex)] = NULL_MARKER;
            }
        }

        for (Map.Entry<Integer, Object> entry : constantValues.entrySet()) {
            // all included columns will be null, the constant columns should have a valid predicate or null marker so that there is no streamReader created below
            if (entry.getValue() != null) {
                this.constantValues[zeroBasedIndices.get(entry.getKey())] = entry.getValue();
            }
        }

        if (!evaluateDeterministicFilterFunctionsWithConstantInputs(filterFunctions)) {
            constantFilterIsFalse = true;
            // No further initialization needed.
            return;
        }

        // Initial order of stream readers is:
        //  - readers with integer equality
        //  - readers with integer range / multivalues / inequality
        //  - readers with filters
        //  - followed by readers for columns that provide input to filter functions
        //  - followed by readers for columns that doesn't have any filtering
        streamReaderOrder = orderStreamReaders(columnTypes.keySet().stream().filter(index -> this.constantValues[index] == null).collect(toImmutableSet()), columnsWithFilterScores, this.filterFunctionInputMapping.keySet(), columnTypes);

        List<FilterFunction> filterFunctionsWithInputs = filterFunctions.stream()
                .filter(OrcSelectiveRecordReader::hasInputs)
                .filter(not(this::allConstantInputs))
                .collect(toImmutableList());

        // figure out when to evaluate filter functions; a function is ready for evaluation as soon as the last input has been read
        List<FilterFunctionWithStats> filterFunctionsWithStats = filterFunctionsWithInputs.stream()
                .map(function -> new FilterFunctionWithStats(function, new FilterStats()))
                .collect(toImmutableList());
        filterFunctionsOrder = orderFilterFunctionsWithInputs(streamReaderOrder, filterFunctionsWithStats, this.filterFunctionInputMapping);
        filterFunctionInputs = collectFilterFunctionInputs(filterFunctionsOrder, this.filterFunctionInputMapping);
        reorderableColumns = Arrays.stream(streamReaderOrder)
                .filter(columnIndex -> !columnsWithFilterScores.containsKey(columnIndex))
                .filter(this.filterFunctionInputMapping::containsKey)
                .toArray();
        reorderFilters = filterFunctionsWithStats.size() > 1 && reorderableColumns.length > 1;

        filterFunctionsWithConstantInputs = filterFunctions.stream()
                .filter(not(FilterFunction::isDeterministic))
                .filter(OrcSelectiveRecordReader::hasInputs)
                .filter(this::allConstantInputs)
                .map(function -> new FilterFunctionWithStats(function, new FilterStats()))
                .collect(toImmutableList());
        filterFunctionConstantInputs = filterFunctionsWithConstantInputs.stream()
                .flatMapToInt(function -> Arrays.stream(function.getFunction().getInputChannels()))
                .boxed()
                .map(this.filterFunctionInputMapping::get)
                .collect(toImmutableSet());
    }

    private boolean evaluateDeterministicFilterFunctionsWithConstantInputs(List<FilterFunction> filterFunctions)
    {
        for (FilterFunction function : filterFunctions) {
            if (function.isDeterministic() && hasInputs(function) && allConstantInputs(function) && !evaluateDeterministicFilterFunctionWithConstantInputs(function)) {
                return false;
            }
        }
        return true;
    }

    private boolean evaluateDeterministicFilterFunctionWithConstantInputs(FilterFunction function)
    {
        int[] inputs = function.getInputChannels();
        Block[] blocks = new Block[inputs.length];
        for (int i = 0; i < inputs.length; i++) {
            int columnIndex = filterFunctionInputMapping.get(inputs[i]);
            Object constantValue = constantValues[columnIndex];
            blocks[i] = RunLengthEncodedBlock.create(columnTypes.get(columnIndex), constantValue == NULL_MARKER ? null : constantValue, 1);
        }

        initializeTmpErrors(1);
        int positionCount = function.filter(new Page(blocks), new int[] {0}, 1, tmpErrors);

        if (tmpErrors[0] != null) {
            constantFilterError = tmpErrors[0];
        }
        return positionCount == 1;
    }

    private static boolean hasInputs(FilterFunction function)
    {
        return function.getInputChannels().length > 0;
    }

    private boolean allConstantInputs(FilterFunction function)
    {
        return Arrays.stream(function.getInputChannels())
                .map(filterFunctionInputMapping::get)
                .allMatch(columnIndex -> constantValues[columnIndex] != null);
    }

    private void reorderFiltersIfNeeded()
    {
        List<FilterFunctionWithStats> filters = Arrays.stream(filterFunctionsOrder)
                .filter(Objects::nonNull)
                .flatMap(functions -> functions.stream())
                .sorted(Comparator.comparingDouble(function -> function.getStats().getElapsedNanonsPerDroppedPosition()))
                .collect(toImmutableList());

        assert filters.size() > 1;

        Map<Integer, Integer> columnScore = new HashMap<>();
        for (int i = 0; i < filters.size(); i++) {
            int score = i;
            Arrays.stream(filters.get(i).getFunction().getInputChannels())
                    .map(filterFunctionInputMapping::get)
                    // exclude columns with range filters
                    .filter(columnIndex -> !columnsWithFilterScores.containsKey(columnIndex))
                    // exclude constant columns
                    .filter(columnIndex -> constantValues[columnIndex] == null)
                    .forEach(columnIndex -> columnScore.compute(columnIndex, (k, v) -> v == null ? score : min(score, v)));
        }

        int[] newColumnOrder = columnScore.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getValue))
                .mapToInt(Map.Entry::getKey)
                .toArray();

        // Update streamReaderOrder,
        // filterFunctionsOrder (aligned with streamReaderOrder),
        // filterFunctionInputs (aligned with filterFunctionsOrder)
        boolean sameOrder = true;
        for (int i = 0; i < streamReaderOrder.length; i++) {
            if (!columnsWithFilterScores.containsKey(streamReaderOrder[i])) {
                for (int j = 0; j < newColumnOrder.length; j++) {
                    if (streamReaderOrder[i] != newColumnOrder[j]) {
                        sameOrder = false;
                    }
                    streamReaderOrder[i++] = newColumnOrder[j];
                }
                break;
            }
        }

        if (!sameOrder) {
            filterFunctionsOrder = orderFilterFunctionsWithInputs(streamReaderOrder, filters, this.filterFunctionInputMapping);
            filterFunctionInputs = collectFilterFunctionInputs(filterFunctionsOrder, this.filterFunctionInputMapping);
        }
    }

    private static List<FilterFunctionWithStats>[] orderFilterFunctionsWithInputs(int[] streamReaderOrder, List<FilterFunctionWithStats> filterFunctions, Map<Integer, Integer> inputMapping)
    {
        List<FilterFunctionWithStats>[] order = new List[streamReaderOrder.length];
        for (FilterFunctionWithStats function : filterFunctions) {
            int[] inputs = function.getFunction().getInputChannels();
            int lastIndex = -1;
            for (int input : inputs) {
                int columnIndex = inputMapping.get(input);
                lastIndex = max(lastIndex, Ints.indexOf(streamReaderOrder, columnIndex));
            }

            verify(lastIndex >= 0);
            if (order[lastIndex] == null) {
                order[lastIndex] = new ArrayList<>();
            }
            order[lastIndex].add(function);
        }

        return order;
    }

    private static Set<Integer>[] collectFilterFunctionInputs(List<FilterFunctionWithStats>[] functionsOrder, Map<Integer, Integer> inputMapping)
    {
        Set<Integer>[] inputs = new Set[functionsOrder.length];
        for (int i = 0; i < functionsOrder.length; i++) {
            List<FilterFunctionWithStats> functions = functionsOrder[i];
            if (functions != null) {
                inputs[i] = functions.stream()
                        .flatMapToInt(function -> Arrays.stream(function.getFunction().getInputChannels()))
                        .boxed()
                        .map(inputMapping::get)
                        .collect(toImmutableSet());
            }
        }

        return inputs;
    }

    private static Optional<FilterFunction> getFilterFunctionWithoutInputs(List<FilterFunction> filterFunctions)
    {
        List<FilterFunction> functions = filterFunctions.stream()
                .filter(not(OrcSelectiveRecordReader::hasInputs))
                .collect(toImmutableList());
        if (functions.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(Iterables.getOnlyElement(functions));
    }

    private static boolean containsNonNullFilter(Map<Subfield, TupleDomainFilter> columnFilters)
    {
        return columnFilters != null && !columnFilters.values().stream().allMatch(TupleDomainFilter::testNull);
    }

    private static int scoreFilter(Map<Subfield, TupleDomainFilter> filters)
    {
        checkArgument(!filters.isEmpty());

        if (filters.size() > 1) {
            // Complex type column. Complex types are expensive!
            return 1000;
        }

        Map.Entry<Subfield, TupleDomainFilter> filterEntry = Iterables.getOnlyElement(filters.entrySet());
        if (!filterEntry.getKey().getPath().isEmpty()) {
            // Complex type column. Complex types are expensive!
            return 1000;
        }

        TupleDomainFilter filter = filterEntry.getValue();
        if (filter instanceof BigintRange) {
            if (((BigintRange) filter).isSingleValue()) {
                // Integer equality. Generally cheap.
                return 10;
            }
            return 50;
        }

        if (filter instanceof BigintValuesUsingHashTable || filter instanceof BigintValuesUsingBitmask || filter instanceof BigintMultiRange) {
            return 50;
        }

        return 100;
    }

    private static int scoreType(Type type)
    {
        if (type == BOOLEAN) {
            return 10;
        }

        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT || type == TIMESTAMP || type == DATE) {
            return 20;
        }

        if (type == REAL || type == DOUBLE) {
            return 30;
        }

        if (type instanceof DecimalType) {
            return 40;
        }

        if (isVarcharType(type) || type instanceof CharType) {
            return 50;
        }

        return 100;
    }

    private static int[] orderStreamReaders(
            Collection<Integer> columnIndices,
            Map<Integer, Integer> columnToScore,
            Set<Integer> filterFunctionInputs,
            Map<Integer, Type> columnTypes)
    {
        List<Integer> sortedColumnsByFilterScore = columnToScore.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .collect(toImmutableList());

        int[] order = new int[columnIndices.size()];
        int i = 0;
        for (int columnIndex : sortedColumnsByFilterScore) {
            if (columnIndices.contains(columnIndex)) {
                order[i++] = columnIndex;
            }
        }

        // read primitive types first
        List<Integer> sortedFilterFunctionInputs = filterFunctionInputs.stream()
                .collect(toImmutableMap(Function.identity(), columnIndex -> scoreType(columnTypes.get(columnIndex))))
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .collect(toImmutableList());

        for (int columnIndex : sortedFilterFunctionInputs) {
            if (columnIndices.contains(columnIndex) && !sortedColumnsByFilterScore.contains(columnIndex)) {
                order[i++] = columnIndex;
            }
        }

        for (int columnIndex : columnIndices) {
            if (!sortedColumnsByFilterScore.contains(columnIndex) && !filterFunctionInputs.contains(columnIndex)) {
                order[i++] = columnIndex;
            }
        }

        return order;
    }

    private static SelectiveStreamReader[] createStreamReaders(
            OrcDataSource orcDataSource,
            List<OrcType> types,
            DateTimeZone hiveStorageTimeZone,
            OrcRecordReaderOptions options,
            boolean legacyMapSubscript,
            Map<Integer, Type> includedColumns,
            List<Integer> outputColumns,
            Map<Integer, Map<Subfield, TupleDomainFilter>> filters,
            List<FilterFunction> filterFunctions,
            Map<Integer, Integer> filterFunctionInputMapping,
            Map<Integer, List<Subfield>> requiredSubfields,
            OrcAggregatedMemoryContext systemMemoryContext)
    {
        List<StreamDescriptor> streamDescriptors = createStreamDescriptor("", "", 0, types, orcDataSource).getNestedStreams();

        requireNonNull(filterFunctions, "filterFunctions is null");
        requireNonNull(filterFunctionInputMapping, "filterFunctionInputMapping is null");

        Set<Integer> filterFunctionInputColumns = filterFunctions.stream()
                .flatMapToInt(function -> Arrays.stream(function.getInputChannels()))
                .boxed()
                .map(filterFunctionInputMapping::get)
                .collect(toImmutableSet());

        OrcType rowType = types.get(0);
        SelectiveStreamReader[] streamReaders = new SelectiveStreamReader[rowType.getFieldCount()];
        for (int columnId = 0; columnId < rowType.getFieldCount(); columnId++) {
            if (includedColumns.containsKey(columnId)) {
                StreamDescriptor streamDescriptor = streamDescriptors.get(columnId);
                boolean outputRequired = outputColumns.contains(columnId) || filterFunctionInputColumns.contains(columnId);
                streamReaders[columnId] = createStreamReader(
                        streamDescriptor,
                        Optional.ofNullable(filters.get(columnId)).orElse(ImmutableMap.of()),
                        outputRequired ? Optional.of(includedColumns.get(columnId)) : Optional.empty(),
                        Optional.ofNullable(requiredSubfields.get(columnId)).orElse(ImmutableList.of()),
                        hiveStorageTimeZone,
                        options,
                        legacyMapSubscript,
                        systemMemoryContext);
            }
        }
        return streamReaders;
    }

    public int getReadPositions()
    {
        return readPositions;
    }

    public Page getNextPage()
            throws IOException
    {
        if (constantFilterIsFalse) {
            return null;
        }
        int batchSize = prepareNextBatch();
        if (batchSize < 0) {
            return null;
        }

        readPositions += batchSize;
        initializePositions(batchSize);

        int[] positionsToRead = this.positions;
        int positionCount = batchSize;

        if (filterFunctionWithoutInput.isPresent()) {
            positionCount = applyFilterFunctionWithNoInputs(positionCount);

            if (positionCount == 0) {
                batchRead(batchSize);
                return EMPTY_PAGE;
            }

            positionsToRead = outputPositions;
        }

        if (!filterFunctionsWithConstantInputs.isEmpty()) {
            positionCount = applyFilterFunctions(filterFunctionsWithConstantInputs, filterFunctionConstantInputs, positionsToRead, positionCount);

            if (positionCount == 0) {
                batchRead(batchSize);
                return EMPTY_PAGE;
            }

            positionsToRead = outputPositions;
        }

        int offset = getNextRowInGroup();

        if (reorderFilters && offset >= MAX_BATCH_SIZE) {
            reorderFiltersIfNeeded();
        }

        for (int i = 0; i < streamReaderOrder.length; i++) {
            int columnIndex = streamReaderOrder[i];

            if (!hasAnyFilter(columnIndex)) {
                break;
            }

            SelectiveStreamReader streamReader = getStreamReader(columnIndex);
            positionCount = streamReader.read(offset, positionsToRead, positionCount);
            if (positionCount == 0) {
                break;
            }

            positionsToRead = streamReader.getReadPositions();
            verify(positionCount == 1 || positionsToRead[positionCount - 1] - positionsToRead[0] >= positionCount - 1, "positions must monotonically increase");

            if (filterFunctionsOrder[i] != null) {
                positionCount = applyFilterFunctions(filterFunctionsOrder[i], filterFunctionInputs[i], positionsToRead, positionCount);
                if (positionCount == 0) {
                    break;
                }

                positionsToRead = outputPositions;
            }
        }

        localMemoryContext.setBytes(getSelfRetainedSizeInBytes());

        batchRead(batchSize);

        if (positionCount == 0) {
            return EMPTY_PAGE;
        }

        if (constantFilterError != null) {
            throw constantFilterError;
        }

        for (int i = 0; i < positionCount; i++) {
            if (errors[positionsToRead[i]] != null) {
                throw errors[positionsToRead[i]];
            }
        }

        for (SelectiveStreamReader reader : getStreamReaders()) {
            if (reader != null) {
                reader.throwAnyError(positionsToRead, positionCount);
            }
        }

        Block[] blocks = new Block[outputColumns.size()];
        for (int i = 0; i < outputColumns.size(); i++) {
            int columnIndex = outputColumns.get(i);
            if (constantValues[columnIndex] != null) {
                blocks[i] = RunLengthEncodedBlock.create(columnTypes.get(columnIndex), constantValues[columnIndex] == NULL_MARKER ? null : constantValues[columnIndex], positionCount);
            }
            else if (!hasAnyFilter(columnIndex)) {
                blocks[i] = new LazyBlock(positionCount, new OrcBlockLoader(columnIndex, offset, positionsToRead, positionCount));
            }
            else {
                Block block = getStreamReader(columnIndex).getBlock(positionsToRead, positionCount);
                updateMaxCombinedBytesPerRow(hiveColumnIndices[columnIndex], block);

                if (coercers[columnIndex] != null) {
                    block = coercers[columnIndex].apply(block);
                }

                blocks[i] = block;
            }
        }

        Page page = new Page(positionCount, blocks);

        validateWritePageChecksum(page);

        return page;
    }

    private long getSelfRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                sizeOf(NULL_MARKER) +
                sizeOf(hiveColumnIndices) +
                sizeOf(constantValues) +
                sizeOf(coercers) +
                sizeOf(streamReaderOrder) +
                sizeOf(filterFunctionsOrder) +
                sizeOf(positions) +
                sizeOf(outputPositions) +
                sizeOf(errors) +
                sizeOf(tmpErrors);
    }

    private SelectiveStreamReader getStreamReader(int columnIndex)
    {
        return getStreamReaders()[hiveColumnIndices[columnIndex]];
    }

    private boolean hasAnyFilter(int columnIndex)
    {
        return columnsWithFilterScores.containsKey(columnIndex) || filterFunctionInputMapping.containsKey(columnIndex);
    }

    private void initializePositions(int batchSize)
    {
        if (positions == null || positions.length < batchSize) {
            positions = new int[batchSize];
            for (int i = 0; i < batchSize; i++) {
                positions[i] = i;
            }
        }

        if (errors == null || errors.length < batchSize) {
            errors = new RuntimeException[batchSize];
        }
        else {
            Arrays.fill(errors, null);
        }
    }

    private int applyFilterFunctionWithNoInputs(int positionCount)
    {
        initializeOutputPositions(positionCount);
        Page page = new Page(positionCount);
        return filterFunctionWithoutInput.get().filter(page, outputPositions, positionCount, errors);
    }

    private int applyFilterFunctions(List<FilterFunctionWithStats> filterFunctions, Set<Integer> filterFunctionInputs, int[] positions, int positionCount)
    {
        BlockLease[] blockLeases = new BlockLease[hiveColumnIndices.length];
        Block[] blocks = new Block[hiveColumnIndices.length];
        for (int columnIndex : filterFunctionInputs) {
            if (constantValues[columnIndex] != null) {
                blocks[columnIndex] = RunLengthEncodedBlock.create(columnTypes.get(columnIndex), constantValues[columnIndex] == NULL_MARKER ? null : constantValues[columnIndex], positionCount);
            }
            else {
                blockLeases[columnIndex] = getStreamReader(columnIndex).getBlockView(positions, positionCount);
                Block block = blockLeases[columnIndex].get();
                if (coercers[columnIndex] != null) {
                    block = coercers[columnIndex].apply(block);
                }
                blocks[columnIndex] = block;
            }
        }

        initializeTmpErrors(positionCount);
        for (int i = 0; i < positionCount; i++) {
            tmpErrors[i] = errors[positions[i]];
        }

        Arrays.fill(errors, null);

        try {
            initializeOutputPositions(positionCount);

            for (int i = 0; i < filterFunctions.size(); i++) {
                FilterFunctionWithStats functionWithStats = filterFunctions.get(i);

                FilterFunction function = functionWithStats.getFunction();
                int[] inputs = function.getInputChannels();
                Block[] inputBlocks = new Block[inputs.length];

                for (int j = 0; j < inputs.length; j++) {
                    inputBlocks[j] = blocks[filterFunctionInputMapping.get(inputs[j])];
                }

                Page page = new Page(positionCount, inputBlocks);
                long startTime = System.nanoTime();
                int inputPositionCount = positionCount;
                positionCount = function.filter(page, outputPositions, positionCount, tmpErrors);
                functionWithStats.getStats().update(inputPositionCount, positionCount, System.nanoTime() - startTime);

                if (positionCount == 0) {
                    break;
                }
            }

            // at this point outputPositions are relative to page, e.g. they are indices into positions array
            // translate outputPositions to positions relative to the start of the row group,
            // e.g. make outputPositions a subset of positions array
            for (int i = 0; i < positionCount; i++) {
                outputPositions[i] = positions[outputPositions[i]];
                errors[outputPositions[i]] = tmpErrors[i];
            }
            return positionCount;
        }
        finally {
            for (BlockLease blockLease : blockLeases) {
                if (blockLease != null) {
                    blockLease.close();
                }
            }
        }
    }

    private void initializeTmpErrors(int positionCount)
    {
        if (tmpErrors == null || tmpErrors.length < positionCount) {
            tmpErrors = new RuntimeException[positionCount];
        }
        else {
            Arrays.fill(tmpErrors, null);
        }
    }

    private void initializeOutputPositions(int positionCount)
    {
        if (outputPositions == null || outputPositions.length < positionCount) {
            outputPositions = new int[positionCount];
        }

        for (int i = 0; i < positionCount; i++) {
            outputPositions[i] = i;
        }
    }

    @Override
    public void close()
            throws IOException
    {
        super.close();
    }

    private final class OrcBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final SelectiveStreamReader reader;
        @Nullable
        private final Function<Block, Block> coercer;
        private final int columnIndex;
        private final int offset;
        private final int[] positions;
        private final int positionCount;
        private boolean loaded;

        public OrcBlockLoader(int columnIndex, int offset, int[] positions, int positionCount)
        {
            this.reader = requireNonNull(getStreamReader(columnIndex), "reader is null");
            this.coercer = coercers[columnIndex]; // can be null
            this.columnIndex = columnIndex;
            this.offset = offset;
            this.positions = requireNonNull(positions, "positions is null");
            this.positionCount = positionCount;
        }

        @Override
        public final void load(LazyBlock lazyBlock)
        {
            if (loaded) {
                return;
            }

            try {
                reader.read(offset, positions, positionCount);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            Block block = reader.getBlock(positions, positionCount);
            if (coercer != null) {
                block = coercer.apply(block);
            }
            lazyBlock.setBlock(block);

            updateMaxCombinedBytesPerRow(hiveColumnIndices[columnIndex], block);

            loaded = true;
        }
    }

    private static final class FilterFunctionWithStats
    {
        private final FilterFunction function;
        private final FilterStats stats;

        private FilterFunctionWithStats(FilterFunction function, FilterStats stats)
        {
            this.function = function;
            this.stats = stats;
        }

        public FilterFunction getFunction()
        {
            return function;
        }

        public FilterStats getStats()
        {
            return stats;
        }
    }

    private static final class FilterStats
    {
        private long inputPositions;
        private long outputPositions;
        private long elapsedNanos;

        public void update(int inputPositions, int outputPositions, long elapsedNanos)
        {
            this.inputPositions += inputPositions;
            this.outputPositions += outputPositions;
            this.elapsedNanos += elapsedNanos;
        }

        public double getElapsedNanonsPerDroppedPosition()
        {
            return (double) elapsedNanos / (1 + inputPositions - outputPositions);
        }
    }
}
