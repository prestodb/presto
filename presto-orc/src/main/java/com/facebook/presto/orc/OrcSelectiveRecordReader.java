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

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.TupleDomainFilter.BigintMultiRange;
import com.facebook.presto.orc.TupleDomainFilter.BigintRange;
import com.facebook.presto.orc.TupleDomainFilter.BigintValues;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.PostScript;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.StripeStatistics;
import com.facebook.presto.orc.reader.SelectiveStreamReader;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockLease;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.orc.reader.SelectiveStreamReaders.createStreamReader;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class OrcSelectiveRecordReader
        extends AbstractOrcRecordReader<SelectiveStreamReader>
{
    // Marks a SQL null when occurring in constantValues.
    private static final byte[] NULL_MARKER = new byte[0];

    private final int[] hiveColumnIndices;                            // elements are hive column indices
    private final List<Integer> outputColumns;                        // elements are hive column indices
    private final Map<Integer, Type> columnTypes;                     // key: index into hiveColumnIndices array
    private final Object[] constantValues;                            // aligned with hiveColumnIndices array
    private final Function<Block, Block>[] coercers;                   // aligned with hiveColumnIndices array
    private final List<FilterFunction> filterFunctions;
    private final Map<Integer, Integer> filterFunctionInputMapping;   // channel-to-index-into-hiveColumnIndices-array mapping
    private final Set<Integer> filterFunctionInputs;                  // channels
    private final Map<Integer, Integer> columnsWithFilterScores;      // keys are indices into hiveColumnIndices array; values are filter scores

    // Optimal order of stream readers
    private int[] streamReaderOrder;                                  // elements are indices into hiveColumnIndices array

    // An immutable list of initial positions; includes all positions: 0,1,2,3,4,..
    // This array may grow, but cannot shrink. The values don't change.
    private int[] positions;

    // Used in applyFilterFunctions; mutable
    private int[] outputPositions;
    private RuntimeException[] errors;
    private boolean constantFilterIsFalse;

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
            int rowsInRowGroup,
            DateTimeZone hiveStorageTimeZone,
            PostScript.HiveWriterVersion hiveWriterVersion,
            MetadataReader metadataReader,
            DataSize maxMergeDistance,
            DataSize tinyStripeThreshold,
            DataSize maxBlockSize,
            Map<String, Slice> userMetadata,
            AggregatedMemoryContext systemMemoryUsage,
            Optional<OrcWriteValidation> writeValidation,
            int initialBatchSize,
            StripeMetadataSource stripeMetadataSource)
    {
        super(includedColumns,
                createStreamReaders(
                        orcDataSource,
                        types,
                        hiveStorageTimeZone,
                        includedColumns,
                        outputColumns,
                        filters,
                        filterFunctions,
                        filterFunctionInputMapping,
                        requiredSubfields,
                        systemMemoryUsage.newAggregatedMemoryContext()),
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
                rowsInRowGroup,
                hiveStorageTimeZone,
                hiveWriterVersion,
                metadataReader,
                maxMergeDistance,
                tinyStripeThreshold,
                maxBlockSize,
                userMetadata,
                systemMemoryUsage,
                writeValidation,
                initialBatchSize,
                stripeMetadataSource);

        // Hive column indices can't be used to index into arrays because they are negative
        // for partition and hidden columns. Hence, we create synthetic zero-based indices.

        List<Integer> hiveColumnIndices = ImmutableList.copyOf(includedColumns.keySet());
        Map<Integer, Integer> zeroBasedIndices = IntStream.range(0, hiveColumnIndices.size())
                .boxed()
                .collect(toImmutableMap(hiveColumnIndices::get, Function.identity()));

        this.hiveColumnIndices = hiveColumnIndices.stream().mapToInt(i -> i).toArray();
        this.outputColumns = outputColumns.stream().map(zeroBasedIndices::get).collect(toImmutableList());
        this.columnTypes = includedColumns.entrySet().stream().collect(toImmutableMap(entry -> zeroBasedIndices.get(entry.getKey()), Map.Entry::getValue));
        this.filterFunctions = filterFunctions;
        this.filterFunctionInputMapping = Maps.transformValues(filterFunctionInputMapping, zeroBasedIndices::get);
        this.filterFunctionInputs = filterFunctions.stream()
                .flatMapToInt(function -> Arrays.stream(function.getInputChannels()))
                .boxed()
                .map(this.filterFunctionInputMapping::get)
                .collect(toImmutableSet());
        this.columnsWithFilterScores = filters
                .entrySet()
                .stream()
                .collect(toImmutableMap(entry -> zeroBasedIndices.get(entry.getKey()), entry -> scoreFilter(entry.getValue())));

        requireNonNull(coercers, "coercers is null");
        this.coercers = new Function[this.hiveColumnIndices.length];
        for (Map.Entry<Integer, Function<Block, Block>> entry : coercers.entrySet()) {
            checkArgument(!filters.containsKey(entry.getKey()), "Coercions for columns with range filters are not supported yet");
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

        // Initial order of stream readers is:
        //  - readers with integer equality
        //  - readers with integer range / multivalues / inequality
        //  - readers with filters
        //  - followed by readers for columns that provide input to filter functions
        //  - followed by readers for columns that doesn't have any filtering
        streamReaderOrder = orderStreamReaders(columnTypes.keySet().stream().filter(index -> this.constantValues[index] == null).collect(toImmutableSet()), columnsWithFilterScores, filterFunctionInputs);
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

        if (filter instanceof BigintValues || filter instanceof BigintMultiRange) {
            return 50;
        }

        return 100;
    }

    private static int[] orderStreamReaders(Collection<Integer> columnIndices, Map<Integer, Integer> columnToScore, Set<Integer> filterFunctionInputs)
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
        for (int columnIndex : filterFunctionInputs) {
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
            Map<Integer, Type> includedColumns,
            List<Integer> outputColumns,
            Map<Integer, Map<Subfield, TupleDomainFilter>> filters,
            List<FilterFunction> filterFunctions,
            Map<Integer, Integer> filterFunctionInputMapping,
            Map<Integer, List<Subfield>> requiredSubfields,
            AggregatedMemoryContext systemMemoryContext)
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
        boolean filterFunctionsApplied = filterFunctions.isEmpty();
        for (int columnIndex : streamReaderOrder) {
            if (!filterFunctionsApplied && !hasAnyFilter(columnIndex)) {
                positionCount = applyFilterFunctions(positionsToRead, positionCount);
                if (positionCount == 0) {
                    break;
                }

                positionsToRead = outputPositions;
                filterFunctionsApplied = true;
            }

            SelectiveStreamReader streamReader = getStreamReader(columnIndex);
            positionCount = streamReader.read(getNextRowInGroup(), positionsToRead, positionCount);
            if (positionCount == 0) {
                break;
            }

            positionsToRead = streamReader.getReadPositions();
            verify(positionCount == 1 || positionsToRead[positionCount - 1] - positionsToRead[0] >= positionCount - 1, "positions must monotonically increase");
        }

        if (positionCount > 0 && !filterFunctionsApplied) {
            positionCount = applyFilterFunctions(positionsToRead, positionCount);
            positionsToRead = outputPositions;
        }

        batchRead(batchSize);

        if (positionCount == 0) {
            return new Page(0);
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
            else {
                Block block = getStreamReader(columnIndex).getBlock(positionsToRead, positionCount);
                updateMaxCombinedBytesPerRow(columnIndex, block);

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

    private SelectiveStreamReader getStreamReader(int columnIndex)
    {
        return getStreamReaders()[hiveColumnIndices[columnIndex]];
    }

    private boolean hasAnyFilter(int columnIndex)
    {
        return columnsWithFilterScores.containsKey(columnIndex) || filterFunctionInputs.contains(columnIndex);
    }

    private void initializePositions(int batchSize)
    {
        if (positions == null || positions.length < batchSize) {
            positions = new int[batchSize];
            for (int i = 0; i < batchSize; i++) {
                positions[i] = i;
            }
        }
    }

    private int applyFilterFunctions(int[] positions, int positionCount)
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

        try {
            initializeOutputPositions(positionCount);

            for (FilterFunction function : filterFunctions) {
                int[] inputs = function.getInputChannels();
                Block[] inputBlocks = new Block[inputs.length];

                for (int i = 0; i < inputs.length; i++) {
                    inputBlocks[i] = blocks[filterFunctionInputMapping.get(inputs[i])];
                }

                Page page = new Page(positionCount, inputBlocks);
                positionCount = function.filter(page, outputPositions, positionCount, errors);
                if (positionCount == 0) {
                    break;
                }
            }

            for (int i = 0; i < positionCount; i++) {
                if (errors[i] != null) {
                    throw errors[i];
                }
            }

            // at this point outputPositions are relative to page, e.g. they are indices into positions array
            // translate outputPositions to positions relative to the start of the row group,
            // e.g. make outputPositions a subset of positions array
            for (int i = 0; i < positionCount; i++) {
                outputPositions[i] = positions[outputPositions[i]];
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

    private void initializeOutputPositions(int positionCount)
    {
        if (outputPositions == null || outputPositions.length < positionCount) {
            outputPositions = new int[positionCount];
        }

        for (int i = 0; i < positionCount; i++) {
            outputPositions[i] = i;
        }

        if (errors == null || errors.length < positionCount) {
            errors = new RuntimeException[positionCount];
        }
        else {
            Arrays.fill(errors, null);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            for (SelectiveStreamReader streamReader : getStreamReaders()) {
                if (streamReader != null) {
                    closer.register(streamReader::close);
                }
            }
        }

        super.close();
    }
}
