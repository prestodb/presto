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
package com.facebook.presto.parquet.reader;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockLease;
import com.facebook.presto.common.predicate.FilterFunction;
import com.facebook.presto.common.predicate.TupleDomainFilter;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.parquet.ColumnReaderFactory;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.selectivereader.AbstractSelectiveColumnReader.ExecutionMode;
import com.facebook.presto.parquet.selectivereader.SelectiveColumnReader;
import com.google.common.io.Closer;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ParquetSelectiveReader
        implements Closeable
{
    private static final int MAX_BATCH_SIZE = 1024;
    private static final int INITIAL_BATCH_SIZE = 1;
    private static final int BATCH_SIZE_GROWTH_FACTOR = 2;

    private final ParquetDataSource parquetDataSource;
    private final List<BlockMetaData> blockMetaDataList;    // Elements are Rowgroup meta data
    private final ColumnDescriptor[] columnDescriptors;     // Indexes are channels for all columns to read
    private final SelectiveColumnReader[] columnReaders;    // Indexes are channels for all columns to read
    private final int[] hiveColumnIndexes;                  // Indexes are channels for all columns to read
    private final List<FilterFunction> filterFunctions;
    private final Set<Integer> filterFunctionChannels;      // Elements are all channels involved in any filter functions
    private final Set<Integer> tupleDomainFilterChannels;   // Elements are all channels involved in any tuple domain filters
    private final List<Integer> outputChannels;
    private final AggregatedMemoryContext systemMemoryContext;

    @Nullable
    private BlockMetaData currentBlockMetadata;
    private int currentBlock;
    private long currentGroupRowCount;
    private int nextRowInGroup;
    private int currentBatchSize;
    private int maxBatchSize = MAX_BATCH_SIZE;
    private int nextBatchSize = INITIAL_BATCH_SIZE;

    private int[] positions;
    private int[] outputPositions;      // Used in applyFilterFunctions; mutable
    private RuntimeException[] errors;
    private int[] columnReaderOrder;    // Optimal order of stream readers
    private int readPositions;

    public ParquetSelectiveReader(
            ParquetDataSource parquetDataSource,
            List<BlockMetaData> blockMetaDataList,
            Field[] fields,
            ColumnDescriptor[] columnDescriptors,
            Map<Integer, Map<Subfield, TupleDomainFilter>> tupleDomainFilters,
            List<FilterFunction> filterFunctions,
            List<Integer> outputChannels,
            int[] hiveColumnIndexes,
            AggregatedMemoryContext systemMemoryContext)
    {
        this.parquetDataSource = requireNonNull(parquetDataSource, "dataSource is null");
        this.blockMetaDataList = requireNonNull(blockMetaDataList, "blocks is null");
        this.columnDescriptors = requireNonNull(columnDescriptors, "includedColumns is null");
        this.filterFunctions = requireNonNull(filterFunctions, "filterFunctions is null");
        this.filterFunctionChannels = filterFunctions.stream()
                .flatMapToInt(function -> Arrays.stream(function.getInputChannels()))
                .boxed()
                .collect(toImmutableSet());
        this.tupleDomainFilterChannels = requireNonNull(tupleDomainFilters, "columnsWithTupleDomainFilters is null").keySet();
        this.outputChannels = requireNonNull(outputChannels, "outputColumns is null");
        this.hiveColumnIndexes = requireNonNull(hiveColumnIndexes, "hiveColumnIndexes is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");

        this.columnReaders = createColumnReaders(tupleDomainFilters, fields);

        // Initial order of stream readers is:
        //  - readers with simple filters
        //  - followed by readers for columns that provide input to filter functions
        //  - followed by readers for columns that doesn't have any filtering
        this.columnReaderOrder = orderColumnReaders(columnDescriptors.length, tupleDomainFilterChannels, filterFunctionChannels);
    }

    public Page getNextPage()
            throws IOException
    {
        int batchSize = prepareNextBatch();
        if (batchSize < 0) {
            return null;
        }

        readPositions += batchSize;
        initializePositions(batchSize);

        int[] positionsToRead = positions;
        int positionCount = batchSize;
        boolean filterFunctionsApplied = filterFunctions.isEmpty();
        for (int channel : columnReaderOrder) {
            if (!filterFunctionsApplied && !hasAnyFilter(channel)) {
                positionCount = applyFilterFunctions(positionsToRead, positionCount);
                if (positionCount == 0) {
                    break;
                }

                positionsToRead = outputPositions;
                filterFunctionsApplied = true;
            }

            SelectiveColumnReader columnReader = columnReaders[channel];

            positionCount = columnReader.read(positionsToRead, positionCount);
            if (positionCount == 0) {
                break;
            }

            positionsToRead = columnReader.getReadPositions();
        }

        if (positionCount > 0 && !filterFunctionsApplied) {
            positionCount = applyFilterFunctions(positionsToRead, positionCount);
            positionsToRead = outputPositions;
        }

        nextRowInGroup += batchSize;

        if (positionCount == 0) {
            return new Page(0);
        }

        Block[] blocks = new Block[outputChannels.size()];
        for (int i = 0; i < outputChannels.size(); i++) {
            int columnIndex = outputChannels.get(i);
            Block block = columnReaders[columnIndex].getBlock(positionsToRead, positionCount);
            blocks[i] = block;
            // TODO: update column statistics
        }

        Page page = new Page(positionCount, blocks);

        return page;
    }

    public int getReadPositions()
    {
        return readPositions;
    }

    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(parquetDataSource);
            for (SelectiveColumnReader columnReader : columnReaders) {
                if (columnReader != null) {
                    closer.register(columnReader::close);
                }
            }
        }
    }

    public void setExecutionMode(ExecutionMode executionMode)
    {
        for (int channel : columnReaderOrder) {
            SelectiveColumnReader columnReader = columnReaders[channel];
            columnReader.setExecutionMode(executionMode);
        }
    }

    private SelectiveColumnReader[] createColumnReaders(Map<Integer, Map<Subfield, TupleDomainFilter>> tupleDomainFilters, Field[] fields)
    {
        SelectiveColumnReader[] columnReaders = new SelectiveColumnReader[columnDescriptors.length];
        for (int channel = 0; channel < columnDescriptors.length; channel++) {
            ColumnDescriptor column = columnDescriptors[channel];
            boolean outputRequired = outputChannels.contains(channel) || filterFunctionChannels.contains(channel);
            columnReaders[channel] = ColumnReaderFactory.createSelectiveReader(
                    parquetDataSource,
                    column,
                    fields[channel],
                    tupleDomainFilters.get(channel),
                    outputRequired,
                    systemMemoryContext);
        }

        return columnReaders;
    }

    private static int[] orderColumnReaders(int channelCount, Set<Integer> columnIndexesWithTupleDomainFilters, Set<Integer> filterFunctionChannels)
    {
        int[] order = new int[channelCount];
        int i = 0;
        for (int columnIndex : columnIndexesWithTupleDomainFilters) {
            order[i++] = columnIndex;
        }
        for (int columnIndex : filterFunctionChannels) {
            if (!columnIndexesWithTupleDomainFilters.contains(columnIndex)) {
                order[i++] = columnIndex;
            }
        }
        for (int channel = 0; channel < channelCount; channel++) {
            if (!columnIndexesWithTupleDomainFilters.contains(channel) && !filterFunctionChannels.contains(channel)) {
                order[i++] = channel;
            }
        }

        return order;
    }

    private int prepareNextBatch()
            throws IOException
    {
        // if next row is within the current group return
        if (nextRowInGroup >= currentGroupRowCount) {
            // attempt to advance to next row group
            if (!advanceToNextRowGroup()) {
                return -1;
            }
        }

        // We will grow currentBatchSize by BATCH_SIZE_GROWTH_FACTOR starting from initialBatchSize to maxBatchSize or
        // the number of rows left in this rowgroup, whichever is smaller. maxBatchSize is adjusted according to the
        // block size for every batch and never exceed MAX_BATCH_SIZE. But when the number of rows in the last batch in
        // the current rowgroup is smaller than min(nextBatchSize, maxBatchSize), the nextBatchSize for next batch in
        // the new rowgroup should be grown based on min(nextBatchSize, maxBatchSize) but not by the number of rows in
        // the last batch, i.e. currentGroupRowCount - nextRowInGroup. For example, if the number of rows read for
        // single fixed width column are: 1, 16, 256, 1024, 1024,..., 1024, 256 and the 256 was because there is only
        // 256 rows left in this row group, then the nextBatchSize should be 1024 instead of 512. So we need to grow the
        // nextBatchSize before limiting the currentBatchSize by currentGroupRowCount - nextRowInGroup.
        currentBatchSize = toIntExact(min(nextBatchSize, maxBatchSize));
        nextBatchSize = min(currentBatchSize * BATCH_SIZE_GROWTH_FACTOR, MAX_BATCH_SIZE);
        currentBatchSize = toIntExact(min(currentBatchSize, currentGroupRowCount - nextRowInGroup));
        return currentBatchSize;
    }

    private boolean hasAnyFilter(int channel)
    {
        return tupleDomainFilterChannels.contains(channel) || filterFunctionChannels.contains(channel);
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
        BlockLease[] blockLeases = new BlockLease[columnDescriptors.length];
        Block[] blocks = new Block[columnDescriptors.length];
        for (int channel : filterFunctionChannels) {
            blockLeases[channel] = columnReaders[channel].getBlockView(positions, positionCount);
            blocks[channel] = blockLeases[channel].get();
        }

        try {
            initializeOutputPositions(positionCount);

            for (FilterFunction function : filterFunctions) {
                int[] inputChannels = function.getInputChannels();
                Block[] inputBlocks = new Block[inputChannels.length];

                for (int i = 0; i < inputChannels.length; i++) {
                    inputBlocks[i] = blocks[inputChannels[i]];
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

    private boolean advanceToNextRowGroup()
            throws IOException
    {
        if (currentBlock == blockMetaDataList.size()) {
            return false;
        }
        currentBlockMetadata = blockMetaDataList.get(currentBlock);
        currentBlock = currentBlock + 1;

        nextRowInGroup = 0;
        currentGroupRowCount = currentBlockMetadata.getRowCount();

        updateColumnChunkMetaData();

        return true;
    }

    private void updateColumnChunkMetaData()
            throws IOException
    {
        List<ColumnChunkMetaData> blocks = currentBlockMetadata.getColumns();
        for (int channel = 0; channel < columnReaders.length; channel++) {
            columnReaders[channel].setColumnChunkMetadata(blocks.get(hiveColumnIndexes[channel]));
        }
    }
}
