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
package com.facebook.presto.parquet.selectivereader;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockLease;
import com.facebook.presto.common.block.ClosingBlockLease;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.predicate.TupleDomainFilter;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.parquet.DataPage;
import com.facebook.presto.parquet.DictionaryPage;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.batchreader.decoders.Decoders;
import com.facebook.presto.parquet.batchreader.decoders.FlatDefinitionLevelDecoder;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.Int64ValuesDecoder;
import com.facebook.presto.parquet.batchreader.dictionary.Dictionaries;
import com.facebook.presto.parquet.dictionary.Dictionary;
import com.facebook.presto.parquet.reader.ColumnChunkDescriptor;
import com.facebook.presto.parquet.reader.ParquetColumnChunk;
import org.apache.parquet.column.ColumnDescriptor;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static com.facebook.presto.common.array.Arrays.ExpansionFactor.SMALL;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.INITIALIZE;
import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.parquet.batchreader.decoders.Decoders.readFlatPage;
import static com.facebook.presto.parquet.selectivereader.AbstractSelectiveColumnReader.ExecutionMode.ADAPTIVE;
import static com.facebook.presto.parquet.selectivereader.AbstractSelectiveColumnReader.ExecutionMode.BATCH;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.toIntExact;

public class Int64FlatSelectiveColumnReader
        extends AbstractSelectiveColumnReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Int64FlatSelectiveColumnReader.class).instanceSize();

    protected final boolean nullsAllowed;
    @Nullable
    private final TupleDomainFilter tupleDomainFilter;
    private final boolean nonDeterministicFilter;

    @Nullable
    private long[] values;
    @Nullable
    private boolean[] nulls;
    private Dictionary dictionary;
    private DataPage currentDataPage;
    private int remainingPositionsInPage;
    private int streamPositionInBatch;
    private FlatDefinitionLevelDecoder definitionLevelDecoder;
    private Int64ValuesDecoder valuesDecoderForCurrentPage;

    public Int64FlatSelectiveColumnReader(
            ParquetDataSource parquetDataSource,
            ColumnDescriptor descriptor,
            @Nullable Field field,
            @Nullable TupleDomainFilter tupleDomainFilter,
            boolean outputRequired,
            LocalMemoryContext systemMemoryContext)
    {
        super(parquetDataSource, descriptor, field, outputRequired, systemMemoryContext);

        this.tupleDomainFilter = tupleDomainFilter;
        this.nonDeterministicFilter = this.tupleDomainFilter != null && !this.tupleDomainFilter.isDeterministic();
        this.nullsAllowed = (this.tupleDomainFilter == null || this.tupleDomainFilter.testNull()) && field != null && !field.isRequired();
    }

    @Override
    public int read(int[] positions, int positionCount)
            throws IOException
    {
        streamPositionInBatch = 0;
        outputPositionCount = 0;
        hasNulls = false;

        if (!rowgroupOpen) {
            openRowGroup();
        }

        ensureCapacities(positions, positionCount);
        initializeOutputPositions(positions, positionCount);

        // account memory used by values, nulls and outputPositions
        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        int maxPosition = positions[positionCount - 1];
        int pageBegin = 0;  // index of positions array
        while (pageBegin < positionCount) {
            if (remainingPositionsInPage == 0) {
                readNextPage();
            }

            int chunkSize = Math.min(remainingPositionsInPage, maxPosition - streamPositionInBatch + 1);
            int pageEnd = Arrays.binarySearch(positions, pageBegin, positionCount, streamPositionInBatch + remainingPositionsInPage);
            if (pageEnd != -1) {
                // If pageEnd is -1, the first position to process is not in this page.
                pageEnd = pageEnd < 0 ? -pageEnd - 1 : pageEnd;

                assert positions[pageEnd - 1] < streamPositionInBatch + remainingPositionsInPage;
                assert positions[pageEnd - 1] - positions[pageBegin] + 1 <= remainingPositionsInPage;

                int nonNullCountInPage = chunkSize;
                if (mayContainNulls()) {
                    nonNullCountInPage = definitionLevelDecoder.readNext(nulls, streamPositionInBatch, chunkSize);
                }

                int positionCountInPage = pageEnd - pageBegin;
                int totalPositionCountInPage = positions[pageEnd - 1] - positions[pageBegin] + 1;

                if (nonNullCountInPage < chunkSize) {
                    // Has nulls in this page
                    hasNulls |= true;
                    if (tupleDomainFilter == null) {
                        if (totalPositionCountInPage * 0.5f <= positionCountInPage && executionMode.equals(ADAPTIVE) || executionMode.equals(BATCH)) {
                            readPageInBatchWithNullsNoFilter(positions, pageBegin, pageEnd, nonNullCountInPage, positionCountInPage, totalPositionCountInPage);
                        }
                        else {
                            readPageWithNullsNoFilter(positions, pageBegin, pageEnd);
                        }
                    }
                    else {
                        if (totalPositionCountInPage * 0.5f <= positionCountInPage && executionMode.equals(ADAPTIVE) || executionMode.equals(BATCH)) {
                            readPageInBatchWithNullsWithFilter(positions, pageBegin, pageEnd, nonNullCountInPage, totalPositionCountInPage);
                        }
                        else {
                            readPageWithNullsWithFilter(positions, pageBegin, pageEnd);
                        }
                    }
                }
                else {
                    // Does not have nulls in this page
                    if (tupleDomainFilter == null) {
                        if (totalPositionCountInPage * 0.5f <= positionCountInPage && executionMode.equals(ADAPTIVE) || executionMode.equals(BATCH)) {
                            readPageInBatchNoNullsNoFilter(positions, pageBegin, positionCountInPage, totalPositionCountInPage);
                        }
                        else {
                            readPageNoNullsNoFilter(positions, pageBegin, pageEnd);
                        }
                    }
                    else {
                        if (totalPositionCountInPage * 0.5f <= positionCountInPage && executionMode.equals(ADAPTIVE) || executionMode.equals(BATCH)) {
                            readPageInBatchNoNullsWithFilter(positions, pageBegin, positionCountInPage, totalPositionCountInPage);
                        }
                        else {
                            readPageNoNullsWithFilter(positions, pageBegin, pageEnd);
                        }
                    }
                }

                pageBegin = pageEnd;
            }

            remainingPositionsInPage -= chunkSize;
        }
        return outputPositionCount;
    }

    @Override
    public Block getBlock(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");

        if (positionCount == outputPositionCount) {
            return new LongArrayBlock(positionCount, Optional.ofNullable(hasNulls ? Arrays.copyOf(nulls, positionCount) : null), Arrays.copyOf(values, positionCount));
        }

        long[] valuesCopy = new long[positionCount];
        boolean[] nullsCopy = hasNulls ? new boolean[positionCount] : null;

        compactValues(valuesCopy, nullsCopy, positions, positionCount);

        return new LongArrayBlock(positionCount, Optional.ofNullable(nullsCopy), valuesCopy);
    }

    @Override
    public BlockLease getBlockView(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (positionCount != outputPositionCount) {
            compactValues(values, nulls, positions, positionCount);
        }

        return newLease(new LongArrayBlock(positionCount, Optional.ofNullable(hasNulls ? nulls : null), values));
    }

    @Override
    public int[] getReadPositions()
    {
        return outputPositions;
    }

    @Override
    public void close()
    {
        values = null;
        outputPositions = null;
        nulls = null;

        systemMemoryContext.close();
    }

    private void ensureCapacities(int[] positions, int positionCount)
    {
        int totalPositionCount = positions[positionCount - 1] + 1;
        if (outputRequired || tupleDomainFilter != null || executionMode.equals(BATCH)) {
            values = ensureCapacity(values, totalPositionCount, SMALL, INITIALIZE);

            if (mayContainNulls()) {
                nulls = ensureCapacity(nulls, totalPositionCount);
            }
        }
    }

    private boolean mayContainNulls()
    {
        return !field.isRequired() && metadata.getStatistics().getNumNulls() > 0;
    }

    private long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(values) + sizeOf(nulls) + sizeOf(outputPositions);
    }

    private void readPageInBatchWithNullsNoFilter(int[] positions, int begin, int end, int nonNullCount, int positionCount, int totalPositionCount)
            throws IOException
    {
        skipToPosition(positions[begin]);

        valuesDecoderForCurrentPage.readNext(values, outputPositionCount, nonNullCount);

        unpackLongsWithNulls(values, outputPositionCount, nulls, outputPositionCount, totalPositionCount, nonNullCount);
        if (totalPositionCount > positionCount) {
            packLongs(values, outputPositionCount, positions, begin, positionCount);
        }

        outputPositionCount += positionCount;
        streamPositionInBatch += totalPositionCount;
    }

    private void readPageWithNullsNoFilter(int[] positions, int begin, int end)
            throws IOException
    {
        while (begin < end) {
            int position = positions[begin++];
            if (position > streamPositionInBatch) {
                skipToPosition(position);
            }

            if (!nulls[streamPositionInBatch]) {
                values[outputPositionCount++] = valuesDecoderForCurrentPage.readNext();
            }
            else {
                values[outputPositionCount++] = 0;
            }

            streamPositionInBatch++;
        }
    }

    private void readPageInBatchWithNullsWithFilter(int[] positions, int begin, int end, int nonNullCount, int totalPositionCountInPage)
            throws IOException
    {
        skipToPosition(positions[begin]);

        valuesDecoderForCurrentPage.readNext(values, positions[begin], nonNullCount);

        unpackLongsWithNulls(values, positions[begin], nulls, positions[begin], totalPositionCountInPage, nonNullCount);
        evaluateFiltersWithNulls(positions, begin, end);

        streamPositionInBatch += totalPositionCountInPage;
    }

    private void readPageWithNullsWithFilter(int[] positions, int begin, int end)
            throws IOException
    {
        while (begin < end) {
            int position = positions[begin++];
            if (position > streamPositionInBatch) {
                skipInPage(position - streamPositionInBatch);
                streamPositionInBatch = position;
            }

            long value = 0;
            if (nulls[streamPositionInBatch]) {
                if ((nonDeterministicFilter && tupleDomainFilter.testNull()) || nullsAllowed) {
                    if (outputRequired) {
                        nulls[outputPositionCount] = true;
                        values[outputPositionCount] = 0;
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                value = valuesDecoderForCurrentPage.readNext();
                if (tupleDomainFilter.testLong(value)) {
                    if (outputRequired) {
                        nulls[outputPositionCount] = false;
                        values[outputPositionCount] = value;
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            streamPositionInBatch++;
        }
    }

    private void readPageInBatchNoNullsNoFilter(int[] positions, int begin, int positionCount, int totalPositionCount)
            throws IOException
    {
        skipToPosition(positions[begin]);

        valuesDecoderForCurrentPage.readNext(values, outputPositionCount, totalPositionCount);
        if (totalPositionCount > positionCount) {
            packLongs(values, outputPositionCount, positions, begin, positionCount);
        }

        outputPositionCount += positionCount;
        streamPositionInBatch += totalPositionCount;
    }

    private void readPageNoNullsNoFilter(int[] positions, int begin, int end)
            throws IOException
    {
        while (begin < end) {
            int position = positions[begin++];
            skipToPosition(position);

            values[outputPositionCount++] = valuesDecoderForCurrentPage.readNext();
            streamPositionInBatch++;
        }
    }

    private void readPageInBatchNoNullsWithFilter(int[] positions, int begin, int end, int totalPositionCount)
            throws IOException
    {
        skipToPosition(positions[begin]);

        valuesDecoderForCurrentPage.readNext(values, positions[begin], totalPositionCount);
        evaluateFilters(positions, begin, end);

        streamPositionInBatch += totalPositionCount;
    }

    private void readPageNoNullsWithFilter(int[] positions, int begin, int end)
            throws IOException
    {
        while (begin < end) {
            int position = positions[begin++];
            if (position > streamPositionInBatch) {
                skipToPosition(position);
            }

            long value = ((Int64ValuesDecoder) valuesDecoderForCurrentPage).readNext();
            if (tupleDomainFilter.testLong(value)) {
                if (outputRequired) {
                    values[outputPositionCount] = value;
                }
                outputPositions[outputPositionCount] = position;
                outputPositionCount++;
            }
            streamPositionInBatch++;
        }
    }

    private void skipToPosition(int position)
            throws IOException
    {
        if (position > streamPositionInBatch) {
            int valuesToSkip = position - streamPositionInBatch;

            assert remainingPositionsInPage >= valuesToSkip;

            valuesDecoderForCurrentPage.skip(valuesToSkip);
            streamPositionInBatch = position;
        }
    }

    private void openRowGroup()
            throws IOException
    {
        long startingPosition = metadata.getStartingPos();
        int totalSize = toIntExact(metadata.getTotalSize());

        buffer = ensureCapacity(buffer, totalSize);
        dataSource.readFully(startingPosition, buffer);

        ColumnChunkDescriptor columnChunkDescriptor = new ColumnChunkDescriptor(columnDescriptor, metadata, totalSize);
        ParquetColumnChunk columnChunk = new ParquetColumnChunk(columnChunkDescriptor, buffer, 0);
        pageReader = columnChunk.readAllPages();
        checkArgument(pageReader.getTotalValueCount() > 0, "page is empty");

        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
        if (dictionaryPage != null) {
            dictionary = Dictionaries.createDictionary(columnDescriptor, dictionaryPage);
        }
        else {
            dictionary = null;
        }

        rowgroupOpen = true;
    }

    private boolean readNextPage()
    {
        definitionLevelDecoder = null;
        valuesDecoderForCurrentPage = null;
        remainingPositionsInPage = 0;

        currentDataPage = pageReader.readPage();
        if (currentDataPage == null) {
            return false;
        }

        Decoders.FlatDecoders flatDecoders = readFlatPage(currentDataPage, (RichColumnDescriptor) columnDescriptor, dictionary);
        definitionLevelDecoder = flatDecoders.getDefinitionLevelDecoder();
        valuesDecoderForCurrentPage = (Int64ValuesDecoder) flatDecoders.getValuesDecoder();

        remainingPositionsInPage = currentDataPage.getValueCount();
        return true;
    }

    private int evaluateFilters(int[] positions, int begin, int end)
    {
        // TODO: seperate into two loops to see the perf diff
        int initialOutputPositionCount = outputPositionCount;
        for (int i = begin; i < end; i++) {
            int position = positions[i];
            long value = values[position];
            if (tupleDomainFilter.testLong(value)) {
                if (outputRequired) {
                    values[outputPositionCount] = value;
                }
                outputPositions[outputPositionCount] = position;
                outputPositionCount++;
            }
        }
        return outputPositionCount - initialOutputPositionCount;
    }

    private int evaluateFiltersWithNulls(int[] positions, int begin, int end)
    {
        // TODO: seperate into two loops to see the perf diff
        int initialOutputPositionCount = outputPositionCount;
        for (int i = begin; i < end; i++) {
            int position = positions[i];
            long value = 0;
            if (nulls[position]) {
                if ((nonDeterministicFilter && tupleDomainFilter.testNull()) || nullsAllowed) {
                    if (outputRequired) {
                        nulls[outputPositionCount] = true;
                        values[outputPositionCount] = 0;
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                value = values[position];
                if (tupleDomainFilter.testLong(value)) {
                    if (outputRequired) {
                        nulls[outputPositionCount] = false;
                        values[outputPositionCount] = value;
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
        }
        return outputPositionCount - initialOutputPositionCount;
    }

    // TODO: move to presto-common
    private static void packLongs(long[] values, int valuesIndex, int[] positions, int positionsIndex, int positionCount)
    {
        for (int i = 0; i < positionCount; i++) {
            values[valuesIndex + i] = values[positions[positionsIndex + i]];
        }
    }

    private static void unpackLongsWithNulls(long[] values, int valuesIndex, boolean[] nulls, int nullsIndex, int positionCount, int nonNullCount)
    {
        int fromIndex = valuesIndex + nonNullCount - 1;
        int toIndex = valuesIndex + positionCount - 1;
        nullsIndex += positionCount - 1;
        while (toIndex >= valuesIndex) {
            if (!nulls[nullsIndex]) {
                values[toIndex] = values[fromIndex--];
            }
            else {
                values[toIndex] = 0;
            }
            toIndex--;
            nullsIndex--;
        }
    }

    private void compactValues(long[] valuesDestination, @Nullable boolean[] nullsDestination, int[] positions, int positionCount)
    {
        checkArgument(valuesDestination.length >= positionCount, "valuesDestination.length must be greater than " + positionCount);

        int maxOutputPosition = outputPositions[outputPositionCount - 1];
        int positionsIndex = 0;
        int outputPositionsIndex = 0;

        if (hasNulls) {
            while (outputPositionsIndex < maxOutputPosition) {
                valuesDestination[positionsIndex] = values[outputPositionsIndex];
                nullsDestination[positionsIndex] = nulls[outputPositionsIndex];

                if (outputPositions[outputPositionsIndex] == positions[positionsIndex]) {
                    positionsIndex++;
                }
                outputPositionsIndex++;
            }
        }
        else {
            while (outputPositionsIndex < maxOutputPosition) {
                valuesDestination[positionsIndex] = values[outputPositionsIndex];
                if (outputPositions[outputPositionsIndex] == positions[positionsIndex]) {
                    positionsIndex++;
                }
                outputPositionsIndex++;
            }
        }
    }

    private BlockLease newLease(Block block)
    {
        valuesInUse = true;
        return ClosingBlockLease.newLease(block, () -> valuesInUse = false);
    }

    private void skipInPage(int items)
            throws IOException
    {
        checkArgument(items <= remainingPositionsInPage);

        remainingPositionsInPage -= items;
        valuesDecoderForCurrentPage.skip(items);
    }
}
