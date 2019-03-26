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
package com.facebook.presto.orc.reader;

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.QualifyingSet;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.ByteArrayInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.orc.stream.RowGroupDictionaryLengthInputStream;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.VariableWidthBlock;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.IN_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_GROUP_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_GROUP_DICTIONARY_LENGTH;
import static com.facebook.presto.orc.reader.SliceStreamReader.computeTruncatedLength;
import static com.facebook.presto.orc.reader.SliceStreamReader.getMaxCodePointCount;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class SliceDictionaryStreamReader
        extends NullWrappingColumnReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceDictionaryStreamReader.class).instanceSize();

    private static final byte[] EMPTY_DICTIONARY_DATA = new byte[0];
    // add one extra entry for null after strip/rowGroup dictionary
    private static final int[] EMPTY_DICTIONARY_OFFSETS = new int[2];

    private static final byte FILTER_NOT_EVALUATED = 0;
    private static final byte FILTER_PASSED = 1;
    private static final byte FILTER_FAILED = 2;

    private final StreamDescriptor streamDescriptor;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);

    private InputStreamSource<ByteArrayInputStream> stripeDictionaryDataStreamSource = missingStreamSource(ByteArrayInputStream.class);
    private boolean stripeDictionaryOpen;
    private int stripeDictionarySize;
    private int[] stripeDictionaryLength = new int[0];
    private byte[] stripeDictionaryData = EMPTY_DICTIONARY_DATA;
    private int[] stripeDictionaryOffsetVector = EMPTY_DICTIONARY_OFFSETS;

    private VariableWidthBlock dictionaryBlock = new VariableWidthBlock(1, wrappedBuffer(EMPTY_DICTIONARY_DATA), EMPTY_DICTIONARY_OFFSETS, Optional.of(new boolean[] {true}));
    private byte[] currentDictionaryData = EMPTY_DICTIONARY_DATA;

    private InputStreamSource<LongInputStream> stripeDictionaryLengthStreamSource = missingStreamSource(LongInputStream.class);

    private InputStreamSource<BooleanInputStream> inDictionaryStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream inDictionaryStream;

    // An indicator per dictionary value of whether the filter has been evaluated and what was the result
    private byte[] filterResults;

    private InputStreamSource<ByteArrayInputStream> rowGroupDictionaryDataStreamSource = missingStreamSource(ByteArrayInputStream.class);

    private InputStreamSource<RowGroupDictionaryLengthInputStream> rowGroupDictionaryLengthStreamSource = missingStreamSource(RowGroupDictionaryLengthInputStream.class);
    private int[] rowGroupDictionaryLength = new int[0];

    private InputStreamSource<LongInputStream> dataStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream dataStream;

    private LocalMemoryContext systemMemoryContext;

    private int[] values;
    private ResultsProcessor resultsProcessor = new ResultsProcessor();
    private int averageResultSize = SIZE_OF_DOUBLE;

    public SliceDictionaryStreamReader(StreamDescriptor streamDescriptor, LocalMemoryContext systemMemoryContext)
    {
        super(OptionalInt.empty());
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public Block readBlock(Type type)
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup(type);
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the length reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (dataStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                if (inDictionaryStream != null) {
                    inDictionaryStream.skip(readOffset);
                }
                dataStream.skip(readOffset);
            }
        }

        int[] idsVector = new int[nextBatchSize];
        if (presentStream == null) {
            // Data doesn't have nulls
            if (dataStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
            }
            if (inDictionaryStream == null) {
                dataStream.nextIntVector(nextBatchSize, idsVector, 0);
            }
            else {
                for (int i = 0; i < nextBatchSize; i++) {
                    idsVector[i] = toIntExact(dataStream.next());
                    if (!inDictionaryStream.nextBit()) {
                        // row group dictionary elements are after the main dictionary
                        idsVector[i] += stripeDictionarySize;
                    }
                }
            }
        }
        else {
            // Data has nulls
            if (dataStream == null) {
                // The only valid case for dataStream is null when data has nulls is that all values are nulls.
                // In that case the only element in the dictionaryBlock is null and the ids in idsVector should
                // be all 0's, so we don't need to update idVector again.
                int nullValues = presentStream.getUnsetBits(nextBatchSize);
                if (nullValues != nextBatchSize) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
            }
            else {
                for (int i = 0; i < nextBatchSize; i++) {
                    if (!presentStream.nextBit()) {
                        // null is the last entry in the slice dictionary
                        idsVector[i] = dictionaryBlock.getPositionCount() - 1;
                    }
                    else {
                        idsVector[i] = toIntExact(dataStream.next());
                        if (inDictionaryStream != null && !inDictionaryStream.nextBit()) {
                            // row group dictionary elements are after the main dictionary
                            idsVector[i] += stripeDictionarySize;
                        }
                    }
                }
            }
        }
        Block block = new DictionaryBlock(nextBatchSize, dictionaryBlock, idsVector);

        readOffset = 0;
        nextBatchSize = 0;
        return block;
    }

    private void setDictionaryBlockData(byte[] dictionaryData, int[] dictionaryOffsets, int positionCount)
    {
        verify(positionCount > 0);
        // only update the block if the array changed to prevent creation of new Block objects, since
        // the engine currently uses identity equality to test if dictionaries are the same
        if (currentDictionaryData != dictionaryData) {
            boolean[] isNullVector = new boolean[positionCount];
            isNullVector[positionCount - 1] = true;
            dictionaryOffsets[positionCount] = dictionaryOffsets[positionCount - 1];
            dictionaryBlock = new VariableWidthBlock(positionCount, wrappedBuffer(dictionaryData), dictionaryOffsets, Optional.of(isNullVector));
            if (deterministicFilter) {
                if (filterResults == null || filterResults.length < positionCount) {
                    filterResults = new byte[positionCount];
                    // the default values are zeros which means FILTER_NOT_EVALUATED
                }
                else {
                    Arrays.fill(filterResults, FILTER_NOT_EVALUATED);
                }
            }
            currentDictionaryData = dictionaryData;
        }
    }

    private void openRowGroup(Type type)
            throws IOException
    {
        // read the dictionary
        if (!stripeDictionaryOpen) {
            if (stripeDictionarySize > 0) {
                // resize the dictionary lengths array if necessary
                if (stripeDictionaryLength.length < stripeDictionarySize) {
                    stripeDictionaryLength = new int[stripeDictionarySize];
                }

                // read the lengths
                LongInputStream lengthStream = stripeDictionaryLengthStreamSource.openStream();
                if (lengthStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Dictionary is not empty but dictionary length stream is not present");
                }
                lengthStream.nextIntVector(stripeDictionarySize, stripeDictionaryLength, 0);

                long dataLength = 0;
                for (int i = 0; i < stripeDictionarySize; i++) {
                    dataLength += stripeDictionaryLength[i];
                }

                // we must always create a new dictionary array because the previous dictionary may still be referenced
                stripeDictionaryData = new byte[toIntExact(dataLength)];
                // add one extra entry for null
                stripeDictionaryOffsetVector = new int[stripeDictionarySize + 2];

                // read dictionary values
                ByteArrayInputStream dictionaryDataStream = stripeDictionaryDataStreamSource.openStream();
                readDictionary(dictionaryDataStream, stripeDictionarySize, stripeDictionaryLength, 0, stripeDictionaryData, stripeDictionaryOffsetVector, type);
            }
            else {
                stripeDictionaryData = EMPTY_DICTIONARY_DATA;
                stripeDictionaryOffsetVector = EMPTY_DICTIONARY_OFFSETS;
            }
        }
        stripeDictionaryOpen = true;

        // read row group dictionary
        RowGroupDictionaryLengthInputStream dictionaryLengthStream = rowGroupDictionaryLengthStreamSource.openStream();
        if (dictionaryLengthStream != null) {
            int rowGroupDictionarySize = dictionaryLengthStream.getEntryCount();

            // resize the dictionary lengths array if necessary
            if (rowGroupDictionaryLength.length < rowGroupDictionarySize) {
                rowGroupDictionaryLength = new int[rowGroupDictionarySize];
            }

            // read the lengths
            dictionaryLengthStream.nextIntVector(rowGroupDictionarySize, rowGroupDictionaryLength, 0);
            long dataLength = 0;
            for (int i = 0; i < rowGroupDictionarySize; i++) {
                dataLength += rowGroupDictionaryLength[i];
            }

            // We must always create a new dictionary array because the previous dictionary may still be referenced
            // The first elements of the dictionary are from the stripe dictionary, then the row group dictionary elements, and then a null
            byte[] rowGroupDictionaryData = Arrays.copyOf(stripeDictionaryData, stripeDictionaryOffsetVector[stripeDictionarySize] + toIntExact(dataLength));
            int[] rowGroupDictionaryOffsetVector = Arrays.copyOf(stripeDictionaryOffsetVector, stripeDictionarySize + rowGroupDictionarySize + 2);

            // read dictionary values
            ByteArrayInputStream dictionaryDataStream = rowGroupDictionaryDataStreamSource.openStream();
            readDictionary(dictionaryDataStream, rowGroupDictionarySize, rowGroupDictionaryLength, stripeDictionarySize, rowGroupDictionaryData, rowGroupDictionaryOffsetVector, type);
            setDictionaryBlockData(rowGroupDictionaryData, rowGroupDictionaryOffsetVector, stripeDictionarySize + rowGroupDictionarySize + 1);
        }
        else {
            // there is no row group dictionary so use the stripe dictionary
            setDictionaryBlockData(stripeDictionaryData, stripeDictionaryOffsetVector, stripeDictionarySize + 1);
        }

        presentStream = presentStreamSource.openStream();
        inDictionaryStream = inDictionaryStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        super.openRowGroup();
    }

    // Reads dictionary into data and offsetVector
    private static void readDictionary(
            @Nullable ByteArrayInputStream dictionaryDataStream,
            int dictionarySize,
            int[] dictionaryLengthVector,
            int offsetVectorOffset,
            byte[] data,
            int[] offsetVector,
            Type type)
            throws IOException
    {
        Slice slice = wrappedBuffer(data);

        // initialize the offset if necessary;
        // otherwise, use the previous offset
        if (offsetVectorOffset == 0) {
            offsetVector[0] = 0;
        }

        // truncate string and update offsets
        for (int i = 0; i < dictionarySize; i++) {
            int offsetIndex = offsetVectorOffset + i;
            int offset = offsetVector[offsetIndex];
            int length = dictionaryLengthVector[i];

            int truncatedLength;
            int maxCodePointCount = getMaxCodePointCount(type);
            boolean isCharType = isCharType(type);
            if (length > 0) {
                // read data without truncation
                dictionaryDataStream.next(data, offset, offset + length);

                // adjust offsets with truncated length
                truncatedLength = computeTruncatedLength(slice, offset, length, maxCodePointCount, isCharType);
                verify(truncatedLength >= 0);
            }
            else {
                truncatedLength = 0;
            }
            offsetVector[offsetIndex + 1] = offsetVector[offsetIndex] + truncatedLength;
        }
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
    {
        stripeDictionaryDataStreamSource = dictionaryStreamSources.getInputStreamSource(streamDescriptor, DICTIONARY_DATA, ByteArrayInputStream.class);
        stripeDictionaryLengthStreamSource = dictionaryStreamSources.getInputStreamSource(streamDescriptor, LENGTH, LongInputStream.class);
        stripeDictionarySize = encoding.get(streamDescriptor.getStreamId())
                .getColumnEncoding(streamDescriptor.getSequence())
                .getDictionarySize();
        stripeDictionaryOpen = false;

        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(LongInputStream.class);

        inDictionaryStreamSource = missingStreamSource(BooleanInputStream.class);
        rowGroupDictionaryLengthStreamSource = missingStreamSource(RowGroupDictionaryLengthInputStream.class);
        rowGroupDictionaryDataStreamSource = missingStreamSource(ByteArrayInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        inDictionaryStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, LongInputStream.class);

        // the "in dictionary" stream signals if the value is in the stripe or row group dictionary
        inDictionaryStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, IN_DICTIONARY, BooleanInputStream.class);
        rowGroupDictionaryLengthStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, ROW_GROUP_DICTIONARY_LENGTH, RowGroupDictionaryLengthInputStream.class);
        rowGroupDictionaryDataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, ROW_GROUP_DICTIONARY, ByteArrayInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        inDictionaryStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    @Override
    public void close()
    {
        systemMemoryContext.close();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public void scan()
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup(type);
        }
        beginScan(presentStream, null);
        ensureValuesCapacity();
        makeInnerQualifyingSet();

        QualifyingSet input = hasNulls ? innerQualifyingSet : inputQualifyingSet;
        if (filter != null) {
            outputQualifyingSet.reset(input.getPositionCount());
        }

        if (input.getPositionCount() > 0) {
            if (filter != null) {
                int numInput = input.getPositionCount();
                outputQualifyingSet.reset(numInput);
                resultsProcessor.reset();
                numInnerResults = dataStream.scan(input.getPositions(), 0, numInput, input.getEnd(), resultsProcessor);
                outputQualifyingSet.setPositionCount(numInnerResults);
            }
            else {
                resultsProcessor.reset();
                numInnerResults = dataStream.scan(input.getPositions(), 0, input.getPositionCount(), input.getEnd(), resultsProcessor);
            }
        }

        if (inDictionaryStream != null) {
            int[] positions;
            if (filter != null) {
                positions = outputQualifyingSet.getPositions();
            }
            else if (hasNulls) {
                positions = innerQualifyingSet.getPositions();
            }
            else {
                positions = inputQualifyingSet.getPositions();
            }
            int prevPosition = hasNulls ? innerPosInRowGroup : posInRowGroup;
            for (int i = 0; i < numInnerResults; i++) {
                inDictionaryStream.skip(positions[i] - prevPosition);
                if (inDictionaryStream.nextBit()) {
                    values[i] += stripeDictionarySize;
                }
            }
        }

        if (hasNulls) {
            innerPosInRowGroup = innerQualifyingSet.getEnd();
        }
        addNullsAfterScan(filter != null ? outputQualifyingSet : inputQualifyingSet, inputQualifyingSet.getEnd());
        if (filter != null) {
            outputQualifyingSet.setEnd(inputQualifyingSet.getEnd());
        }
        if (outputChannelSet) {
            if (numValues > 0) {
                averageResultSize = SIZE_OF_DOUBLE + toIntExact(dictionaryBlock.getSizeInBytes() / numValues);
            }
            else {
                averageResultSize = SIZE_OF_DOUBLE;
            }
        }
        endScan(presentStream);
    }

    private final class ResultsProcessor
            implements LongInputStream.ResultsConsumer
    {
        private int[] offsets;
        private int[] rowNumbers;
        private int[] inputNumbers;
        private int[] rowNumbersOut;
        private int[] inputNumbersOut;
        private int numResults;

        void reset()
        {
            QualifyingSet input = hasNulls ? innerQualifyingSet : inputQualifyingSet;
            offsets = input.getPositions();
            numResults = 0;
            if (filter != null) {
                rowNumbers = inputQualifyingSet.getPositions();
                inputNumbers = hasNulls ? innerQualifyingSet.getInputNumbers() : null;
                rowNumbersOut = outputQualifyingSet.getPositions();
                inputNumbersOut = outputQualifyingSet.getInputNumbers();
            }
            else {
                rowNumbers = null;
                inputNumbers = null;
                rowNumbersOut = null;
                inputNumbersOut = null;
            }
        }

        private boolean test(long value)
        {
            int dictionaryPosition = toIntExact(value);
            if (!deterministicFilter) {
                return evaluateFilter(dictionaryPosition);
            }

            if (filterResults[dictionaryPosition] == FILTER_NOT_EVALUATED) {
                filterResults[dictionaryPosition] = evaluateFilter(dictionaryPosition) ? FILTER_PASSED : FILTER_FAILED;
            }

            return filterResults[dictionaryPosition] == FILTER_PASSED;
        }

        private boolean evaluateFilter(int dictionaryPosition)
        {
            Slice slice = dictionaryBlock.getSlice(dictionaryPosition, 0, dictionaryBlock.getSliceLength(dictionaryPosition));
            return filter.testBytes(slice.getBytes(), 0, slice.length());
        }

        private int adjustValue(long value)
                throws IOException
        {
            int intValue = toIntExact(value);
            if (inDictionaryStream != null && inDictionaryStream.nextBit()) {
                intValue += stripeDictionarySize;
            }
            return intValue;
        }

        @Override
        public boolean consume(int offsetIndex, long value)
                throws IOException
        {
            int intValue = adjustValue(value);
            if (filter != null && !test(intValue)) {
                return false;
            }

            addResult(offsetIndex, intValue);
            return true;
        }

        @Override
        public int consumeRepeated(int offsetIndex, long value, int count)
                throws IOException
        {
            int intValue = adjustValue(value);
            if (filter != null && deterministicFilter && !test(intValue)) {
                return 0;
            }

            int added = 0;
            for (int i = 0; i < count; i++) {
                if (filter != null && !deterministicFilter && !test(intValue)) {
                    continue;
                }
                addResult(offsetIndex + i, intValue);
                added++;
            }
            return added;
        }

        private void addResult(int offsetIndex, int value)
        {
            if (rowNumbersOut != null) {
                if (inputNumbers == null) {
                    rowNumbersOut[numResults] = offsets[offsetIndex];
                    inputNumbersOut[numResults] = offsetIndex;
                }
                else {
                    int outerIdx = inputNumbers[offsetIndex];
                    rowNumbersOut[numResults] = rowNumbers[outerIdx];
                    inputNumbersOut[numResults] = outerIdx;
                }
            }
            if (values != null) {
                values[numResults + numValues] = value;
            }
            ++numResults;
        }
    }

    // TODO Change return type to long
    @Override
    public int getResultSizeInBytes()
    {
        return numValues * SIZE_OF_DOUBLE + toIntExact(dictionaryBlock.getSizeInBytes());
    }

    @Override
    public int getAverageResultSize()
    {
        return averageResultSize;
    }

    @Override
    public Block getBlock(int numFirstRows, boolean mayReuse)
    {
        checkEnoughValues(numFirstRows);

        if (mayReuse) {
            return new DictionaryBlock(numFirstRows, dictionaryBlock, values);
        }
        else {
            int[] idsVector = new int[numFirstRows];
            System.arraycopy(values, 0, idsVector, 0, numFirstRows);
            return new DictionaryBlock(numFirstRows, dictionaryBlock, idsVector);
        }
    }

    @Override
    public void erase(int numFirstRows)
    {
        if (values == null) {
            return;
        }

        if (numValues > numFirstRows) {
            System.arraycopy(values, numFirstRows, values, 0, numValues - numFirstRows);
            if (valueIsNull != null) {
                System.arraycopy(valueIsNull, numFirstRows, valueIsNull, 0, numValues - numFirstRows);
            }
            numValues -= numFirstRows;
        }
        else {
            numValues = 0;
        }
    }

    @Override
    public void compactValues(int[] surviving, int base, int numSurviving)
    {
        if (outputChannelSet) {
            StreamReaders.compactArrays(surviving, base, numSurviving, values, valueIsNull);
            numValues = base + numSurviving;
        }
        compactQualifyingSet(surviving, numSurviving);
    }

    private void ensureValuesCapacity()
    {
        if (!outputChannelSet) {
            return;
        }
        int capacity = numValues + inputQualifyingSet.getPositionCount();
        if (values == null) {
            values = new int[capacity];
        }
        else if (values.length < capacity) {
            values = Arrays.copyOf(values, capacity);
        }
    }

    @Override
    protected void shiftUp(int from, int to)
    {
        values[to] = values[from];
    }

    @Override
    protected void writeNull(int position)
    {
        values[position] = dictionaryBlock.getPositionCount() - 1;
    }
}
