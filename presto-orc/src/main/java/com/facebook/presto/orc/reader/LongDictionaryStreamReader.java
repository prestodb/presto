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
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.orc.ResizedArrays.newBooleanArrayForReuse;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.IN_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class LongDictionaryStreamReader
        extends AbstractLongStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongDictionaryStreamReader.class).instanceSize();

    private static final byte FILTER_NOT_EVALUATED = 0;
    private static final byte FILTER_PASSED = 1;
    private static final byte FILTER_FAILED = 2;

    private final StreamDescriptor streamDescriptor;

    private int readOffset;
    private int nextBatchSize;

    @Nullable
    private boolean[] nullVector = new boolean[0];

    private InputStreamSource<LongInputStream> dictionaryDataStreamSource = missingStreamSource(LongInputStream.class);
    private int dictionarySize;
    private long[] dictionary = new long[0];
    private boolean[] inDictionaryFlags;

    private InputStreamSource<BooleanInputStream> inDictionaryStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream inDictionaryStream;
    private boolean[] inDictionaryVector = new boolean[0];
    // An indicator per dictionary value of whether the filter has been evaluated and what was the result
    private byte[] filterResults;

    private InputStreamSource<LongInputStream> dataStreamSource;
    @Nullable
    private LongInputStream dataStream;
    private long[] dataVector = new long[0];

    private boolean dictionaryOpen;

    private LocalMemoryContext systemMemoryContext;
    private ResultsProcessor resultsProcessor = new ResultsProcessor();

    public LongDictionaryStreamReader(StreamDescriptor streamDescriptor, LocalMemoryContext systemMemoryContext)
    {
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
            openRowGroup();
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the length reader
                readOffset = presentStream.countBitsSet(readOffset);
            }

            if (inDictionaryStream != null) {
                inDictionaryStream.skip(readOffset);
            }

            if (readOffset > 0) {
                if (dataStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                dataStream.skip(readOffset);
            }
        }

        BlockBuilder builder = type.createBlockBuilder(null, nextBatchSize);

        if (presentStream == null) {
            // Data doesn't have nulls
            if (dataStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
            }
            if (inDictionaryStream == null) {
                for (int i = 0; i < nextBatchSize; i++) {
                    type.writeLong(builder, dictionary[((int) dataStream.next())]);
                }
            }
            else {
                for (int i = 0; i < nextBatchSize; i++) {
                    long id = dataStream.next();
                    if (inDictionaryStream.nextBit()) {
                        type.writeLong(builder, dictionary[(int) id]);
                    }
                    else {
                        type.writeLong(builder, id);
                    }
                }
            }
        }
        else {
            // Data has nulls
            if (dataStream == null) {
                // The only valid case for dataStream is null when data has nulls is that all values are nulls.
                int nullValues = presentStream.getUnsetBits(nextBatchSize);
                if (nullValues != nextBatchSize) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                for (int i = 0; i < nextBatchSize; i++) {
                    builder.appendNull();
                }
            }
            else {
                for (int i = 0; i < nextBatchSize; i++) {
                    if (!presentStream.nextBit()) {
                        builder.appendNull();
                    }
                    else {
                        long id = dataStream.next();
                        if (inDictionaryStream == null || inDictionaryStream.nextBit()) {
                            type.writeLong(builder, dictionary[(int) id]);
                        }
                        else {
                            type.writeLong(builder, id);
                        }
                    }
                }
            }
        }
        readOffset = 0;
        nextBatchSize = 0;
        return builder.build();
    }

    @Override
    protected void openRowGroup()
            throws IOException
    {
        // read the dictionary
        if (!dictionaryOpen && dictionarySize > 0) {
            if (dictionary.length < dictionarySize) {
                dictionary = new long[dictionarySize];
            }

            LongInputStream dictionaryStream = dictionaryDataStreamSource.openStream();
            if (dictionaryStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Dictionary is not empty but data stream is not present");
            }
            dictionaryStream.nextLongVector(dictionarySize, dictionary);
            if (deterministicFilter) {
                if (filterResults == null || filterResults.length < dictionarySize) {
                    filterResults = new byte[dictionarySize];
                    // the default values are zeros which means FILTER_NOT_EVALUATED
                }
                else {
                    Arrays.fill(filterResults, FILTER_NOT_EVALUATED);
                }
            }
            dictionaryOpen = true;
        }
        presentStream = presentStreamSource.openStream();
        inDictionaryStream = inDictionaryStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        super.openRowGroup();
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
    {
        dictionaryDataStreamSource = dictionaryStreamSources.getInputStreamSource(streamDescriptor, DICTIONARY_DATA, LongInputStream.class);
        dictionarySize = encoding.get(streamDescriptor.getStreamId())
                .getColumnEncoding(streamDescriptor.getSequence())
                .getDictionarySize();
        dictionaryOpen = false;

        inDictionaryStreamSource = missingStreamSource(BooleanInputStream.class);
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(LongInputStream.class);

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
        inDictionaryStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, IN_DICTIONARY, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, LongInputStream.class);

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
        nullVector = null;
        dataVector = null;
        inDictionaryVector = null;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(nullVector) + sizeOf(dataVector) + sizeOf(inDictionaryVector);
    }

    @Override
    public void scan()
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }
        beginScan(presentStream, null);
        ensureValuesCapacity();
        makeInnerQualifyingSet();

        QualifyingSet input = hasNulls ? innerQualifyingSet : inputQualifyingSet;
        int numInput = input.getPositionCount();
        if (inDictionaryStream != null) {
            if (inDictionaryFlags == null || inDictionaryFlags.length < numInput) {
                inDictionaryFlags = newBooleanArrayForReuse(numInput);
            }
            int begin = hasNulls ? innerPosInRowGroup : posInRowGroup;
            int end = input.getEnd();
            inDictionaryStream.getSetBits(input.getPositions(), numInput, begin, end - begin, inDictionaryFlags);
        }
        if (filter != null) {
            outputQualifyingSet.reset(numInput);
        }

        if (numInput > 0) {
            resultsProcessor.reset();
            if (filter != null) {
                numInnerResults = dataStream.scan(input.getPositions(), 0, numInput, input.getEnd(), resultsProcessor);
                outputQualifyingSet.setPositionCount(numInnerResults);
            }
            else {
                numInnerResults = dataStream.scan(input.getPositions(), 0, numInput, input.getEnd(), resultsProcessor);
            }
        }

        if (hasNulls) {
            innerPosInRowGroup = innerQualifyingSet.getEnd();
        }
        addNullsAfterScan(filter != null ? outputQualifyingSet : inputQualifyingSet, inputQualifyingSet.getEnd());
        if (filter != null) {
            outputQualifyingSet.setEnd(inputQualifyingSet.getEnd());
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

        private long adjustValue(int offsetIndex, long value)
        {
            if (inDictionaryStream != null) {
                return inDictionaryFlags[offsetIndex] ? dictionary[(int) value] : value;
            }
            return dictionary[(int) value];
        }

        private boolean test(int offsetIndex, long value)
        {
            if (deterministicFilter && (inDictionaryStream == null || inDictionaryFlags[offsetIndex])) {
                if (filterResults[(int) value] == FILTER_NOT_EVALUATED) {
                    filterResults[(int) value] = filter.testLong(dictionary[(int) value]) ? FILTER_PASSED : FILTER_FAILED;
                }
                return filterResults[(int) value] == FILTER_PASSED;
            }
            return filter.testLong(adjustValue(offsetIndex, value));
        }

        @Override
        public boolean consume(int offsetIndex, long value)
        {
            if (filter != null && !test(offsetIndex, value)) {
                return false;
            }

            addResult(offsetIndex, adjustValue(offsetIndex, value));
            return true;
        }

        @Override
        public int consumeRepeated(int offsetIndex, long value, int count)
        {
            if (inDictionaryStream == null && (deterministicFilter || filter == null)) {
                // Common case: One filter and one dictionary lookup
                // is enough if there are no out of dictionary values
                // and the filter is null or deterministic.
                if (filter != null && !test(offsetIndex, value)) {
                    return 0;
                }
                long finalValue = adjustValue(offsetIndex, value);
                for (int i = 0; i < count; i++) {
                    addResult(offsetIndex + i, finalValue);
                }
                return count;
            }
            int added = 0;
            for (int i = 0; i < count; i++) {
                if (filter != null && !test(offsetIndex + i, value)) {
                    continue;
                }
                addResult(offsetIndex + i, adjustValue(offsetIndex, value));
                added++;
            }
            return added;
        }

        private void addResult(int offsetIndex, long value)
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
}
