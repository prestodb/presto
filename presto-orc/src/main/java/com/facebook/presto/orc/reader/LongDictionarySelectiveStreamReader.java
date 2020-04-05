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

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcLocalMemoryContext;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockLease;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.array.Arrays.ensureCapacity;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.IN_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.SelectiveStreamReaders.initializeOutputPositions;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class LongDictionarySelectiveStreamReader
        extends AbstractLongSelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongDictionarySelectiveStreamReader.class).instanceSize();

    // filter evaluation status for dictionary entries; using byte instead of enum for memory efficiency
    private static final byte FILTER_NOT_EVALUATED = 0;
    private static final byte FILTER_PASSED = 1;
    private static final byte FILTER_FAILED = 2;

    private final StreamDescriptor streamDescriptor;
    @Nullable
    private final TupleDomainFilter filter;
    private final boolean nonDeterministicFilter;
    private final boolean nullsAllowed;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<LongInputStream> dictionaryDataStreamSource = missingStreamSource(LongInputStream.class);
    private int dictionarySize;
    private long[] dictionary = new long[0];
    private byte[] dictionaryFilterStatus;

    private InputStreamSource<BooleanInputStream> inDictionaryStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream inDictionaryStream;

    private InputStreamSource<LongInputStream> dataStreamSource;
    @Nullable
    private LongInputStream dataStream;

    private boolean dictionaryOpen;
    private boolean rowGroupOpen;
    private int readOffset;

    private OrcLocalMemoryContext systemMemoryContext;

    public LongDictionarySelectiveStreamReader(
            StreamDescriptor streamDescriptor,
            Optional<TupleDomainFilter> filter,
            Optional<Type> outputType,
            OrcLocalMemoryContext systemMemoryContext)
    {
        super(outputType);
        requireNonNull(filter, "filter is null");
        checkArgument(filter.isPresent() || outputRequired, "filter must be present if output is not required");
        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.filter = filter.orElse(null);
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");

        nonDeterministicFilter = this.filter != null && !this.filter.isDeterministic();
        nullsAllowed = this.filter == null || nonDeterministicFilter || this.filter.testNull();
    }

    @Override
    public int read(int offset, int[] positions, int positionCount)
            throws IOException
    {
        checkArgument(positionCount > 0, "positionCount must be greater than zero");

        if (!rowGroupOpen) {
            openRowGroup();
        }

        prepareNextRead(positionCount, presentStream != null && nullsAllowed);

        outputPositions = initializeOutputPositions(outputPositions, positions, positionCount);

        // account memory used by values, nulls and outputPositions
        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        // TODO In case of all nulls, the stream type will be LongDirect
        int streamPosition;
        if (filter == null) {
            streamPosition = readNoFilter(positions, positionCount);
        }
        else {
            streamPosition = readWithFilter(positions, positionCount);
        }

        readOffset = offset + streamPosition;

        return outputPositionCount;
    }

    private int readNoFilter(int[] positions, int positionCount)
            throws IOException
    {
        // no filter implies output is required
        int streamPosition = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                nulls[i] = true;
                values[i] = 0;
            }
            else {
                long value = dataStream.next();
                if (inDictionaryStream == null || inDictionaryStream.nextBit()) {
                    value = dictionary[(int) value];
                }
                values[i] = value;
                if (presentStream != null) {
                    nulls[i] = false;
                }
            }

            streamPosition++;
        }

        outputPositionCount = positionCount;
        return streamPosition;
    }

    private int readWithFilter(int[] positions, int positionCount)
            throws IOException
    {
        outputPositionCount = 0;
        int streamPosition = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                if ((nonDeterministicFilter && filter.testNull()) || nullsAllowed) {
                    if (outputRequired) {
                        nulls[outputPositionCount] = true;
                        values[outputPositionCount] = 0;
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                boolean filterPassed;
                long value = dataStream.next();
                if (inDictionaryStream == null || inDictionaryStream.nextBit()) {
                    int id = (int) value;
                    value = dictionary[id];

                    if (!nonDeterministicFilter) {
                        if (dictionaryFilterStatus[id] == FILTER_NOT_EVALUATED) {
                            if (filter.testLong(value)) {
                                dictionaryFilterStatus[id] = FILTER_PASSED;
                            }
                            else {
                                dictionaryFilterStatus[id] = FILTER_FAILED;
                            }
                        }
                        filterPassed = dictionaryFilterStatus[id] == FILTER_PASSED;
                    }
                    else {
                        filterPassed = filter.testLong(value);
                    }
                }
                else {
                    filterPassed = filter.testLong(value);
                }
                if (filterPassed) {
                    if (outputRequired) {
                        values[outputPositionCount] = value;
                        if (presentStream != null && nullsAllowed) {
                            nulls[outputPositionCount] = false;
                        }
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }

            streamPosition++;

            outputPositionCount -= filter.getPrecedingPositionsToFail();

            int succeedingPositionsToFail = filter.getSucceedingPositionsToFail();
            if (succeedingPositionsToFail > 0) {
                int positionsToSkip = 0;
                for (int j = 0; j < succeedingPositionsToFail; j++) {
                    i++;
                    int nextPosition = positions[i];
                    positionsToSkip += 1 + nextPosition - streamPosition;
                    streamPosition = nextPosition + 1;
                }
                skip(positionsToSkip);
            }
        }

        return streamPosition;
    }

    private void skip(int items)
            throws IOException
    {
        if (presentStream != null) {
            int dataToSkip = presentStream.countBitsSet(items);
            if (inDictionaryStream != null) {
                inDictionaryStream.skip(dataToSkip);
            }
            if (dataStream != null) {
                dataStream.skip(dataToSkip);
            }
        }
        else {
            if (inDictionaryStream != null) {
                inDictionaryStream.skip(items);
            }
            dataStream.skip(items);
        }
    }

    private void openRowGroup()
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
            if (filter != null && !nonDeterministicFilter) {
                dictionaryFilterStatus = ensureCapacity(dictionaryFilterStatus, dictionarySize);
                Arrays.fill(dictionaryFilterStatus, 0, dictionarySize, FILTER_NOT_EVALUATED);
            }
        }
        dictionaryOpen = true;

        presentStream = presentStreamSource.openStream();
        inDictionaryStream = inDictionaryStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        rowGroupOpen = true;
    }

    @Override
    public Block getBlock(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");

        return buildOutputBlock(positions, positionCount, nullsAllowed && presentStream != null);
    }

    @Override
    public BlockLease getBlockView(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");

        return buildOutputBlockView(positions, positionCount, nullsAllowed && presentStream != null);
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
        return INSTANCE_SIZE +
                sizeOf(dictionary) +
                sizeOf(dictionaryFilterStatus) +
                sizeOf(values) +
                sizeOf(nulls) +
                sizeOf(outputPositions);
    }
}
