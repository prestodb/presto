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
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.IN_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
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

    private InputStreamSource<BooleanInputStream> inDictionaryStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream inDictionaryStream;

    private InputStreamSource<LongInputStream> dataStreamSource;
    @Nullable
    private LongInputStream dataStream;

    private boolean dictionaryOpen;
    private boolean rowGroupOpen;
    private int readOffset;

    private LocalMemoryContext systemMemoryContext;

    public LongDictionarySelectiveStreamReader(
            StreamDescriptor streamDescriptor,
            Optional<TupleDomainFilter> filter,
            Optional<Type> outputType,
            LocalMemoryContext systemMemoryContext)
    {
        super(outputType);
        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.filter = requireNonNull(filter, "filter is null").orElse(null);
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");

        nonDeterministicFilter = this.filter != null && !this.filter.isDeterministic();
        nullsAllowed = this.filter == null || nonDeterministicFilter || this.filter.testNull();
    }

    @Override
    public int read(int offset, int[] positions, int positionCount)
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        prepareNextRead(positionCount, presentStream != null && nullsAllowed);

        if (filter != null) {
            ensureOutputPositionsCapacity(positionCount);
        }
        else {
            outputPositions = positions;
        }

        // account memory used by values, nulls and outputPositions
        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        outputPositionCount = 0;
        int streamPosition = 0;
        // TODO In case of all nulls, the stream type will be LongDirect
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
                    }
                    if (filter != null) {
                        outputPositions[outputPositionCount] = position;
                    }
                    outputPositionCount++;
                }
            }
            else {
                long value = dataStream.next();
                if (inDictionaryStream == null || inDictionaryStream.nextBit()) {
                    value = dictionary[(int) value];
                }
                if (filter == null || filter.testLong(value)) {
                    if (outputRequired) {
                        values[outputPositionCount] = value;
                        if (nulls != null) {
                            nulls[outputPositionCount] = false;
                        }
                    }
                    if (filter != null) {
                        outputPositions[outputPositionCount] = position;
                    }
                    outputPositionCount++;
                }
            }

            streamPosition++;

            if (filter != null) {
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
        }

        readOffset = offset + streamPosition;

        return outputPositionCount;
    }

    private void skip(int items)
            throws IOException
    {
        if (presentStream != null) {
            int dataToSkip = presentStream.countBitsSet(items);
            if (inDictionaryStream != null) {
                inDictionaryStream.skip(dataToSkip);
            }
            dataStream.skip(dataToSkip);
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
        return INSTANCE_SIZE + sizeOf(dictionary) + sizeOf(values) + sizeOf(nulls) + sizeOf(outputPositions);
    }
}
