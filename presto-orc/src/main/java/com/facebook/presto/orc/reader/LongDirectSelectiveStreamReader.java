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
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockLease;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.array.Arrays.ensureCapacity;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.facebook.presto.spi.block.ClosingBlockLease.newLease;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class LongDirectSelectiveStreamReader
        extends AbstractLongSelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongDirectSelectiveStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;
    @Nullable
    private final TupleDomainFilter filter;
    private final boolean nonDeterministicFilter;
    private final boolean nullsAllowed;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<LongInputStream> dataStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream dataStream;

    private boolean rowGroupOpen;
    private int readOffset;

    private boolean allNulls;

    private LocalMemoryContext systemMemoryContext;

    public LongDirectSelectiveStreamReader(
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
        checkArgument(positionCount > 0, "positionCount must be greater than zero");

        if (!rowGroupOpen) {
            openRowGroup();
        }

        prepareNextRead(positionCount, nullsAllowed && presentStream != null);

        allNulls = false;

        if (filter != null) {
            outputPositions = ensureCapacity(outputPositions, positionCount);
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
        if (dataStream == null && presentStream != null) {
            streamPosition = readAllNulls(positions, positionCount);
        }
        else {
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
                    if (filter == null || filter.testLong(value)) {
                        if (outputRequired) {
                            values[outputPositionCount] = value;
                            if (nullsAllowed && presentStream != null) {
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
        }

        readOffset = offset + streamPosition;

        return outputPositionCount;
    }

    private int readAllNulls(int[] positions, int positionCount)
            throws IOException
    {
        presentStream.skip(positions[positionCount - 1]);

        if (nonDeterministicFilter) {
            outputPositionCount = 0;
            for (int i = 0; i < positionCount; i++) {
                if (filter.testNull()) {
                    outputPositionCount++;
                }
                else {
                    outputPositionCount -= filter.getPrecedingPositionsToFail();
                    i += filter.getSucceedingPositionsToFail();
                }
            }
        }
        else if (nullsAllowed) {
            outputPositionCount = positionCount;
        }
        else {
            outputPositionCount = 0;
        }

        allNulls = true;
        return positions[positionCount - 1] + 1;
    }

    private void skip(int items)
            throws IOException
    {
        if (presentStream != null) {
            int dataToSkip = presentStream.countBitsSet(items);
            dataStream.skip(dataToSkip);
        }
        else {
            dataStream.skip(items);
        }
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        rowGroupOpen = true;
    }

    @Override
    public Block getBlock(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");

        if (allNulls) {
            return new RunLengthEncodedBlock(outputType.createBlockBuilder(null, 1).appendNull().build(), outputPositionCount);
        }

        return buildOutputBlock(positions, positionCount, nullsAllowed && presentStream != null);
    }

    @Override
    public BlockLease getBlockView(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");

        if (allNulls) {
            return newLease(new RunLengthEncodedBlock(outputType.createBlockBuilder(null, 1).appendNull().build(), outputPositionCount));
        }

        return buildOutputBlockView(positions, positionCount, nullsAllowed && presentStream != null);
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(LongInputStream.class);

        readOffset = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, LongInputStream.class);

        readOffset = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void close()
    {
        systemMemoryContext.close();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(values) + sizeOf(nulls) + sizeOf(outputPositions);
    }
}
