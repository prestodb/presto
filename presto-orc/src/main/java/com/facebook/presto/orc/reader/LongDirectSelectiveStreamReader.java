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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockLease;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.predicate.TupleDomainFilter;
import com.facebook.presto.orc.OrcLocalMemoryContext;
import com.facebook.presto.orc.Stripe;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;

import static com.facebook.presto.common.block.ClosingBlockLease.newLease;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.SelectiveStreamReaders.initializeOutputPositions;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.getBooleanMissingStreamSource;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.getLongMissingStreamSource;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class LongDirectSelectiveStreamReader
        extends AbstractLongSelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongDirectSelectiveStreamReader.class).instanceSize();
    private final OrcLocalMemoryContext systemMemoryContext;

    private InputStreamSource<BooleanInputStream> presentStreamSource = getBooleanMissingStreamSource();
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<LongInputStream> dataStreamSource = getLongMissingStreamSource();
    @Nullable
    private LongInputStream dataStream;

    private boolean rowGroupOpen;
    private int readOffset;

    private boolean allNulls;

    public LongDirectSelectiveStreamReader(LongSelectiveReaderContext context)
    {
        super(context);
        this.systemMemoryContext = context.getSystemMemoryContext().newOrcLocalMemoryContext(
                LongSelectiveStreamReader.class.getSimpleName());
    }

    @Override
    public int read(int offset, int[] positions, int positionCount)
            throws IOException
    {
        checkArgument(positionCount > 0, "positionCount must be greater than zero");

        if (!rowGroupOpen) {
            openRowGroup();
        }

        prepareNextRead(positionCount, context.isNullsAllowed() && presentStream != null);

        allNulls = false;

        outputPositions = initializeOutputPositions(outputPositions, positions, positionCount);

        // account memory used by values, nulls and outputPositions
        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        outputPositionCount = 0;
        int streamPosition;
        if (dataStream == null && presentStream != null) {
            streamPosition = readAllNulls(positions, positionCount);
        }
        else if (context.getFilter() == null) {
            streamPosition = readNoFilter(positions, positionCount);
        }
        else {
            streamPosition = readWithFilter(positions, positionCount);
        }

        readOffset = offset + streamPosition;

        return outputPositionCount;
    }

    private int readAllNulls(int[] positions, int positionCount)
            throws IOException
    {
        presentStream.skip(positions[positionCount - 1]);

        TupleDomainFilter filter = context.getFilter();
        if (context.isNonDeterministicFilter()) {
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
        else if (context.isNullsAllowed()) {
            outputPositionCount = positionCount;
        }
        else {
            outputPositionCount = 0;
        }

        allNulls = true;
        return positions[positionCount - 1] + 1;
    }

    private int readNoFilter(int[] positions, int positionCount)
            throws IOException
    {
        if (positions[positionCount - 1] == positionCount - 1) {
            // no skipping
            if (presentStream != null) {
                // some nulls
                int nullCount = presentStream.getUnsetBits(positionCount, nulls);
                if (nullCount == positionCount) {
                    allNulls = true;
                }
                else {
                    for (int i = 0; i < positionCount; i++) {
                        if (nulls[i]) {
                            values[i] = 0;
                        }
                        else {
                            values[i] = dataStream.next();
                        }
                    }
                }
            }
            else {
                // no nulls
                for (int i = 0; i < positionCount; i++) {
                    values[i] = dataStream.next();
                }
            }
            outputPositionCount = positionCount;
            return positionCount;
        }

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
                values[i] = dataStream.next();
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
        TupleDomainFilter filter = context.getFilter();
        if (positions[positionCount - 1] == positionCount - 1) {
            // no skipping
            if (presentStream == null) {
                // no nulls
                if (!context.isOutputRequired() && !filter.isPositionalFilter()) {
                    // no output; just filter
                    for (int i = 0; i < positionCount; i++) {
                        long value = dataStream.next();
                        if (filter.testLong(value)) {
                            outputPositions[outputPositionCount] = positions[i];
                            outputPositionCount++;
                        }
                    }
                    return positionCount;
                }
            }
        }

        int streamPosition = 0;

        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                if ((context.isNonDeterministicFilter() && filter.testNull()) || context.isNullsAllowed()) {
                    if (context.isOutputRequired()) {
                        nulls[outputPositionCount] = true;
                        values[outputPositionCount] = 0;
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                long value = dataStream.next();
                if (filter.testLong(value)) {
                    if (context.isOutputRequired()) {
                        values[outputPositionCount] = value;
                        if (context.isNullsAllowed() && presentStream != null) {
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
        if (dataStream == null) {
            presentStream.skip(items);
        }
        else if (presentStream != null) {
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
        checkState(context.isOutputRequired(), "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");

        if (allNulls) {
            return new RunLengthEncodedBlock(context.getOutputType().createBlockBuilder(null, 1).appendNull().build(), positionCount);
        }

        return buildOutputBlock(positions, positionCount, context.isNullsAllowed() && presentStream != null);
    }

    @Override
    public BlockLease getBlockView(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(context.isOutputRequired(), "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");

        if (allNulls) {
            return newLease(new RunLengthEncodedBlock(context.getOutputType().createBlockBuilder(null, 1).appendNull().build(), positionCount));
        }

        return buildOutputBlockView(positions, positionCount, context.isNullsAllowed() && presentStream != null);
    }

    @Override
    public void startStripe(Stripe stripe)
    {
        presentStreamSource = getBooleanMissingStreamSource();
        dataStreamSource = getLongMissingStreamSource();

        readOffset = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(context.getStreamDescriptor(), PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(context.getStreamDescriptor(), DATA, LongInputStream.class);

        readOffset = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void close()
    {
        values = null;
        outputPositions = null;

        presentStream = null;
        presentStreamSource = null;
        dataStream = null;
        dataStreamSource = null;

        systemMemoryContext.close();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + super.getRetainedSizeInBytes();
    }
}
