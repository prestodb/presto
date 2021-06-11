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
import com.facebook.presto.common.block.ClosingBlockLease;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.OrcLocalMemoryContext;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.Stripe;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.DecimalInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.SECONDARY;
import static com.facebook.presto.orc.reader.SelectiveStreamReaders.initializeOutputPositions;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public abstract class AbstractDecimalSelectiveStreamReader
        implements SelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(AbstractDecimalSelectiveStreamReader.class).instanceSize();

    protected final TupleDomainFilter filter;
    protected final boolean nullsAllowed;
    protected final boolean outputRequired;
    protected final boolean nonDeterministicFilter;
    protected final int scale;

    protected long[] values;
    protected boolean[] nulls;
    protected int[] outputPositions;
    protected int outputPositionCount;
    protected BooleanInputStream presentStream;
    protected DecimalInputStream dataStream;
    protected LongInputStream scaleStream;

    private final int valuesPerPosition;
    private final Block nullBlock;
    private final StreamDescriptor streamDescriptor;
    private final OrcLocalMemoryContext systemMemoryContext;

    private int readOffset;
    private boolean rowGroupOpen;
    private boolean allNulls;
    private boolean valuesInUse;
    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    private InputStreamSource<DecimalInputStream> dataStreamSource = missingStreamSource(DecimalInputStream.class);
    private InputStreamSource<LongInputStream> scaleStreamSource = missingStreamSource(LongInputStream.class);

    public AbstractDecimalSelectiveStreamReader(
            StreamDescriptor streamDescriptor,
            Optional<TupleDomainFilter> filter,
            Optional<Type> outputType,
            OrcLocalMemoryContext systemMemoryContext,
            int valuesPerPosition)
    {
        requireNonNull(filter, "filter is null");
        requireNonNull(outputType, "outputType is null");
        checkArgument(filter.isPresent() || outputType.isPresent(), "filter must be present if output is not required");
        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.filter = filter.orElse(null);
        this.outputRequired = outputType.isPresent();
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.nonDeterministicFilter = this.filter != null && !this.filter.isDeterministic();
        this.nullsAllowed = this.filter == null || this.nonDeterministicFilter || this.filter.testNull();
        this.scale = streamDescriptor.getOrcType().getScale().get();
        this.nullBlock = outputType.map(type -> type.createBlockBuilder(null, 1).appendNull().build()).orElse(null);
        this.valuesPerPosition = valuesPerPosition;
    }

    @Override
    public void startStripe(Stripe stripe)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(DecimalInputStream.class);
        scaleStreamSource = missingStreamSource(LongInputStream.class);
        readOffset = 0;
        presentStream = null;
        dataStream = null;
        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, DecimalInputStream.class);
        scaleStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, SECONDARY, LongInputStream.class);
        readOffset = 0;
        presentStream = null;
        dataStream = null;
        scaleStream = null;
        rowGroupOpen = false;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(values) + sizeOf(nulls) + sizeOf(outputPositions) + (nullBlock == null ? 0 : nullBlock.getRetainedSizeInBytes());
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        dataStream = dataStreamSource.openStream();
        scaleStream = scaleStreamSource.openStream();
        rowGroupOpen = true;
    }

    @Override
    public int read(int offset, int[] positions, int positionCount)
            throws IOException
    {
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (!rowGroupOpen) {
            openRowGroup();
        }

        allNulls = false;

        if (outputRequired) {
            ensureValuesCapacity(positionCount, nullsAllowed && presentStream != null);
        }

        outputPositions = initializeOutputPositions(outputPositions, positions, positionCount);

        // account memory used by values, nulls and outputPositions
        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        int streamPosition = 0;
        outputPositionCount = 0;
        if (dataStream == null && scaleStream == null && presentStream != null) {
            streamPosition = readAllNulls(positions, positionCount);
        }
        else if (filter == null) {
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

    protected void skip(int items)
            throws IOException
    {
        if (dataStream == null) {
            presentStream.skip(items);
        }
        else if (presentStream != null) {
            int dataToSkip = presentStream.countBitsSet(items);
            dataStream.skip(dataToSkip);
            scaleStream.skip(dataToSkip);
        }
        else {
            dataStream.skip(items);
            scaleStream.skip(items);
        }
    }

    @Override
    public int[] getReadPositions()
    {
        return outputPositions;
    }

    @Override
    public void throwAnyError(int[] positions, int positionCount)
    {
    }

    private BlockLease newLease(Block block)
    {
        valuesInUse = true;
        return ClosingBlockLease.newLease(block, () -> valuesInUse = false);
    }

    @Override
    public Block getBlock(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (allNulls) {
            return new RunLengthEncodedBlock(nullBlock, positionCount);
        }

        boolean includeNulls = nullsAllowed && presentStream != null;

        if (positionCount == outputPositionCount) {
            Block block = makeBlock(positionCount, nullsAllowed, nulls, values);
            nulls = null;
            values = null;
            return block;
        }

        long[] valuesCopy = new long[valuesPerPosition * positionCount];
        boolean[] nullsCopy = null;

        if (includeNulls) {
            nullsCopy = new boolean[positionCount];
        }

        copyValues(positions, positionCount, valuesCopy, nullsCopy);

        return makeBlock(positionCount, includeNulls, nullsCopy, valuesCopy);
    }

    @Override
    public BlockLease getBlockView(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (allNulls) {
            return newLease(new RunLengthEncodedBlock(nullBlock, positionCount));
        }

        boolean includeNulls = nullsAllowed && presentStream != null;
        if (positionCount != outputPositionCount) {
            compactValues(positions, positionCount, includeNulls);
        }

        return newLease(makeBlock(positionCount, includeNulls, nulls, values));
    }

    private void ensureValuesCapacity(int capacity, boolean nullAllowed)
    {
        int valuesCapacity = valuesPerPosition * capacity;
        if (values == null || values.length < valuesCapacity) {
            values = new long[valuesCapacity];
        }

        if (nullAllowed) {
            if (nulls == null || nulls.length < capacity) {
                nulls = new boolean[capacity];
            }
        }
    }

    abstract void copyValues(int[] positions, int positionsCount, long[] valuesCopy, boolean[] nullsCopy);

    abstract Block makeBlock(int positionCount, boolean includeNulls, boolean[] nulls, long[] values);

    abstract void compactValues(int[] positions, int positionCount, boolean compactNulls);

    abstract int readNoFilter(int[] positions, int position)
            throws IOException;

    abstract int readWithFilter(int[] positions, int position)
            throws IOException;

    @Override
    public void close()
    {
        values = null;
        nulls = null;
        outputPositions = null;

        presentStream = null;
        presentStreamSource = null;
        dataStream = null;
        dataStreamSource = null;
        scaleStream = null;
        scaleStreamSource = null;

        systemMemoryContext.close();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
