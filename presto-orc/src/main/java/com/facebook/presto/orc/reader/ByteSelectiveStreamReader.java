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
import com.facebook.presto.common.block.ByteArrayBlock;
import com.facebook.presto.common.block.ClosingBlockLease;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.orc.OrcLocalMemoryContext;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.ByteInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.orc.array.Arrays.ensureCapacity;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.ReaderUtils.packBytes;
import static com.facebook.presto.orc.reader.ReaderUtils.packBytesAndNulls;
import static com.facebook.presto.orc.reader.ReaderUtils.unpackByteNulls;
import static com.facebook.presto.orc.reader.SelectiveStreamReaders.initializeOutputPositions;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class ByteSelectiveStreamReader
        implements SelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ByteSelectiveStreamReader.class).instanceSize();
    private static final Block NULL_BLOCK = TINYINT.createBlockBuilder(null, 1).appendNull().build();

    private final StreamDescriptor streamDescriptor;
    private final TupleDomainFilter filter;
    private final boolean nullsAllowed;
    private final boolean outputRequired;
    private final OrcLocalMemoryContext systemMemoryContext;
    private final boolean nonDeterministicFilter;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    private InputStreamSource<ByteInputStream> dataStreamSource = missingStreamSource(ByteInputStream.class);

    @Nullable
    private BooleanInputStream presentStream;
    @Nullable
    private ByteInputStream dataStream;
    private boolean rowGroupOpen;

    private int readOffset;
    @Nullable
    private byte[] values;
    @Nullable
    private boolean[] nulls;
    @Nullable
    private int[] outputPositions;
    private int outputPositionCount;
    private boolean allNulls;
    private boolean valuesInUse;

    public ByteSelectiveStreamReader(
            StreamDescriptor streamDescriptor,
            Optional<TupleDomainFilter> filter,
            boolean outputRequired,
            OrcLocalMemoryContext systemMemoryContext)
    {
        requireNonNull(filter, "filter is null");
        checkArgument(filter.isPresent() || outputRequired, "filter must be present if outputRequired is false");
        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.filter = filter.orElse(null);
        this.outputRequired = outputRequired;
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.nonDeterministicFilter = this.filter != null && !this.filter.isDeterministic();
        this.nullsAllowed = this.filter == null || nonDeterministicFilter || this.filter.testNull();
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, Map<Integer, ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(ByteInputStream.class);
        readOffset = 0;
        presentStream = null;
        dataStream = null;
        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, ByteInputStream.class);
        readOffset = 0;
        presentStream = null;
        dataStream = null;
        rowGroupOpen = false;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(values) + sizeOf(nulls) + sizeOf(outputPositions);
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        dataStream = dataStreamSource.openStream();
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

        if (useBatchMode(positionCount, positions[positionCount - 1] + 1)) {
            // values need to be allocated for batch mode, because they need to be used for evaluating filters even when output is not required,
            // nulls need to be allocated when presentStream != null, because values need to be unpacked with nulls
            ensureValuesCapacity(positions[positionCount - 1] + 1, presentStream != null);
        }
        else if (outputRequired) {
            ensureValuesCapacity(positionCount, nullsAllowed && presentStream != null);
        }

        outputPositions = initializeOutputPositions(outputPositions, positions, positionCount);

        // account memory used by values, nulls and outputPositions
        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        int streamPosition = 0;
        if (dataStream == null && presentStream != null) {
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

    private int readWithFilter(int[] positions, int positionCount)
            throws IOException
    {
        int totalPositionCount = positions[positionCount - 1] + 1;
        if (useBatchMode(positionCount, totalPositionCount)) {
            int readCount = 0;

            int filteredPositionCount;

            if (presentStream == null) {
                dataStream.next(values, totalPositionCount);

                filteredPositionCount = evaluateFilter(positions, positionCount);

                if (outputRequired && totalPositionCount > filteredPositionCount) {
                    packBytes(values, outputPositions, filteredPositionCount);
                }
            }
            else {
                int nullCount = presentStream.getUnsetBits(totalPositionCount, nulls);
                if (nullCount == totalPositionCount) {
                    // all nulls
                    if ((nonDeterministicFilter && this.filter.testNull()) || nullsAllowed) {
                        allNulls = true;
                        filteredPositionCount = positionCount; // No positions were filtered out
                    }
                    else {
                        filteredPositionCount = 0;
                    }
                }
                else {
                    // some nulls
                    readCount = totalPositionCount - nullCount;
                    dataStream.next(values, readCount);

                    if (nullCount != 0) {
                        unpackByteNulls(values, nulls, totalPositionCount, readCount);
                    }

                    filteredPositionCount = evaluateFilterWithNulls(positions, positionCount);

                    if (outputRequired && totalPositionCount > filteredPositionCount) {
                        // both values and nulls need to be packed
                        packBytesAndNulls(values, nulls, outputPositions, filteredPositionCount);
                    }
                }
            }
            outputPositionCount = filteredPositionCount;

            // Should return totalPositionCount instead of readCount
            return totalPositionCount;
        }

        int streamPosition = 0;
        outputPositionCount = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                if ((nonDeterministicFilter && this.filter.testNull()) || nullsAllowed) {
                    if (outputRequired) {
                        nulls[outputPositionCount] = true;
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                byte value = dataStream.next();
                if (filter.testLong(value)) {
                    if (outputRequired) {
                        values[outputPositionCount] = value;
                        if (nullsAllowed && presentStream != null) {
                            nulls[outputPositionCount] = false;
                        }
                    }
                    outputPositions[outputPositionCount] = position;
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
        return streamPosition;
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

    private int readNoFilter(int[] positions, int positionCount)
            throws IOException
    {
        // filter == null implies outputRequired == true

        int totalPositionCount = positions[positionCount - 1] + 1;
        if (useBatchMode(positionCount, totalPositionCount)) {
            if (presentStream == null) {
                dataStream.next(values, totalPositionCount);
                if (totalPositionCount > positionCount) {
                    packBytes(values, positions, positionCount);
                }
            }
            else {
                int nullCount = presentStream.getUnsetBits(totalPositionCount, nulls);

                if (nullCount == totalPositionCount) {
                    // all nulls
                    allNulls = true;
                }
                else {
                    // some nulls
                    dataStream.next(values, totalPositionCount - nullCount);

                    if (outputRequired) {
                        if (nullCount != 0) {
                            unpackByteNulls(values, nulls, totalPositionCount, totalPositionCount - nullCount);
                        }

                        if (totalPositionCount > positionCount) {
                            // Need to pack both values and nulls
                            packBytesAndNulls(values, nulls, positions, positionCount);
                        }
                    }
                }
            }
            outputPositionCount = positionCount;
            return totalPositionCount;
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

    private void ensureValuesCapacity(int capacity, boolean recordNulls)
    {
        values = ensureCapacity(values, capacity);

        if (recordNulls) {
            nulls = ensureCapacity(nulls, capacity);
        }
    }

    @Override
    public int[] getReadPositions()
    {
        return outputPositions;
    }

    @Override
    public Block getBlock(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (allNulls) {
            return new RunLengthEncodedBlock(NULL_BLOCK, positionCount);
        }

        boolean includeNulls = nullsAllowed && presentStream != null;
        if (positionCount == outputPositionCount) {
            Block block = new ByteArrayBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), values);
            nulls = null;
            values = null;
            return block;
        }

        byte[] valuesCopy = new byte[positionCount];
        boolean[] nullsCopy = null;
        if (includeNulls) {
            nullsCopy = new boolean[positionCount];
        }

        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            assert outputPositions[i] == nextPosition;

            valuesCopy[positionIndex] = this.values[i];
            if (nullsCopy != null) {
                nullsCopy[positionIndex] = this.nulls[i];
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }

            nextPosition = positions[positionIndex];
        }

        return new ByteArrayBlock(positionCount, Optional.ofNullable(nullsCopy), valuesCopy);
    }

    @Override
    public BlockLease getBlockView(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (allNulls) {
            return newLease(new RunLengthEncodedBlock(NULL_BLOCK, positionCount));
        }

        boolean includeNulls = nullsAllowed && presentStream != null;
        if (positionCount != outputPositionCount) {
            compactValues(positions, positionCount, includeNulls);
        }

        return newLease(new ByteArrayBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), values));
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

    private void compactValues(int[] positions, int positionCount, boolean compactNulls)
    {
        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            assert outputPositions[i] == nextPosition;

            values[positionIndex] = values[i];
            if (compactNulls) {
                nulls[positionIndex] = nulls[i];
            }
            outputPositions[positionIndex] = nextPosition;

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }
            nextPosition = positions[positionIndex];
        }

        outputPositionCount = positionCount;
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
        values = null;
        outputPositions = null;
        nulls = null;

        dataStream = null;
        presentStream = null;
        presentStreamSource = null;
        dataStreamSource = null;

        systemMemoryContext.close();
    }

    private boolean useBatchMode(int positionCount, int totalPositionCount)
    {
        // JMH benchmark shows that when there is no null or no filter, the batch read mode was always better than the skipping mode.
        // When there is filter and partial nulls, the batch read mode was better than the skipping mode when the input filter rate is less than 0.4
        if (presentStream == null || filter == null || (double) (totalPositionCount - positionCount) / totalPositionCount <= 0.4) {
            return true;
        }

        return false;
    }

    private int evaluateFilter(int[] positions, int positionCount)
    {
        int positionsIndex = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (filter.testLong(values[position])) {
                outputPositions[positionsIndex++] = position;  // compact positions on the fly
            }
            else {
                i += filter.getSucceedingPositionsToFail();
                positionsIndex -= filter.getPrecedingPositionsToFail();
            }
        }

        return positionsIndex;
    }

    private int evaluateFilterWithNulls(int[] positions, int positionCount)
    {
        int positionsIndex = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];

            if (nulls[position]) {
                if ((nonDeterministicFilter && this.filter.testNull()) || nullsAllowed) {
                    outputPositions[positionsIndex++] = position;
                }
                else {
                    i += filter.getSucceedingPositionsToFail();
                    positionsIndex -= filter.getPrecedingPositionsToFail();
                }
            }
            else {
                if (filter.testLong(values[position])) {
                    outputPositions[positionsIndex++] = position;  // compact positions on the fly
                }
                else {
                    i += filter.getSucceedingPositionsToFail();
                    positionsIndex -= filter.getPrecedingPositionsToFail();
                }
            }
        }

        return positionsIndex;
    }
}
