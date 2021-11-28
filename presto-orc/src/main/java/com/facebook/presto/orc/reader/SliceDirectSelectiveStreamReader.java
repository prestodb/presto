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

import com.facebook.presto.common.GenericInternalException;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockLease;
import com.facebook.presto.common.block.ClosingBlockLease;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.block.VariableWidthBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.OrcLocalMemoryContext;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.Stripe;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.ByteArrayInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.common.array.Arrays.ExpansionFactor.SMALL;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.INITIALIZE;
import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.ReaderUtils.convertLengthVectorToOffsetVector;
import static com.facebook.presto.orc.reader.ReaderUtils.packByteArrayAndOffsets;
import static com.facebook.presto.orc.reader.ReaderUtils.packByteArrayOffsetsAndNulls;
import static com.facebook.presto.orc.reader.SelectiveStreamReaders.initializeOutputPositions;
import static com.facebook.presto.orc.reader.SliceSelectiveStreamReader.computeTruncatedLength;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SliceDirectSelectiveStreamReader
        implements SelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceDirectSelectiveStreamReader.class).instanceSize();
    private static final int ONE_GIGABYTE = toIntExact(new DataSize(1, GIGABYTE).toBytes());

    private final TupleDomainFilter filter;
    private final boolean nonDeterministicFilter;
    private final boolean nullsAllowed;

    private final StreamDescriptor streamDescriptor;
    private final boolean outputRequired;
    private final Type outputType;
    private final boolean isCharType;
    private final int maxCodePointCount;

    private int readOffset;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    private BooleanInputStream presentStream;
    private InputStreamSource<ByteArrayInputStream> dataStreamSource = missingStreamSource(ByteArrayInputStream.class);
    private ByteArrayInputStream dataStream;
    private InputStreamSource<LongInputStream> lengthStreamSource = missingStreamSource(LongInputStream.class);
    private LongInputStream lengthStream;

    private boolean rowGroupOpen;
    private OrcLocalMemoryContext systemMemoryContext;
    private boolean[] nulls;

    private int[] outputPositions;
    private int outputPositionCount;

    private boolean allNulls;           // true if all requested positions are null
    private boolean[] isNullVector;     // isNull flags for all positions up to the last positions requested in read()
    private int[] lengthVector;         // lengths for all positions up to the last positions requested in read()
    private int lengthIndex;            // index into lengthVector array
    private int[] offsets;              // offsets of requested positions only; specifies position boundaries for the data array
    private byte[] data;                // data for requested positions only
    private Slice dataAsSlice;          // data array wrapped in Slice
    private boolean valuesInUse;

    public SliceDirectSelectiveStreamReader(StreamDescriptor streamDescriptor, Optional<TupleDomainFilter> filter, Optional<Type> outputType, OrcLocalMemoryContext newLocalMemoryContext)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.filter = requireNonNull(filter, "filter is null").orElse(null);
        this.systemMemoryContext = newLocalMemoryContext;
        this.nonDeterministicFilter = this.filter != null && !this.filter.isDeterministic();
        this.nullsAllowed = this.filter == null || nonDeterministicFilter || this.filter.testNull();
        this.outputType = requireNonNull(outputType, "outputType is null").orElse(null);
        this.outputRequired = outputType.isPresent();
        this.isCharType = streamDescriptor.getOrcType().getOrcTypeKind() == OrcType.OrcTypeKind.CHAR;
        this.maxCodePointCount = streamDescriptor.getOrcType().getLength().orElse(-1);
        checkArgument(filter.isPresent() || outputRequired, "filter must be present if outputRequired is false");
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

        outputPositions = initializeOutputPositions(outputPositions, positions, positionCount);

        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        int dataLength = prepareForNextRead(positionCount, positions);

        int streamPosition;

        if (lengthStream == null) {
            streamPosition = readAllNulls(positions, positionCount);
        }
        else if (filter == null) {
            streamPosition = readNoFilter(positions, positionCount, dataLength);
        }
        else {
            streamPosition = readWithFilter(positions, positionCount, dataLength);
        }

        readOffset = offset + streamPosition;
        return outputPositionCount;
    }

    private int readNoFilter(int[] positions, int positionCount, int dataLength)
            throws IOException
    {
        // filter == null implies outputRequired == true

        int totalPositionCount = positions[positionCount - 1] + 1;
        if (useBatchMode(positionCount, totalPositionCount)) {
            if (presentStream == null) {
                if (dataStream != null) {
                    dataStream.next(data, 0, dataLength);
                    convertLengthVectorToOffsetVector(lengthVector, totalPositionCount, offsets);

                    if (totalPositionCount > positionCount) {
                        packByteArrayAndOffsets(data, offsets, positions, positionCount);
                    }
                }
            }
            else {
                if (dataStream != null) {
                    dataStream.next(data, 0, dataLength);
                    convertLengthVectorToOffsetVector(lengthVector, isNullVector, totalPositionCount, offsets);
                }

                if (totalPositionCount > positionCount) {
                    packByteArrayOffsetsAndNulls(data, offsets, isNullVector, positions, positionCount);
                }

                if (nullsAllowed) {
                    System.arraycopy(isNullVector, 0, nulls, 0, positionCount);
                }
            }
            outputPositionCount = positionCount;
            return totalPositionCount;
        }

        int streamPosition = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skipData(streamPosition, position - streamPosition);
                streamPosition = position;
            }

            int offset = offsets[i];
            if (presentStream != null && isNullVector[position]) {
                if (offsets != null) {
                    offsets[i + 1] = offset;
                }
                nulls[i] = true;
            }
            else {
                int length = lengthVector[lengthIndex];
                int truncatedLength = 0;
                if (length > 0) {
                    dataStream.next(data, offset, offset + length);
                    truncatedLength = computeTruncatedLength(dataAsSlice, offset, length, maxCodePointCount, isCharType);
                }
                offsets[i + 1] = offset + truncatedLength;
                lengthIndex++;
                if (presentStream != null) {
                    nulls[i] = false;
                }
            }
            streamPosition++;
        }
        outputPositionCount = positionCount;
        return streamPosition;
    }

    private int readWithFilter(int[] positions, int positionCount, int dataLength)
            throws IOException
    {
        int totalPositionCount = positions[positionCount - 1] + 1;
        if (useBatchMode(positionCount, totalPositionCount)) {
            if (dataStream != null) {
                dataStream.next(data, 0, dataLength);
            }

            final int filteredPositionCount;
            if (presentStream == null) {
                filteredPositionCount = evaluateFilter(positions, positionCount);

                if (outputRequired && totalPositionCount > filteredPositionCount && filteredPositionCount > 0 && dataStream != null) {
                    packByteArrayAndOffsets(data, offsets, outputPositions, filteredPositionCount);
                }
            }
            else {
                filteredPositionCount = evaluateFilterWithNull(positions, positionCount);

                if (outputRequired) {
                    if (filteredPositionCount > 0) {
                        if (outputRequired && totalPositionCount > filteredPositionCount) {
                            packByteArrayOffsetsAndNulls(data, offsets, isNullVector, outputPositions, filteredPositionCount);
                        }

                        if (nullsAllowed) {
                            System.arraycopy(isNullVector, 0, nulls, 0, filteredPositionCount);
                        }
                    }
                }
            }

            outputPositionCount = filteredPositionCount;
            return totalPositionCount;
        }

        int streamPosition = 0;
        int dataToSkip = 0;

        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skipData(streamPosition, position - streamPosition);
                streamPosition = position;
            }

            int offset = outputRequired ? offsets[outputPositionCount] : 0;
            if (presentStream != null && isNullVector[position]) {
                if ((nonDeterministicFilter && filter.testNull()) || nullsAllowed) {
                    if (outputRequired) {
                        offsets[outputPositionCount + 1] = offset;
                        nulls[outputPositionCount] = true;
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                int length = lengthVector[lengthIndex];
                int dataOffset = outputRequired ? offset : 0;
                if (filter.testLength(length)) {
                    if (dataStream != null) {
                        dataStream.skip(dataToSkip);
                        dataToSkip = 0;
                        dataStream.next(data, dataOffset, dataOffset + length);
                        if (filter.testBytes(data, dataOffset, length)) {
                            if (outputRequired) {
                                int truncatedLength = computeTruncatedLength(dataAsSlice, dataOffset, length, maxCodePointCount, isCharType);
                                offsets[outputPositionCount + 1] = offset + truncatedLength;
                                if (nullsAllowed && presentStream != null) {
                                    nulls[outputPositionCount] = false;
                                }
                            }
                            outputPositions[outputPositionCount] = position;
                            outputPositionCount++;
                        }
                    }
                    else {
                        assert length == 0;
                        if (filter.testBytes("".getBytes(), 0, 0)) {
                            if (outputRequired) {
                                offsets[outputPositionCount + 1] = offset;
                                if (nullsAllowed && presentStream != null) {
                                    nulls[outputPositionCount] = false;
                                }
                            }
                            outputPositions[outputPositionCount] = position;
                            outputPositionCount++;
                        }
                    }
                }
                else {
                    dataToSkip += length;
                }
                lengthIndex++;
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
                    skipData(streamPosition, positionsToSkip);
                }
            }
        }
        if (dataToSkip > 0) {
            dataStream.skip(dataToSkip);
        }
        return streamPosition;
    }

    private int readAllNulls(int[] positions, int positionCount)
    {
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
        // in case of an empty varbinary both the presentStream and dataStream are null and only lengthStream is present.
        if (dataStream == null && presentStream != null) {
            presentStream.skip(items);
        }
        else if (presentStream != null) {
            int lengthToSkip = presentStream.countBitsSet(items);
            dataStream.skip(lengthStream.sum(lengthToSkip));
        }
        else {
            long sum = lengthStream.sum(items);
            if (dataStream != null) {
                dataStream.skip(sum);
            }
        }
    }

    private void skipData(int start, int items)
            throws IOException
    {
        int dataToSkip = 0;
        for (int i = 0; i < items; i++) {
            if (presentStream == null || !isNullVector[start + i]) {
                dataToSkip += lengthVector[lengthIndex];
                lengthIndex++;
            }
        }
        // in case of an empty varbinary both the presentStream and dataStream are null and only lengthStream is present.
        if (dataStream != null) {
            dataStream.skip(dataToSkip);
        }
    }

    // No nulls
    private int evaluateFilter(int[] positions, int positionCount)
    {
        int positionsIndex = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (filter.testLength(lengthVector[position])) {
                outputPositions[positionsIndex++] = position;  // compact positions on the fly
            }
            else {
                i += filter.getSucceedingPositionsToFail();
                positionsIndex -= filter.getPrecedingPositionsToFail();
            }
        }

        int filteredPositionCount = 0;
        if (positionsIndex > 0) {
            if (dataStream == null) {
                filteredPositionCount = testEmptyStrings(outputPositions, positionsIndex);
            }
            else {
                int totalPositionCount = outputPositions[positionsIndex - 1] + 1;
                convertLengthVectorToOffsetVector(lengthVector, totalPositionCount, offsets);
                filteredPositionCount = testBytes(outputPositions, positionsIndex);
            }
        }

        return filteredPositionCount;
    }

    private int evaluateFilterWithNull(int[] positions, int positionCount)
    {
        if (dataStream != null) {
            int totalPositionCount = positions[positionCount - 1] + 1;
            convertLengthVectorToOffsetVector(lengthVector, isNullVector, totalPositionCount, offsets);
        }

        int positionsIndex = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];

            if (isNullVector[position]) {
                if ((nonDeterministicFilter && filter.testNull()) || nullsAllowed) {
                    outputPositions[positionsIndex++] = position;
                }
                else {
                    i += filter.getSucceedingPositionsToFail();
                    positionsIndex -= filter.getPrecedingPositionsToFail();
                }
            }
            else {
                int dataOffset = offsets[position];
                int length = offsets[position + 1] - dataOffset;

                if (filter.testLength(length) && filter.testBytes(data, dataOffset, length)) {
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

    private int testEmptyStrings(int[] positions, int positionCount)
    {
        if (nonDeterministicFilter) {
            int positionsIndex = 0;
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];

                if (filter.testBytes("".getBytes(), 0, 0)) {
                    positions[positionsIndex++] = position;
                }
                else {
                    i += filter.getSucceedingPositionsToFail();
                    positionsIndex -= filter.getPrecedingPositionsToFail();
                }
            }
            return positionsIndex;
        }

        if (filter.testBytes("".getBytes(), 0, 0)) {
            return positionCount;
        }

        return 0;
    }

    private int testBytes(int[] positions, int positionCount)
    {
        int positionsIndex = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];

            int dataOffset = offsets[position];
            int length = offsets[position + 1] - dataOffset;
            if (filter.testBytes(data, dataOffset, length)) {
                positions[positionsIndex++] = position;
            }
            else {
                i += filter.getSucceedingPositionsToFail();
                positionsIndex -= filter.getPrecedingPositionsToFail();
            }
        }
        return positionsIndex;
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
            return new RunLengthEncodedBlock(outputType.createBlockBuilder(null, 1).appendNull().build(), positionCount);
        }

        boolean includeNulls = nullsAllowed && presentStream != null;

        if (positionCount != outputPositionCount) {
            compactValues(positions, positionCount, includeNulls);
        }

        Block block = new VariableWidthBlock(positionCount, dataAsSlice, offsets, Optional.ofNullable(includeNulls ? nulls : null));
        dataAsSlice = null;
        data = null;
        offsets = null;
        nulls = null;
        return block;
    }

    private void compactValues(int[] positions, int positionCount, boolean includeNulls)
    {
        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            assert outputPositions[i] == nextPosition;

            int length = offsets[i + 1] - offsets[i];
            if (length > 0) {
                System.arraycopy(data, offsets[i], data, offsets[positionIndex], length);
            }
            offsets[positionIndex + 1] = offsets[positionIndex] + length;
            outputPositions[positionIndex] = nextPosition;

            if (includeNulls) {
                nulls[positionIndex] = nulls[i];
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }
            nextPosition = positions[positionIndex];
        }

        outputPositionCount = positionCount;
    }

    @Override
    public BlockLease getBlockView(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (allNulls) {
            return newLease(new RunLengthEncodedBlock(outputType.createBlockBuilder(null, 1).appendNull().build(), positionCount));
        }
        boolean includeNulls = nullsAllowed && presentStream != null;
        if (positionCount != outputPositionCount) {
            compactValues(positions, positionCount, includeNulls);
        }
        return newLease(new VariableWidthBlock(positionCount, dataAsSlice, offsets, Optional.ofNullable(includeNulls ? nulls : null)));
    }

    private BlockLease newLease(Block block)
    {
        valuesInUse = true;
        return ClosingBlockLease.newLease(block, () -> valuesInUse = false);
    }

    @Override
    public void throwAnyError(int[] positions, int positionCount)
    {
    }

    @Override
    public void close()
    {
        dataAsSlice = null;
        data = null;
        lengthVector = null;
        isNullVector = null;
        offsets = null;
        outputPositions = null;
        systemMemoryContext.close();
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        lengthStream = lengthStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        rowGroupOpen = true;
    }

    @Override
    public void startStripe(Stripe stripe)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        lengthStreamSource = missingStreamSource(LongInputStream.class);
        dataStreamSource = missingStreamSource(ByteArrayInputStream.class);

        readOffset = 0;

        presentStream = null;
        lengthStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        lengthStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, LENGTH, LongInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, ByteArrayInputStream.class);

        readOffset = 0;

        presentStream = null;
        lengthStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(offsets) + sizeOf(outputPositions) + sizeOf(data) + sizeOf(nulls) + sizeOf(lengthVector) + sizeOf(isNullVector);
    }

    @VisibleForTesting
    public void resetDataStream()
    {
        dataStream = null;
    }

    private int prepareForNextRead(int positionCount, int[] positions)
            throws IOException
    {
        lengthIndex = 0;
        outputPositionCount = 0;

        int totalLength = 0;
        int maxLength = 0;
        int dataLength = 0;

        int totalPositions = positions[positionCount - 1] + 1;
        int nullCount = 0;
        if (presentStream != null) {
            isNullVector = ensureCapacity(isNullVector, totalPositions);
            nullCount = presentStream.getUnsetBits(totalPositions, isNullVector);
        }

        if (lengthStream != null) {
            int nonNullCount = totalPositions - nullCount;
            lengthVector = ensureCapacity(lengthVector, nonNullCount);
            lengthStream.next(lengthVector, nonNullCount);

            if (useBatchMode(positionCount, totalPositions)) {
                for (int i = 0; i < nonNullCount; i++) {
                    totalLength += lengthVector[i];
                    maxLength = Math.max(maxLength, lengthVector[i]);
                }
            }
            else {
                int positionIndex = 0;
                int lengthIndex = 0;
                for (int i = 0; i < totalPositions; i++) {
                    boolean isNotNull = nullCount == 0 || !isNullVector[i];
                    if (i == positions[positionIndex]) {
                        if (isNotNull) {
                            totalLength += lengthVector[lengthIndex];
                            maxLength = Math.max(maxLength, lengthVector[lengthIndex]);
                            lengthIndex++;
                        }
                        positionIndex++;
                    }
                    else if (isNotNull) {
                        lengthIndex++;
                    }
                }
            }

            // TODO Do not throw if outputRequired == false
            if (totalLength > ONE_GIGABYTE) {
                throw new GenericInternalException(
                        format("Values in column \"%s\" are too large to process for Presto. %s column values are larger than 1GB [%s]",
                                streamDescriptor.getFieldName(), positionCount,
                                streamDescriptor.getOrcDataSourceId()));
            }
        }

        if (outputRequired) {
            if (presentStream != null && nullsAllowed) {
                nulls = ensureCapacity(nulls, positionCount);
            }
            dataLength = totalLength;
            data = ensureCapacity(data, totalLength);
            offsets = ensureCapacity(offsets, totalPositions + 1, SMALL, INITIALIZE);
        }
        else {
            if (useBatchMode(positionCount, totalPositions)) {
                dataLength = totalLength;
                if (filter != null) {
                    offsets = ensureCapacity(offsets, totalPositions + 1, SMALL, INITIALIZE);
                }
            }
            else {
                dataLength = maxLength;
            }

            data = ensureCapacity(data, dataLength);
        }

        dataAsSlice = Slices.wrappedBuffer(data);
        return dataLength;
    }

    private boolean useBatchMode(int positionCount, int totalPositionCount)
    {
        // maxCodePointCount < 0 means it's unbounded varchar VARCHAR.
        // If the types are VARCHAR(N) or CHAR(N), the length of the string need to be calculated and truncated.
        if (lengthStream == null || maxCodePointCount >= 0) {
            return false;
        }

        double inputFilterRate = (double) (totalPositionCount - positionCount) / totalPositionCount;
        if (filter == null) {  // readNoFilter
            // When there is no filter, batch mode performs better for almost all inputFilterRate.
            // But to limit data buffer size, we enable it for the range of [0.0f, 0.5f]
            if (inputFilterRate >= 0.0f && inputFilterRate <= 0.5f) {
                return true;
            }

            return false;
        }
        else { // readWithFilter
            // When there is filter, batch mode performs better for almost all inputFilterRate except when inputFilterRate is around 0.1f.
            // To limit data buffer size, we enable it for the range of [0.0f, 0.05f] and [0.15f, 0.5f]
            if (inputFilterRate >= 0.0f && inputFilterRate <= 0.05f || inputFilterRate >= 0.15f && inputFilterRate <= 0.5f) {
                return true;
            }

            return false;
        }
    }
}
