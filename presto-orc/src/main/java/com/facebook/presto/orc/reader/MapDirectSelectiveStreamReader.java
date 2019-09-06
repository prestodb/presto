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

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.TupleDomainFilter.BigintRange;
import com.facebook.presto.orc.TupleDomainFilter.BigintValues;
import com.facebook.presto.orc.TupleDomainFilter.BytesRange;
import com.facebook.presto.orc.TupleDomainFilter.BytesValues;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockLease;
import com.facebook.presto.spi.block.ClosingBlockLease;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.array.Arrays.ensureCapacity;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NOT_NULL;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NULL;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class MapDirectSelectiveStreamReader
        implements SelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapDirectSelectiveStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;
    private final boolean nullsAllowed;
    private final boolean nonNullsAllowed;
    private final boolean outputRequired;
    @Nullable
    private final MapType outputType;

    private final SelectiveStreamReader keyReader;
    private final SelectiveStreamReader valueReader;

    private final LocalMemoryContext systemMemoryContext;

    private int readOffset;
    private int nestedReadOffset;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<LongInputStream> lengthStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream lengthStream;

    private boolean rowGroupOpen;
    @Nullable
    private int[] offsets;
    private boolean[] nulls;
    private int[] outputPositions;
    private int outputPositionCount;
    private boolean outputPositionsReadOnly;
    private boolean allNulls;
    private int[] nestedLengths;
    private int[] nestedOffsets;
    private int[] nestedPositions;
    private int[] nestedOutputPositions;
    private int nestedOutputPositionCount;

    private boolean valuesInUse;

    public MapDirectSelectiveStreamReader(
            StreamDescriptor streamDescriptor,
            Map<Subfield, TupleDomainFilter> filters,
            List<Subfield> requiredSubfields,
            Optional<Type> outputType,
            DateTimeZone hiveStorageTimeZone,
            AggregatedMemoryContext systemMemoryContext)
    {
        checkArgument(filters.keySet().stream().map(Subfield::getPath).allMatch(List::isEmpty), "filters on nested columns are not supported yet");

        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null").newLocalMemoryContext(MapDirectSelectiveStreamReader.class.getSimpleName());
        this.outputRequired = requireNonNull(outputType, "outputType is null").isPresent();
        this.outputType = outputType.map(MapType.class::cast).orElse(null);

        TupleDomainFilter filter = getTopLevelFilter(filters).orElse(null);
        this.nullsAllowed = filter == null || filter.testNull();
        this.nonNullsAllowed = filter == null || filter.testNonNull();

        List<StreamDescriptor> nestedStreams = streamDescriptor.getNestedStreams();

        Optional<Type> keyOutputType = outputType.map(MapType.class::cast).map(MapType::getKeyType);
        Optional<Type> valueOutputType = outputType.map(MapType.class::cast).map(MapType::getValueType);

        Map<Subfield, TupleDomainFilter> keyFilter = makeKeyFilter(nestedStreams.get(0).getStreamType(), requiredSubfields)
                .map(f -> ImmutableMap.of(new Subfield("c"), f))
                .orElse(ImmutableMap.of());

        this.keyReader = SelectiveStreamReaders.createStreamReader(nestedStreams.get(0), keyFilter, keyOutputType, ImmutableList.of(), hiveStorageTimeZone, systemMemoryContext.newAggregatedMemoryContext());
        this.valueReader = SelectiveStreamReaders.createStreamReader(nestedStreams.get(1), ImmutableMap.of(), valueOutputType, ImmutableList.of(), hiveStorageTimeZone, systemMemoryContext.newAggregatedMemoryContext());
    }

    private static Optional<TupleDomainFilter> makeKeyFilter(OrcType.OrcTypeKind orcType, List<Subfield> requiredSubfields)
    {
        if (requiredSubfields.isEmpty()) {
            return Optional.empty();
        }

        switch (orcType) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG: {
                long[] requiredIndices = requiredSubfields.stream()
                        .map(Subfield::getPath)
                        .map(path -> path.get(0))
                        .map(Subfield.LongSubscript.class::cast)
                        .mapToLong(Subfield.LongSubscript::getIndex)
                        .toArray();

                if (requiredIndices.length == 0) {
                    return Optional.empty();
                }

                if (requiredIndices.length == 1) {
                    return Optional.of(BigintRange.of(requiredIndices[0], requiredIndices[0], false));
                }

                return Optional.of(BigintValues.of(requiredIndices, false));
            }
            case STRING:
            case CHAR:
            case VARCHAR: {
                byte[][] requiredIndices = requiredSubfields.stream()
                        .map(Subfield::getPath)
                        .map(path -> path.get(0))
                        .map(Subfield.StringSubscript.class::cast)
                        .map(Subfield.StringSubscript::getIndex)
                        .map(String::getBytes)
                        .toArray(byte[][]::new);

                if (requiredIndices.length == 0) {
                    return Optional.empty();
                }

                if (requiredIndices.length == 1) {
                    return Optional.of(BytesRange.of(requiredIndices[0], false, requiredIndices[0], false, false));
                }

                return Optional.of(BytesValues.of(requiredIndices, false));
            }
            default:
                return Optional.empty();
        }
    }

    private static Optional<TupleDomainFilter> getTopLevelFilter(Map<Subfield, TupleDomainFilter> filters)
    {
        Map<Subfield, TupleDomainFilter> topLevelFilters = Maps.filterEntries(filters, entry -> entry.getKey().getPath().isEmpty());
        if (topLevelFilters.isEmpty()) {
            return Optional.empty();
        }

        checkArgument(topLevelFilters.size() == 1, "MAP column may have at most one top-level range filter");
        TupleDomainFilter filter = Iterables.getOnlyElement(topLevelFilters.values());
        checkArgument(filter == IS_NULL || filter == IS_NOT_NULL, "Top-level range filter on MAP column must be IS NULL or IS NOT NULL");
        return Optional.of(filter);
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

        if (!nullsAllowed || !nonNullsAllowed) {
            outputPositions = ensureCapacity(outputPositions, positionCount);
        }
        else {
            outputPositions = positions;
            outputPositionsReadOnly = true;
        }

        offsets = ensureCapacity(offsets, positionCount + 1);
        offsets[0] = 0;

        nestedLengths = ensureCapacity(nestedLengths, positionCount);
        nestedOffsets = ensureCapacity(nestedOffsets, positionCount + 1);

        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (presentStream == null) {
            readNoNulls(offset, positions, positionCount);
        }
        else {
            readWithNulls(offset, positions, positionCount);
        }

        return outputPositionCount;
    }

    private void readNoNulls(int offset, int[] positions, int positionCount)
            throws IOException
    {
        if (!nonNullsAllowed) {
            outputPositionCount = 0;
            return;
        }

        if (readOffset < offset) {
            nestedReadOffset += lengthStream.sum(offset - readOffset);
        }

        int streamPosition = 0;
        int nestedOffset = 0;

        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                nestedOffset += lengthStream.sum(position - streamPosition);
                streamPosition = position;
            }

            streamPosition++;

            int length = toIntExact(lengthStream.next());
            offsets[i + 1] = offsets[i] + length;
            nestedLengths[i] = length;
            nestedOffsets[i] = nestedOffset;
            nestedOffset += length;
        }
        nestedOffsets[positionCount] = nestedOffset;

        int nestedPositionCount = populateNestedPositions(positionCount, nestedOffset);

        outputPositions = positions;
        outputPositionCount = positionCount;
        outputPositionsReadOnly = true;
        readOffset = offset + streamPosition;

        readKeyValueStreams(nestedPositionCount);
        nestedReadOffset += nestedOffset;
    }

    private void readWithNulls(int offset, int[] positions, int positionCount)
            throws IOException
    {
        if (readOffset < offset) {
            int dataToSkip = presentStream.countBitsSet(offset - readOffset);
            nestedReadOffset += lengthStream.sum(dataToSkip);
        }

        nulls = ensureCapacity(nulls, positionCount);
        outputPositionCount = 0;

        int streamPosition = 0;
        int nonNullPositionCount = 0;
        int nestedOffset = 0;

        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                int dataToSkip = presentStream.countBitsSet(position - streamPosition);
                nestedOffset += lengthStream.sum(dataToSkip);
                streamPosition = position;
            }

            streamPosition++;

            if (presentStream.nextBit()) {
                // not null
                int length = toIntExact(lengthStream.next());
                if (nonNullsAllowed) {
                    nulls[outputPositionCount] = false;
                    offsets[outputPositionCount + 1] = offsets[outputPositionCount] + length;
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;

                    nestedLengths[nonNullPositionCount] = length;
                    nestedOffsets[nonNullPositionCount] = nestedOffset;
                    nonNullPositionCount++;
                }
                nestedOffset += length;
            }
            else {
                // null
                if (nullsAllowed) {
                    nulls[outputPositionCount] = true;
                    offsets[outputPositionCount + 1] = offsets[outputPositionCount];
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
        }

        nestedOffsets[nonNullPositionCount] = nestedOffset;

        int nestedPositionCount = populateNestedPositions(nonNullPositionCount, nestedOffset);

        if (nestedPositionCount > 0) {
            readKeyValueStreams(nestedPositionCount);
        }
        else {
            allNulls = true;
        }

        readOffset = offset + streamPosition;
        nestedReadOffset += nestedOffset;
    }

    private int populateNestedPositions(int positionCount, int nestedOffset)
    {
        nestedPositions = ensureCapacity(nestedPositions, nestedOffset);
        int nestedPositionCount = 0;
        for (int i = 0; i < positionCount; i++) {
            for (int j = 0; j < nestedLengths[i]; j++) {
                nestedPositions[nestedPositionCount++] = nestedOffsets[i] + j;
            }
        }
        return nestedPositionCount;
    }

    private void readKeyValueStreams(int positionCount)
            throws IOException
    {
        int readCount = keyReader.read(nestedReadOffset, nestedPositions, positionCount);
        int[] readPositions = keyReader.getReadPositions();

        if (readCount == 0) {
            nestedOutputPositionCount = 0;
            return;
        }

        if (readCount < positionCount) {
            int positionIndex = 0;
            int nextPosition = readPositions[positionIndex];
            int offset = 0;
            int previousOffset = 0;
            for (int i = 0; i < outputPositionCount; i++) {
                int length = 0;
                for (int j = previousOffset; j < offsets[i + 1]; j++) {
                    if (j == nextPosition) {
                        length++;
                        positionIndex++;
                        if (positionIndex >= readCount) {
                            break;
                        }
                        nextPosition = readPositions[positionIndex];
                    }
                }
                offset += length;
                previousOffset = offsets[i + 1];
                offsets[i + 1] = offset;

                if (positionIndex >= readCount) {
                    for (int j = i + 1; j < outputPositionCount; j++) {
                        offsets[j + 1] = offset;
                    }
                    break;
                }
            }
        }

        int valueReadCount = valueReader.read(nestedReadOffset, readPositions, readCount);
        assert valueReadCount == readCount;

        nestedOutputPositions = ensureCapacity(nestedOutputPositions, readCount);
        System.arraycopy(readPositions, 0, nestedOutputPositions, 0, readCount);
        nestedOutputPositionCount = readCount;
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        lengthStream = lengthStreamSource.openStream();

        rowGroupOpen = true;
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
            return createNullBlock(outputType, positionCount);
        }

        boolean includeNulls = nullsAllowed && presentStream != null;
        if (outputPositionCount == positionCount) {
            Block keyBlock = keyReader.getBlock(nestedOutputPositions, nestedOutputPositionCount);
            Block valueBlock = valueReader.getBlock(nestedOutputPositions, nestedOutputPositionCount);

            Block block = outputType.createBlockFromKeyValue(positionCount, Optional.ofNullable(includeNulls ? nulls : null), offsets, keyBlock, valueBlock);
            nulls = null;
            offsets = null;
            return block;
        }

        int[] offsetsCopy = new int[positionCount + 1];
        boolean[] nullsCopy = null;
        if (includeNulls) {
            nullsCopy = new boolean[positionCount];
        }

        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        int nestedSkipped = 0;
        nestedOutputPositionCount = 0;

        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                nestedSkipped += offsets[i + 1] - offsets[i];
                continue;
            }

            assert outputPositions[i] == nextPosition;

            offsetsCopy[positionIndex + 1] = offsets[i + 1] - nestedSkipped;
            for (int j = offsetsCopy[positionIndex]; j < offsetsCopy[positionIndex + 1]; j++) {
                nestedOutputPositions[nestedOutputPositionCount] = nestedOutputPositions[nestedOutputPositionCount + nestedSkipped];
                nestedOutputPositionCount++;
            }

            if (nullsCopy != null) {
                nullsCopy[positionIndex] = this.nulls[i];
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }

            nextPosition = positions[positionIndex];
        }

        if (nestedOutputPositionCount == 0) {
            return createNullBlock(outputType, positionCount);
        }

        Block keyBlock = keyReader.getBlock(nestedOutputPositions, nestedOutputPositionCount);
        Block valueBlock = valueReader.getBlock(nestedOutputPositions, nestedOutputPositionCount);
        return outputType.createBlockFromKeyValue(positionCount, Optional.ofNullable(includeNulls ? nullsCopy : null), offsetsCopy, keyBlock, valueBlock);
    }

    private static RunLengthEncodedBlock createNullBlock(Type type, int positionCount)
    {
        return new RunLengthEncodedBlock(type.createBlockBuilder(null, 1).appendNull().build(), positionCount);
    }

    @Override
    public BlockLease getBlockView(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (allNulls) {
            return newLease(createNullBlock(outputType, positionCount));
        }

        boolean includeNulls = nullsAllowed && presentStream != null;
        if (positionCount != outputPositionCount) {
            compactValues(positions, positionCount, includeNulls);

            if (nestedOutputPositionCount == 0) {
                allNulls = true;
                return newLease(createNullBlock(outputType, positionCount));
            }
        }

        BlockLease keyBlockLease = keyReader.getBlockView(nestedOutputPositions, nestedOutputPositionCount);
        BlockLease valueBlockLease = valueReader.getBlockView(nestedOutputPositions, nestedOutputPositionCount);
        return newLease(outputType.createBlockFromKeyValue(positionCount, Optional.ofNullable(includeNulls ? nulls : null), offsets, keyBlockLease.get(), valueBlockLease.get()), keyBlockLease, valueBlockLease);
    }

    private void compactValues(int[] positions, int positionCount, boolean compactNulls)
    {
        if (outputPositionsReadOnly) {
            outputPositions = Arrays.copyOf(outputPositions, outputPositionCount);
            outputPositionsReadOnly = false;
        }

        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        int nestedSkipped = 0;
        nestedOutputPositionCount = 0;
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                nestedSkipped += offsets[i + 1] - offsets[i];
                continue;
            }

            assert outputPositions[i] == nextPosition;

            offsets[positionIndex + 1] = offsets[i + 1] - nestedSkipped;
            for (int j = offsets[positionIndex]; j < offsets[positionIndex + 1]; j++) {
                nestedOutputPositions[nestedOutputPositionCount] = nestedOutputPositions[nestedOutputPositionCount + nestedSkipped];
                nestedOutputPositionCount++;
            }

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

    private BlockLease newLease(Block block, BlockLease...fieldBlockLeases)
    {
        valuesInUse = true;
        return ClosingBlockLease.newLease(block, () -> {
            for (BlockLease lease : fieldBlockLeases) {
                lease.close();
            }
            valuesInUse = false;
        });
    }

    @Override
    public void throwAnyError(int[] positions, int positionCount)
    {
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
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        lengthStreamSource = missingStreamSource(LongInputStream.class);

        readOffset = 0;
        nestedReadOffset = 0;

        presentStream = null;
        lengthStream = null;

        rowGroupOpen = false;

        keyReader.startStripe(dictionaryStreamSources, encoding);
        valueReader.startStripe(dictionaryStreamSources, encoding);
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        lengthStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, LENGTH, LongInputStream.class);

        readOffset = 0;
        nestedReadOffset = 0;

        presentStream = null;
        lengthStream = null;

        rowGroupOpen = false;

        keyReader.startRowGroup(dataStreamSources);
        valueReader.startRowGroup(dataStreamSources);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                sizeOf(outputPositions) +
                sizeOf(offsets) +
                sizeOf(nulls) +
                sizeOf(nestedLengths) +
                sizeOf(nestedOffsets) +
                sizeOf(nestedPositions) +
                sizeOf(nestedOutputPositions) +
                keyReader.getRetainedSizeInBytes() +
                valueReader.getRetainedSizeInBytes();
    }
}
