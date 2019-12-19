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
import com.facebook.presto.orc.TupleDomainFilter.BytesRange;
import com.facebook.presto.orc.TupleDomainFilter.BytesValues;
import com.facebook.presto.orc.TupleDomainFilter.NullsFilter;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.PrestoException;
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
import static com.facebook.presto.orc.TupleDomainFilterUtils.toBigintValues;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.SelectiveStreamReaders.createNestedStreamReader;
import static com.facebook.presto.orc.reader.SelectiveStreamReaders.initializeOutputPositions;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class MapDirectSelectiveStreamReader
        implements SelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapDirectSelectiveStreamReader.class).instanceSize();
    private static final Block EMPTY_BLOCK = BOOLEAN.createBlockBuilder(null, 0).build();

    private final StreamDescriptor streamDescriptor;
    private final int level;
    private final MapFilter mapFilter;
    private final NullsFilter nullsFilter;
    private final boolean nullsAllowed;
    private final boolean nonNullsAllowed;
    private final boolean outputRequired;
    @Nullable
    private final MapType outputType;

    private final SelectiveStreamReader keyReader;
    private final SelectiveStreamReader valueReader;

    private final LocalMemoryContext systemMemoryContext;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<LongInputStream> lengthStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream lengthStream;

    private boolean rowGroupOpen;
    private int readOffset;
    @Nullable
    private int[] offsets;
    private boolean[] nulls;
    private int[] outputPositions;
    private int outputPositionCount;
    private boolean[] indexOutOfBounds;    // positions with not enough elements to evaluate all element filters;
                                           // aligned with outputPositions
    private boolean allNulls;

    private int nestedReadOffset;          // offset within elementStream relative to row group start
    private int[] nestedOffsets;           // offsets within elementStream relative to nestedReadOffset
    private int[] nestedLengths;           // aligned with nestedOffsets
    private int[] nestedPositions;         // positions in elementStream corresponding to non-null positions passed to read();
                                           // relative to nestedReadOffset; calculated from nestedOffsets and nestedLengths
    private int[] nestedOutputPositions;   // subset of nestedPositions that passed filters on the elements
    private int nestedOutputPositionCount; // number of valid entries in nestedPositions

    private boolean valuesInUse;

    public MapDirectSelectiveStreamReader(
            StreamDescriptor streamDescriptor,
            Map<Subfield, TupleDomainFilter> filters,
            List<Subfield> requiredSubfields,
            MapFilter mapFilter,
            int subfieldLevel,  // 0 - top level
            Optional<Type> outputType,
            DateTimeZone hiveStorageTimeZone,
            boolean legacyMapSubscript,
            AggregatedMemoryContext systemMemoryContext)
    {
        requireNonNull(filters, "filters is null");
        requireNonNull(requiredSubfields, "subfields is null");

        if (mapFilter != null) {
            checkArgument(subfieldLevel > 0, "SubfieldFilter is not expected at the top level");
            checkArgument(filters.isEmpty(), "Range filters are not expected at mid level");
        }

        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null").newLocalMemoryContext(MapDirectSelectiveStreamReader.class.getSimpleName());
        this.outputRequired = requireNonNull(outputType, "outputType is null").isPresent();
        this.outputType = outputType.map(MapType.class::cast).orElse(null);
        this.level = subfieldLevel;

        if (mapFilter != null) {
            nullsAllowed = true;
            nonNullsAllowed = true;
            this.mapFilter = mapFilter;
            nullsFilter = mapFilter.getParent().getNullsFilter();
        }
        else if (!filters.isEmpty()) {
            Optional<TupleDomainFilter> topLevelFilter = getTopLevelFilter(filters);
            if (topLevelFilter.isPresent()) {
                nullsAllowed = topLevelFilter.get() == IS_NULL;
                nonNullsAllowed = !nullsAllowed;
            }
            else {
                nullsAllowed = filters.values().stream().allMatch(TupleDomainFilter::testNull);
                nonNullsAllowed = true;
            }

            if (filters.keySet().stream().anyMatch(path -> !path.getPath().isEmpty())) {
                this.mapFilter = new MapFilter(streamDescriptor, filters, legacyMapSubscript);
            }
            else {
                this.mapFilter = null;
            }

            nullsFilter = null;
        }
        else {
            nullsAllowed = true;
            nonNullsAllowed = true;
            this.mapFilter = null;
            nullsFilter = null;
        }

        List<StreamDescriptor> nestedStreams = streamDescriptor.getNestedStreams();

        if (outputRequired || this.mapFilter != null) {
            Map<Subfield, TupleDomainFilter> keyFilter = ImmutableMap.of(new Subfield("c"), makeKeyFilter(nestedStreams.get(0).getOrcTypeKind(), requiredSubfields));

            List<Subfield> elementRequiredSubfields = ImmutableList.of();
            if (requiredSubfields.stream().map(Subfield::getPath).allMatch(path -> path.size() > 1)) {
                elementRequiredSubfields = requiredSubfields.stream()
                        .map(subfield -> subfield.tail(subfield.getRootName()))
                        .distinct()
                        .collect(toImmutableList());
            }

            Type keyOutputType = outputType
                    .map(MapType.class::cast)
                    .map(MapType::getKeyType)
                    .orElseGet(() -> MapFilter.getKeyOutputType(nestedStreams.get(0)));
            Optional<Type> valueOutputType = outputType.map(MapType.class::cast).map(MapType::getValueType);

            this.keyReader = SelectiveStreamReaders.createStreamReader(nestedStreams.get(0), keyFilter, Optional.of(keyOutputType), ImmutableList.of(), hiveStorageTimeZone, legacyMapSubscript, systemMemoryContext.newAggregatedMemoryContext());
            this.valueReader = createNestedStreamReader(nestedStreams.get(1), level + 1, Optional.ofNullable(this.mapFilter), valueOutputType, elementRequiredSubfields, hiveStorageTimeZone, legacyMapSubscript, systemMemoryContext.newAggregatedMemoryContext());
        }
        else {
            this.keyReader = null;
            this.valueReader = null;
        }
    }

    private static TupleDomainFilter makeKeyFilter(OrcTypeKind orcType, List<Subfield> requiredSubfields)
    {
        // Map entries with a null key are skipped in the Hive ORC reader, so skip them here also

        if (requiredSubfields.isEmpty()) {
            return IS_NOT_NULL;
        }

        if (requiredSubfields.stream()
                .map(Subfield::getPath)
                .map(path -> path.get(0))
                .anyMatch(Subfield.AllSubscripts.class::isInstance)) {
            return IS_NOT_NULL;
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
                        .distinct()
                        .toArray();

                if (requiredIndices.length == 0) {
                    return IS_NOT_NULL;
                }

                if (requiredIndices.length == 1) {
                    return BigintRange.of(requiredIndices[0], requiredIndices[0], false);
                }

                return toBigintValues(requiredIndices, false);
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
                    return IS_NOT_NULL;
                }

                if (requiredIndices.length == 1) {
                    return BytesRange.of(requiredIndices[0], false, requiredIndices[0], false, false);
                }

                return BytesValues.of(requiredIndices, false);
            }
            default:
                return IS_NOT_NULL;
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
        checkArgument(positionCount > 0 || mapFilter != null, "positionCount must be greater than zero");
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (!rowGroupOpen) {
            openRowGroup();
        }

        allNulls = false;

        if (outputRequired) {
            offsets = ensureCapacity(offsets, positionCount + 1);
        }

        if (nullsAllowed && presentStream != null && (outputRequired || mapFilter != null)) {
            nulls = ensureCapacity(nulls, positionCount);
        }

        outputPositions = initializeOutputPositions(outputPositions, positions, positionCount);

        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            nestedReadOffset += skip(offset - readOffset);
        }

        int streamPosition;
        if (lengthStream == null && presentStream != null) {
            streamPosition = readAllNulls(positions, positionCount);
        }
        else {
            streamPosition = readNotAllNulls(positions, positionCount);
        }

        readOffset = offset + streamPosition;
        return outputPositionCount;
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        lengthStream = lengthStreamSource.openStream();

        rowGroupOpen = true;
    }

    private int skip(int items)
            throws IOException
    {
        if (lengthStream == null) {
            presentStream.skip(items);
            return 0;
        }

        if (presentStream != null) {
            int lengthsToSkip = presentStream.countBitsSet(items);
            return toIntExact(lengthStream.sum(lengthsToSkip));
        }

        return toIntExact(lengthStream.sum(items));
    }

    private int readAllNulls(int[] positions, int positionCount)
            throws IOException
    {
        presentStream.skip(positions[positionCount - 1]);
        if (nullsAllowed) {
            if (nullsFilter != null) {
                outputPositionCount = 0;
                for (int i = 0; i < positionCount; i++) {
                    if (nullsFilter.testNull()) {
                        outputPositions[outputPositionCount] = positions[i];
                        outputPositionCount++;
                    }
                    else {
                        outputPositionCount -= nullsFilter.getPrecedingPositionsToFail();
                        i += nullsFilter.getSucceedingPositionsToFail();
                    }
                }
            }
            else {
                outputPositionCount = positionCount;
            }
        }
        else {
            outputPositionCount = 0;
        }

        if (mapFilter != null) {
            mapFilter.populateElementFilters(0, null, null, 0, null);
        }

        allNulls = true;
        return positions[positionCount - 1] + 1;
    }

    private int readNotAllNulls(int[] positions, int positionCount)
            throws IOException
    {
        // populate nestedOffsets, nestedLengths, and nulls
        nestedOffsets = ensureCapacity(nestedOffsets, positionCount + 1);
        nestedLengths = ensureCapacity(nestedLengths, positionCount);

        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        int streamPosition = 0;
        int skippedElements = 0;
        int elementPositionCount = 0;

        outputPositionCount = 0;

        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skippedElements += skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                if (nullsAllowed && (nullsFilter == null || nullsFilter.testNull())) {
                    if (outputRequired || mapFilter != null) {
                        nulls[outputPositionCount] = true;
                    }

                    nestedOffsets[outputPositionCount] = elementPositionCount + skippedElements;
                    nestedLengths[outputPositionCount] = 0;

                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                int length = toIntExact(lengthStream.next());

                if (nonNullsAllowed && (nullsFilter == null || nullsFilter.testNonNull())) {
                    nestedOffsets[outputPositionCount] = elementPositionCount + skippedElements;
                    nestedLengths[outputPositionCount] = length;

                    elementPositionCount += length;

                    if ((outputRequired || mapFilter != null) && nullsAllowed && presentStream != null) {
                        nulls[outputPositionCount] = false;
                    }

                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
                else {
                    skippedElements += length;
                }
            }

            streamPosition++;

            if (nullsFilter != null) {
                int precedingPositionsToFail = nullsFilter.getPrecedingPositionsToFail();

                for (int j = 0; j < precedingPositionsToFail; j++) {
                    int length = nestedLengths[outputPositionCount - 1 - j];
                    skippedElements += length;
                    elementPositionCount -= length;
                }
                outputPositionCount -= precedingPositionsToFail;

                int succeedingPositionsToFail = nullsFilter.getSucceedingPositionsToFail();
                if (succeedingPositionsToFail > 0) {
                    int positionsToSkip = 0;
                    for (int j = 0; j < succeedingPositionsToFail; j++) {
                        i++;
                        int nextPosition = positions[i];
                        positionsToSkip += 1 + nextPosition - streamPosition;
                        streamPosition = nextPosition + 1;
                    }
                    skippedElements += skip(positionsToSkip);
                }
            }
        }
        nestedOffsets[outputPositionCount] = elementPositionCount + skippedElements;

        if (keyReader != null) {
            int nestedPositionCount = populateNestedPositions(elementPositionCount);
            readKeyValueStreams(nestedPositionCount);
        }

        nestedReadOffset += elementPositionCount + skippedElements;

        return streamPosition;
    }

    private int populateNestedPositions(int nestedPositionCount)
    {
        nestedPositions = ensureCapacity(nestedPositions, nestedPositionCount);
        int index = 0;
        for (int i = 0; i < outputPositionCount; i++) {
            for (int j = 0; j < nestedLengths[i]; j++) {
                nestedPositions[index] = nestedOffsets[i] + j;
                index++;
            }
        }
        return index;
    }

    private void populateOutputPositionsNoFilter(int elementPositionCount)
    {
        if (outputRequired) {
            nestedOutputPositionCount = elementPositionCount;
            nestedOutputPositions = ensureCapacity(nestedOutputPositions, elementPositionCount);
            System.arraycopy(nestedPositions, 0, nestedOutputPositions, 0, elementPositionCount);

            int offset = 0;
            for (int i = 0; i < outputPositionCount; i++) {
                offsets[i] = offset;
                offset += nestedLengths[i];
            }
            offsets[outputPositionCount] = offset;
        }
    }

    private void populateOutputPositionsWithFilter(int elementPositionCount)
    {
        nestedOutputPositionCount = 0;
        nestedOutputPositions = ensureCapacity(nestedOutputPositions, elementPositionCount);

        indexOutOfBounds = mapFilter.getTopLevelIndexOutOfBounds();

        int outputPosition = 0;
        int elementOffset = 0;
        boolean[] positionsFailed = mapFilter.getTopLevelFailed();
        for (int i = 0; i < outputPositionCount; i++) {
            if (!positionsFailed[i]) {
                indexOutOfBounds[outputPosition] = indexOutOfBounds[i];
                outputPositions[outputPosition] = outputPositions[i];
                if (outputRequired) {
                    if (nullsAllowed && presentStream != null) {
                        nulls[outputPosition] = nulls[i];
                    }
                    offsets[outputPosition] = nestedOutputPositionCount;
                    for (int j = 0; j < nestedLengths[i]; j++) {
                        nestedOutputPositions[nestedOutputPositionCount] = nestedPositions[elementOffset + j];
                        nestedOutputPositionCount++;
                    }
                }
                outputPosition++;
            }
            elementOffset += nestedLengths[i];
        }
        if (outputRequired) {
            this.offsets[outputPosition] = nestedOutputPositionCount;
        }

        outputPositionCount = outputPosition;
    }

    private void readKeyValueStreams(int positionCount)
            throws IOException
    {
        int readCount = 0;
        int[] readPositions = null;
        if (positionCount > 0) {
            readCount = keyReader.read(nestedReadOffset, nestedPositions, positionCount);
            readPositions = keyReader.getReadPositions();
        }

        if (readCount == 0) {
            Arrays.fill(nestedOffsets, 0, outputPositionCount + 1, 0);
            Arrays.fill(nestedLengths, 0, outputPositionCount, 0);
        }
        else if (readCount < positionCount) {
            int positionIndex = 0;
            int nextPosition = readPositions[positionIndex];
            int previousOffset = 0;
            for (int i = 0; i < outputPositionCount; i++) {
                int length = 0;
                for (int j = previousOffset; j < previousOffset + nestedLengths[i]; j++) {
                    if (nestedPositions[j] == nextPosition) {
                        length++;
                        positionIndex++;
                        if (positionIndex >= readCount) {
                            break;
                        }
                        nextPosition = readPositions[positionIndex];
                    }
                    else {
                        assert nestedPositions[j] < nextPosition;
                    }
                }
                previousOffset += nestedLengths[i];
                nestedOffsets[i + 1] = nestedOffsets[i] + length;
                nestedLengths[i] = length;

                if (positionIndex >= readCount) {
                    for (int j = i + 1; j < outputPositionCount; j++) {
                        nestedOffsets[j + 1] = nestedOffsets[i + 1];
                        nestedLengths[j] = 0;
                    }
                    break;
                }
            }
            System.arraycopy(readPositions, 0, nestedPositions, 0, readCount);
        }

        if (mapFilter != null) {
            if (readCount == 0) {
                mapFilter.populateElementFilters(outputPositionCount, nulls, nestedLengths, readCount, EMPTY_BLOCK);
            }
            else {
                try (BlockLease keyBlockView = keyReader.getBlockView(readPositions, readCount)) {
                    mapFilter.populateElementFilters(outputPositionCount, nulls, nestedLengths, readCount, keyBlockView.get());
                }
            }
        }

        if (readCount > 0) {
            valueReader.read(nestedReadOffset, readPositions, readCount);
        }

        if (mapFilter == null || level > 0) {
            populateOutputPositionsNoFilter(readCount);
        }
        else {
            populateOutputPositionsWithFilter(readCount);
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
        checkState(positionCount <= outputPositionCount, "Not enough values: " + outputPositionCount + " < " + positionCount);
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (allNulls) {
            return createNullBlock(outputType, positionCount);
        }

        boolean includeNulls = nullsAllowed && presentStream != null;
        if (positionCount == outputPositionCount) {
            Block block = createMapBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), offsets);
            nulls = null;
            offsets = null;
            return block;
        }

        int[] offsetsCopy = new int[positionCount + 1];
        boolean[] nullsCopy = null;
        if (includeNulls) {
            nullsCopy = new boolean[positionCount];
        }

        nestedOutputPositionCount = 0;

        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        int nestedSkipped = 0;

        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                nestedSkipped += offsets[i + 1] - offsets[i];
                continue;
            }

            assert outputPositions[i] == nextPosition;

            offsetsCopy[positionIndex + 1] = offsets[i + 1] - nestedSkipped;
            if (nullsCopy != null) {
                nullsCopy[positionIndex] = this.nulls[i];
            }
            for (int j = 0; j < offsets[i + 1] - offsets[i]; j++) {
                nestedOutputPositions[nestedOutputPositionCount] = nestedOutputPositions[nestedOutputPositionCount + nestedSkipped];
                nestedOutputPositionCount++;
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }

            nextPosition = positions[positionIndex];
        }
        return createMapBlock(positionCount, Optional.ofNullable(nullsCopy), offsetsCopy);
    }

    private Block createMapBlock(int positionCount, Optional<boolean[]> nulls, int[] offsets)
    {
        Block keyBlock;
        Block valueBlock;
        if (nestedOutputPositionCount == 0) {
            keyBlock = EMPTY_BLOCK;
            valueBlock = EMPTY_BLOCK;
        }
        else {
            keyBlock = keyReader.getBlock(nestedOutputPositions, nestedOutputPositionCount);
            valueBlock = valueReader.getBlock(nestedOutputPositions, nestedOutputPositionCount);
        }

        return outputType.createBlockFromKeyValue(positionCount, nulls, offsets, keyBlock, valueBlock);
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
        }

        if (nestedOutputPositionCount == 0) {
            return newLease(outputType.createBlockFromKeyValue(positionCount, Optional.ofNullable(includeNulls ? nulls : null), offsets, EMPTY_BLOCK, EMPTY_BLOCK));
        }

        BlockLease keyBlockLease = keyReader.getBlockView(nestedOutputPositions, nestedOutputPositionCount);
        BlockLease valueBlockLease = valueReader.getBlockView(nestedOutputPositions, nestedOutputPositionCount);
        return newLease(outputType.createBlockFromKeyValue(positionCount, Optional.ofNullable(includeNulls ? nulls : null), offsets, keyBlockLease.get(), valueBlockLease.get()), keyBlockLease, valueBlockLease);
    }

    private void compactValues(int[] positions, int positionCount, boolean compactNulls)
    {
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

            for (int j = 0; j < offsets[i + 1] - offsets[i]; j++) {
                nestedOutputPositions[nestedOutputPositionCount] = nestedOutputPositions[nestedOutputPositionCount + nestedSkipped];
                nestedOutputPositionCount++;
            }

            offsets[positionIndex + 1] = offsets[i + 1] - nestedSkipped;
            if (compactNulls) {
                nulls[positionIndex] = nulls[i];
            }
            outputPositions[positionIndex] = nextPosition;
            if (indexOutOfBounds != null) {
                indexOutOfBounds[positionIndex] = indexOutOfBounds[i];
            }

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
        if (indexOutOfBounds == null) {
            return;
        }

        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            assert outputPositions[i] == nextPosition;

            if (indexOutOfBounds[i]) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Key not present in map");
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }

            nextPosition = positions[positionIndex];
        }
    }

    @Override
    public void close()
    {
        if (keyReader != null) {
            keyReader.close();
            valueReader.close();
        }

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

        if (keyReader != null) {
            keyReader.startStripe(dictionaryStreamSources, encoding);
            valueReader.startStripe(dictionaryStreamSources, encoding);
        }
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

        if (keyReader != null) {
            keyReader.startRowGroup(dataStreamSources);
            valueReader.startRowGroup(dataStreamSources);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                sizeOf(offsets) +
                sizeOf(nulls) +
                sizeOf(outputPositions) +
                sizeOf(indexOutOfBounds) +
                sizeOf(nestedOffsets) +
                sizeOf(nestedLengths) +
                sizeOf(nestedPositions) +
                sizeOf(nestedOutputPositions) +
                (mapFilter != null ? mapFilter.getRetainedSizeInBytes() : 0) +
                (keyReader != null ? keyReader.getRetainedSizeInBytes() : 0) +
                (valueReader != null ? valueReader.getRetainedSizeInBytes() : 0);
    }
}
