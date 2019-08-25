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
import com.facebook.presto.orc.TupleDomainFilter.NullsFilter;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockLease;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.array.Arrays.ensureCapacity;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NOT_NULL;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NULL;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.SelectiveStreamReaders.createNestedStreamReader;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.block.ClosingBlockLease.newLease;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ListSelectiveStreamReader
        implements SelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ListSelectiveStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;
    private final int level;
    private final ListFilter listFilter;
    private final NullsFilter nullsFilter;
    private final boolean nullsAllowed;
    private final boolean nonNullsAllowed;
    private final boolean outputRequired;
    @Nullable
    private final Type outputType;
    // elementStreamReader is null if output is not required and filter is a simple IS [NOT] NULL
    @Nullable
    private final SelectiveStreamReader elementStreamReader;
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
    @Nullable
    private boolean[] nulls;
    @Nullable
    private int[] outputPositions;
    private int outputPositionCount;
    private boolean[] indexOutOfBounds;
    private boolean allNulls;

    private int elementReadOffset;          // offset within elementStream relative to row group start
    private int[] elementOffsets;           // offsets within elementStream relative to elementReadOffset
    private int[] elementLengths;           // aligned with elementOffsets
    private int[] elementPositions;         // positions in elementStream corresponding to positions passed to read(); relative to elementReadOffset
    private int elementOutputPositionCount;
    private int[] elementOutputPositions;

    private boolean valuesInUse;

    public ListSelectiveStreamReader(
            StreamDescriptor streamDescriptor,
            Map<Subfield, TupleDomainFilter> filters,
            List<Subfield> subfields,
            ListFilter listFilter,
            int subfieldLevel,  // 0 - top level
            Optional<Type> outputType,
            DateTimeZone hiveStorageTimeZone,
            AggregatedMemoryContext systemMemoryContext)
    {
        requireNonNull(filters, "filters is null");
        requireNonNull(subfields, "subfields is null");
        requireNonNull(systemMemoryContext, "systemMemoryContext is null");

        checkArgument(subfields.isEmpty(), "Subfield pruning is not supported yet");

        if (listFilter != null) {
            checkArgument(subfieldLevel > 0, "SubfieldFilter is not expected at the top level");
            checkArgument(filters.isEmpty(), "Range filters are not expected at mid level");
        }

        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.outputRequired = requireNonNull(outputType, "outputType is null").isPresent();
        this.outputType = outputType.orElse(null);
        this.level = subfieldLevel;

        if (listFilter != null) {
            nullsAllowed = true;
            nonNullsAllowed = true;
            this.listFilter = listFilter;
            nullsFilter = listFilter.getParent().getNullsFilter();
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
                this.listFilter = new ListFilter(streamDescriptor, filters);
            }
            else {
                this.listFilter = null;
            }

            nullsFilter = null;
        }
        else {
            nullsAllowed = true;
            nonNullsAllowed = true;
            this.listFilter = null;
            nullsFilter = null;
        }

        StreamDescriptor elementStreamDescriptor = streamDescriptor.getNestedStreams().get(0);
        Optional<Type> elementOutputType = outputType.map(type -> type.getTypeParameters().get(0));

        this.elementStreamReader = createNestedStreamReader(elementStreamDescriptor, level + 1, Optional.ofNullable(this.listFilter), elementOutputType, hiveStorageTimeZone, systemMemoryContext);
        this.systemMemoryContext = systemMemoryContext.newLocalMemoryContext(ListSelectiveStreamReader.class.getSimpleName());
    }

    private static Optional<TupleDomainFilter> getTopLevelFilter(Map<Subfield, TupleDomainFilter> filters)
    {
        Map<Subfield, TupleDomainFilter> topLevelFilters = Maps.filterEntries(filters, entry -> entry.getKey().getPath().isEmpty());
        if (topLevelFilters.isEmpty()) {
            return Optional.empty();
        }

        checkArgument(topLevelFilters.size() == 1, "ARRAY column may have at most one top-level range filter");
        TupleDomainFilter filter = Iterables.getOnlyElement(topLevelFilters.values());
        checkArgument(filter == IS_NULL || filter == IS_NOT_NULL, "Top-level range filter on ARRAY column must be IS NULL or IS NOT NULL");
        return Optional.of(filter);
    }

    @Override
    public int read(int offset, int[] positions, int positionCount)
            throws IOException
    {
        checkArgument(positionCount > 0, "positionCount must be greater than zero");
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (!rowGroupOpen) {
            openRowGroup();
        }

        allNulls = false;

        if (outputRequired) {
            offsets = ensureCapacity(offsets, positionCount + 1);
        }

        if (nullsAllowed && presentStream != null && (outputRequired || listFilter != null)) {
            nulls = ensureCapacity(nulls, positionCount);
        }

        if (!(nullsAllowed && nonNullsAllowed) || nullsFilter != null || listFilter != null) {
            outputPositions = ensureCapacity(outputPositions, positionCount);
        }
        else {
            outputPositions = positions;
        }

        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            elementReadOffset += skip(offset - readOffset);
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
                for (int i = 0; i < positionCount; i++) {
                    if (nullsFilter.testNull()) {
                        outputPositionCount++;
                    }
                }
            }
            else {
                outputPositionCount = positionCount;
                allNulls = true;
            }
        }
        else {
            outputPositionCount = 0;
        }

        return positions[positionCount - 1] + 1;
    }

    private int readNotAllNulls(int[] positions, int positionCount)
            throws IOException
    {
        // populate elementOffsets, elementLengths, and nulls
        elementOffsets = ensureCapacity(elementOffsets, positionCount + 1);
        elementLengths = ensureCapacity(elementLengths, positionCount);

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
                    if (outputRequired || listFilter != null) {
                        nulls[outputPositionCount] = true;
                    }

                    elementOffsets[outputPositionCount] = elementPositionCount + skippedElements;
                    elementLengths[outputPositionCount] = 0;

                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                int length = toIntExact(lengthStream.next());

                if (nonNullsAllowed && (nullsFilter == null || nullsFilter.testNonNull())) {
                    elementOffsets[outputPositionCount] = elementPositionCount + skippedElements;
                    elementLengths[outputPositionCount] = length;

                    elementPositionCount += length;

                    if ((outputRequired || listFilter != null) && nullsAllowed && presentStream != null) {
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
                    int length = elementLengths[outputPositionCount - 1 - j];
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
        elementOffsets[outputPositionCount] = elementPositionCount + skippedElements;

        populateElementPositions(elementPositionCount);

        if (listFilter != null) {
            listFilter.populateElementFilters(outputPositionCount, nulls, elementLengths, elementPositionCount);
        }

        if (elementStreamReader != null && elementPositionCount > 0) {
            elementStreamReader.read(elementReadOffset, elementPositions, elementPositionCount);
        }
        elementReadOffset += elementPositionCount + skippedElements;

        if (listFilter == null || level > 0) {
            populateOutputPositionsNoFilter(elementPositionCount);
        }
        else {
            populateOutputPositionsWithFilter(elementPositionCount);
        }

        return streamPosition;
    }

    private void populateElementPositions(int elementPositionCount)
    {
        elementPositions = ensureCapacity(elementPositions, elementPositionCount);

        int index = 0;
        for (int i = 0; i < outputPositionCount; i++) {
            for (int j = 0; j < elementLengths[i]; j++) {
                elementPositions[index] = elementOffsets[i] + j;
                index++;
            }
        }
    }

    private void populateOutputPositionsNoFilter(int elementPositionCount)
    {
        if (outputRequired) {
            elementOutputPositionCount = elementPositionCount;
            elementOutputPositions = ensureCapacity(elementOutputPositions, elementPositionCount);
            System.arraycopy(elementPositions, 0, elementOutputPositions, 0, elementPositionCount);

            int offset = 0;
            for (int i = 0; i < outputPositionCount; i++) {
                offsets[i] = offset;
                offset += elementLengths[i];
            }
            offsets[outputPositionCount] = offset;
        }
    }

    private void populateOutputPositionsWithFilter(int elementPositionCount)
    {
        elementOutputPositionCount = 0;
        elementOutputPositions = ensureCapacity(elementOutputPositions, elementPositionCount);

        indexOutOfBounds = listFilter.getTopLevelIndexOutOfBounds();

        int outputPosition = 0;
        int elementOffset = 0;
        boolean[] positionsFailed = listFilter.getTopLevelFailed();
        for (int i = 0; i < outputPositionCount; i++) {
            if (!positionsFailed[i]) {
                indexOutOfBounds[outputPosition] = indexOutOfBounds[i];
                outputPositions[outputPosition] = outputPositions[i];
                if (outputRequired) {
                    if (nullsAllowed && presentStream != null) {
                        nulls[outputPosition] = nulls[i];
                    }
                    offsets[outputPosition] = elementOutputPositionCount;
                    for (int j = 0; j < elementLengths[i]; j++) {
                        elementOutputPositions[elementOutputPositionCount] = elementPositions[elementOffset + j];
                        elementOutputPositionCount++;
                    }
                }
                outputPosition++;
            }
            elementOffset += elementLengths[i];
        }
        if (outputRequired) {
            this.offsets[outputPosition] = elementOutputPositionCount;
        }

        outputPositionCount = outputPosition;
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
        checkState(positionCount <= outputPositionCount, "Not enough values: " + outputPositionCount + ", " + positionCount);
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (allNulls) {
            return new RunLengthEncodedBlock(outputType.createBlockBuilder(null, 1).appendNull().build(), outputPositionCount);
        }

        boolean mayHaveNulls = nullsAllowed && presentStream != null;

        if (positionCount == outputPositionCount) {
            if (elementOutputPositionCount == 0) {
                return new RunLengthEncodedBlock(outputType.createBlockBuilder(null, 1).appendNull().build(), outputPositionCount);
            }

            Block block = ArrayBlock.fromElementBlock(positionCount, Optional.ofNullable(mayHaveNulls ? nulls : null), offsets, elementStreamReader.getBlock(elementOutputPositions, elementOutputPositionCount));
            nulls = null;
            offsets = null;
            return block;
        }

        int[] offsetsCopy = new int[positionCount + 1];
        boolean[] nullsCopy = null;
        if (mayHaveNulls) {
            nullsCopy = new boolean[positionCount];
        }

        elementOutputPositionCount = 0;

        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        int skippedElements = 0;
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                skippedElements += offsets[i + 1] - offsets[i];
                continue;
            }

            assert outputPositions[i] == nextPosition;

            offsetsCopy[positionIndex] = this.offsets[i] - skippedElements;
            if (nullsCopy != null) {
                nullsCopy[positionIndex] = this.nulls[i];
            }
            for (int j = 0; j < offsets[i + 1] - offsets[i]; j++) {
                elementOutputPositions[elementOutputPositionCount] = elementOutputPositions[elementOutputPositionCount + skippedElements];
                elementOutputPositionCount++;
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                offsetsCopy[positionCount] = this.offsets[i + 1] - skippedElements;
                break;
            }

            nextPosition = positions[positionIndex];
        }

        return ArrayBlock.fromElementBlock(positionCount, Optional.ofNullable(nullsCopy), offsetsCopy, elementStreamReader.getBlock(elementOutputPositions, elementOutputPositionCount));
    }

    @Override
    public BlockLease getBlockView(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (allNulls) {
            return newLease(new RunLengthEncodedBlock(outputType.createBlockBuilder(null, 1).appendNull().build(), outputPositionCount));
        }

        boolean includeNulls = nullsAllowed && presentStream != null;
        if (positionCount != outputPositionCount) {
            compactValues(positions, positionCount, includeNulls);
        }

        if (elementOutputPositionCount == 0) {
            return newLease(new RunLengthEncodedBlock(outputType.createBlockBuilder(null, 1).appendNull().build(), positionCount));
        }

        valuesInUse = true;
        BlockLease elementBlockLease = elementStreamReader.getBlockView(elementOutputPositions, elementOutputPositionCount);
        Block block = ArrayBlock.fromElementBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), offsets, elementBlockLease.get());
        return newLease(block, () -> closeBlockLease(elementBlockLease));
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
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Array subscript out of bounds");
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }

            nextPosition = positions[positionIndex];
        }
    }

    private void closeBlockLease(BlockLease elementBlockLease)
    {
        elementBlockLease.close();
        valuesInUse = false;
    }

    private void compactValues(int[] positions, int positionCount, boolean compactNulls)
    {
        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        int skippedElements = 0;
        elementOutputPositionCount = 0;
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                skippedElements += offsets[i + 1] - offsets[i];
                continue;
            }

            assert outputPositions[i] == nextPosition;

            for (int j = 0; j < offsets[i + 1] - offsets[i]; j++) {
                elementOutputPositions[elementOutputPositionCount] = elementOutputPositions[elementOutputPositionCount + skippedElements];
                elementOutputPositionCount++;
            }

            offsets[positionIndex] = offsets[i] - skippedElements;
            if (compactNulls) {
                nulls[positionIndex] = nulls[i];
            }
            outputPositions[positionIndex] = nextPosition;
            if (indexOutOfBounds != null) {
                indexOutOfBounds[positionIndex] = indexOutOfBounds[i];
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                offsets[positionCount] = offsets[i + 1] - skippedElements;
                break;
            }
            nextPosition = positions[positionIndex];
        }

        outputPositionCount = positionCount;
    }

    @Override
    public void close()
    {
        if (elementStreamReader != null) {
            elementStreamReader.close();
        }
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        lengthStreamSource = missingStreamSource(LongInputStream.class);

        readOffset = 0;
        elementReadOffset = 0;

        presentStream = null;
        lengthStream = null;

        rowGroupOpen = false;

        if (elementStreamReader != null) {
            elementStreamReader.startStripe(dictionaryStreamSources, encoding);
        }
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        lengthStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, LENGTH, LongInputStream.class);

        readOffset = 0;
        elementReadOffset = 0;

        presentStream = null;
        lengthStream = null;

        rowGroupOpen = false;

        if (elementStreamReader != null) {
            elementStreamReader.startRowGroup(dataStreamSources);
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
        return INSTANCE_SIZE + sizeOf(offsets) + sizeOf(nulls) + sizeOf(outputPositions) + sizeOf(indexOutOfBounds) +
                sizeOf(elementOffsets) + sizeOf(elementLengths) + sizeOf(elementPositions) +
                sizeOf(elementOutputPositions) +
                (listFilter != null ? listFilter.getRetainedSizeInBytes() : 0) +
                (elementStreamReader != null ? elementStreamReader.getRetainedSizeInBytes() : 0);
    }
}
