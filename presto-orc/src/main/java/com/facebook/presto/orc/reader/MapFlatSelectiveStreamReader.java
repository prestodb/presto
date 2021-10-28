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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockLease;
import com.facebook.presto.common.block.ClosingBlockLease;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.block.VariableWidthBlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.OrcAggregatedMemoryContext;
import com.facebook.presto.orc.OrcLocalMemoryContext;
import com.facebook.presto.orc.OrcRecordReaderOptions;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.Stripe;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.DwrfSequenceEncoding;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;

import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NOT_NULL;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NULL;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.IN_MAP;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.SelectiveStreamReaders.initializeOutputPositions;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class MapFlatSelectiveStreamReader
        implements SelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapFlatSelectiveStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;
    private final boolean legacyMapSubscript;

    // This is the StreamDescriptor for the value stream with sequence ID 0, it is used to derive StreamDescriptors for the
    // value streams with other sequence IDs
    private final StreamDescriptor baseValueStreamDescriptor;
    private final OrcTypeKind keyOrcTypeKind;
    private final DateTimeZone hiveStorageTimeZone;

    private final boolean nullsAllowed;
    private final boolean nonNullsAllowed;
    private final boolean outputRequired;
    @Nullable
    private final MapType outputType;
    @Nullable
    private final Set<Long> requiredLongKeys;
    @Nullable
    private final Set<String> requiredStringKeys;

    private int[] keyIndices;   // indices of the required keys in the encodings array passed to startStripe
    private int keyCount;       // number of valid entries in keyIndices array

    private final List<InputStreamSource<BooleanInputStream>> inMapStreamSources = new ArrayList<>();
    private final List<BooleanInputStream> inMapStreams = new ArrayList<>();
    private final List<SelectiveStreamReader> valueStreamReaders = new ArrayList<>();
    private final List<StreamDescriptor> valueStreamDescriptors = new ArrayList<>();

    private Block keyBlock;
    private int readOffset;
    private int[] nestedReadOffsets;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private boolean rowGroupOpen;
    private int[] offsets;
    private boolean[] nulls;
    private int[] outputPositions;
    private int outputPositionCount;
    private boolean outputPositionsReadOnly;
    private boolean allNulls;
    private boolean valuesInUse;

    private int[] nestedLengths;
    private int[][] nestedPositions;
    private int[] nestedPositionCounts;
    private int[][] nestedOutputPositions;
    private boolean[][] inMap;

    private final OrcAggregatedMemoryContext systemMemoryContext;
    private final OrcLocalMemoryContext localMemoryContext;
    private final OrcRecordReaderOptions options;

    public MapFlatSelectiveStreamReader(
            StreamDescriptor streamDescriptor,
            Map<Subfield, TupleDomainFilter> filters,
            List<Subfield> requiredSubfields,
            Optional<Type> outputType,
            DateTimeZone hiveStorageTimeZone,
            OrcRecordReaderOptions options,
            boolean legacyMapSubscript,
            OrcAggregatedMemoryContext systemMemoryContext)
    {
        this.options = requireNonNull(options);
        checkArgument(filters.keySet().stream().map(Subfield::getPath).allMatch(List::isEmpty), "filters on nested columns are not supported yet");
        checkArgument(streamDescriptor.getNestedStreams().size() == 2, "there must be exactly 2 nested stream descriptor");

        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.legacyMapSubscript = legacyMapSubscript;
        this.keyOrcTypeKind = streamDescriptor.getNestedStreams().get(0).getOrcTypeKind();
        this.baseValueStreamDescriptor = streamDescriptor.getNestedStreams().get(1);
        this.hiveStorageTimeZone = requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");

        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.localMemoryContext = systemMemoryContext.newOrcLocalMemoryContext(MapFlatSelectiveStreamReader.class.getSimpleName());
        this.outputRequired = requireNonNull(outputType, "outputType is null").isPresent();
        this.outputType = outputType.map(MapType.class::cast).orElse(null);

        TupleDomainFilter filter = getTopLevelFilter(filters).orElse(null);
        this.nullsAllowed = filter == null || filter.testNull();
        this.nonNullsAllowed = filter == null || filter.testNonNull();

        requireNonNull(requiredSubfields, "requiredSubfields is null");
        if (requiredSubfields.stream()
                .map(Subfield::getPath)
                .map(path -> path.get(0))
                .anyMatch(Subfield.AllSubscripts.class::isInstance)) {
            requiredLongKeys = null;
            requiredStringKeys = null;
        }
        else {
            switch (keyOrcTypeKind) {
                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                    requiredLongKeys = requiredSubfields.stream()
                            .map(Subfield::getPath)
                            .map(path -> path.get(0))
                            .map(Subfield.LongSubscript.class::cast)
                            .map(Subfield.LongSubscript::getIndex)
                            .collect(toImmutableSet());
                    requiredStringKeys = null;
                    return;
                case STRING:
                case BINARY:
                    requiredStringKeys = requiredSubfields.stream()
                            .map(Subfield::getPath)
                            .map(path -> path.get(0))
                            .map(Subfield.StringSubscript.class::cast)
                            .map(Subfield.StringSubscript::getIndex)
                            .collect(toImmutableSet());
                    requiredLongKeys = null;
                    return;
                default:
                    requiredStringKeys = null;
                    requiredLongKeys = null;
            }
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

        if (outputRequired && nullsAllowed && presentStream != null) {
            nulls = ensureCapacity(nulls, positionCount);
        }

        outputPositions = initializeOutputPositions(outputPositions, positions, positionCount);

        if (presentStream != null && keyCount == 0) {
            readAllNulls(positions, positionCount);
        }
        else {
            readNotAllNulls(offset, positions, positionCount);
        }

        localMemoryContext.setBytes(getRetainedSizeInBytes());

        return outputPositionCount;
    }

    private void readAllNulls(int[] positions, int positionCount)
            throws IOException
    {
        presentStream.skip(positions[positionCount - 1]);

        allNulls = true;

        if (!nullsAllowed) {
            outputPositionCount = 0;
        }
        else {
            outputPositionCount = positionCount;
            outputPositions = positions;
            outputPositionsReadOnly = true;
        }
    }

    private void readNotAllNulls(int offset, int[] positions, int positionCount)
            throws IOException
    {
        int streamPosition = 0;

        int[] nonNullPositions = new int[positionCount];
        int[] nullPositions = new int[positionCount];
        int nonNullPositionCount = 0;
        int nullPositionCount = 0;
        int nonNullSkipped = 0;

        if (presentStream == null) {
            if (readOffset < offset) {
                for (int i = 0; i < keyCount; i++) {
                    nestedReadOffsets[i] += inMapStreams.get(i).countBitsSet(offset - readOffset);
                }
            }

            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (position > streamPosition) {
                    nonNullSkipped += position - streamPosition;
                    streamPosition = position;
                }

                nonNullPositions[i] = i + nonNullSkipped;
                streamPosition++;
            }
            nonNullPositionCount = positionCount;
        }
        else {
            if (readOffset < offset) {
                int skipped = presentStream.countBitsSet(offset - readOffset);
                if (skipped > 0) {
                    for (int i = 0; i < keyCount; i++) {
                        nestedReadOffsets[i] += inMapStreams.get(i).countBitsSet(skipped);
                    }
                }
            }
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (position > streamPosition) {
                    nonNullSkipped += presentStream.countBitsSet(position - streamPosition);
                    streamPosition = position;
                }

                streamPosition++;
                if (presentStream.nextBit()) {
                    // not null
                    if (nullsAllowed) {
                        nulls[i] = false;
                    }
                    nonNullPositions[nonNullPositionCount] = nonNullPositionCount + nonNullSkipped;
                    nonNullPositionCount++;
                }
                else {
                    if (nullsAllowed) {
                        nulls[i] = true;
                        nullPositions[nullPositionCount] = positions[i];
                        nullPositionCount++;
                    }
                }
            }
        }

        readOffset = offset + streamPosition;

        if (!nonNullsAllowed) {
            checkState(nullPositionCount == (positionCount - nonNullPositionCount), "nullPositionCount should be equal to postitionCount - nonNullPositionCount");
            outputPositionCount = nullPositionCount;
            allNulls = true;
            System.arraycopy(nullPositions, 0, outputPositions, 0, nullPositionCount);
        }
        else {
            nestedLengths = ensureCapacity(nestedLengths, nonNullPositionCount);
            Arrays.fill(nestedLengths, 0);

            for (int keyIndex = 0; keyIndex < keyCount; keyIndex++) {
                BooleanInputStream inMapStream = inMapStreams.get(keyIndex);

                nestedPositions[keyIndex] = ensureCapacity(nestedPositions[keyIndex], nonNullPositionCount);
                inMap[keyIndex] = ensureCapacity(inMap[keyIndex], nonNullPositionCount);

                int nestedStreamPosition = 0;

                int nestedSkipped = 0;
                int nestedPositionCount = 0;
                for (int i = 0; i < nonNullPositionCount; i++) {
                    int position = nonNullPositions[i];
                    if (position > nestedStreamPosition) {
                        nestedSkipped += inMapStream.countBitsSet(position - nestedStreamPosition);
                        nestedStreamPosition = position;
                    }

                    nestedStreamPosition++;
                    if (inMapStream.nextBit()) {
                        nestedPositions[keyIndex][nestedPositionCount] = nestedPositionCount + nestedSkipped;
                        nestedPositionCount++;

                        nestedLengths[i]++;
                        inMap[keyIndex][i] = true;
                    }
                    else {
                        inMap[keyIndex][i] = false;
                    }
                }

                if (nonNullSkipped > nestedStreamPosition - nonNullPositionCount) {
                    inMapStream.skip(nonNullSkipped - (nestedStreamPosition - nonNullPositionCount));
                }

                nestedPositionCounts[keyIndex] = nestedPositionCount;

                if (nestedPositionCount > 0) {
                    int readCount = valueStreamReaders.get(keyIndex).read(nestedReadOffsets[keyIndex], nestedPositions[keyIndex], nestedPositionCount);
                    verify(readCount == nestedPositionCount);
                }
                nestedReadOffsets[keyIndex] += nestedSkipped + nestedPositionCount;
            }

            if (nullsAllowed) {
                outputPositionCount = positionCount;
            }
            else {
                System.arraycopy(nonNullPositions, 0, outputPositions, 0, nonNullPositionCount);
                outputPositionCount = nonNullPositionCount;
            }
        }

        if (outputRequired) {
            nestedOutputPositions = ensureCapacity(nestedOutputPositions, keyCount);
            for (int i = 0; i < keyCount; i++) {
                int nestedPositionCount = nestedPositionCounts[i];
                if (nestedPositionCount > 0) {
                    nestedOutputPositions[i] = ensureCapacity(nestedOutputPositions[i], nestedPositionCount);
                    System.arraycopy(nestedPositions[i], 0, nestedOutputPositions[i], 0, nestedPositionCount);
                }
            }
        }
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();

        for (int i = 0; i < keyCount; i++) {
            BooleanInputStream inMapStream = requireNonNull(inMapStreamSources.get(i).openStream(), "missing inMapStream at position " + i);
            inMapStreams.add(inMapStream);
        }

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
        if (outputPositionCount != positionCount) {
            compactValues(positions, positionCount, includeNulls);
        }

        Block block = assembleMapBlock(includeNulls);
        nulls = null;
        offsets = null;
        return block;
    }

    private Block assembleMapBlock(boolean includeNulls)
    {
        offsets = ensureCapacity(offsets, outputPositionCount + 1);
        offsets[0] = 0;

        int offset = 0;
        int inMapIndex = 0;
        for (int i = 0; i < outputPositionCount; i++) {
            if (!includeNulls || !nulls[i]) {
                offset += nestedLengths[inMapIndex];
                inMapIndex++;
            }
            offsets[i + 1] = offset;
        }

        BlockLease[] valueBlockLeases = new BlockLease[keyCount];
        Block[] valueBlocks = new Block[keyCount];
        for (int i = 0; i < keyCount; i++) {
            if (nestedPositionCounts[i] > 0) {
                valueBlockLeases[i] = valueStreamReaders.get(i).getBlockView(nestedOutputPositions[i], nestedPositionCounts[i]);
                valueBlocks[i] = valueBlockLeases[i].get();
            }
            else {
                valueBlocks[i] = outputType.getKeyType().createBlockBuilder(null, 0).build();
            }
        }

        int[] keyIds = new int[offset];
        int count = 0;

        Type valueType = outputType.getValueType();
        BlockBuilder valueBlockBuilder = valueType.createBlockBuilder(null, offset);

        int[] valueBlockPositions = new int[keyCount];

        inMapIndex = 0;
        for (int i = 0; i < outputPositionCount; i++) {
            if (includeNulls && nulls[i]) {
                continue;
            }
            for (int keyIndex = 0; keyIndex < keyCount; keyIndex++) {
                if (inMap[keyIndex][inMapIndex]) {
                    valueType.appendTo(valueBlocks[keyIndex], valueBlockPositions[keyIndex], valueBlockBuilder);
                    valueBlockPositions[keyIndex]++;
                    keyIds[count++] = keyIndex;
                }
            }
            inMapIndex++;
        }

        for (int i = 0; i < keyCount; i++) {
            if (valueBlockLeases[i] != null) {
                valueBlockLeases[i].close();
            }
        }

        return outputType.createBlockFromKeyValue(outputPositionCount, Optional.ofNullable(includeNulls ? nulls : null), offsets, new DictionaryBlock(keyBlock, keyIds), valueBlockBuilder);
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

        return newLease(assembleMapBlock(includeNulls));
    }

    private BlockLease newLease(Block block)
    {
        valuesInUse = true;
        return ClosingBlockLease.newLease(block, () -> valuesInUse = false);
    }

    private void compactValues(int[] positions, int positionCount, boolean includeNulls)
    {
        if (outputPositionsReadOnly) {
            outputPositions = Arrays.copyOf(outputPositions, outputPositionCount);
            outputPositionsReadOnly = false;
        }

        int positionIndex = 0;
        int nextPosition = positions[positionIndex];

        int skipped = 0;
        int inMapSkipped = 0;
        int inMapIndex = 0;
        int[] nestedSkipped = new int[keyCount];
        int[] nestedIndex = new int[keyCount];

        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                // skip this position
                if (!includeNulls || !nulls[i]) {
                    // not null
                    for (int keyIndex = 0; keyIndex < keyCount; keyIndex++) {
                        if (inMap[keyIndex][inMapIndex]) {
                            nestedSkipped[keyIndex]++;
                            nestedIndex[keyIndex]++;
                        }
                    }
                    inMapSkipped++;
                    inMapIndex++;
                }
                skipped++;
                continue;
            }

            outputPositions[i - skipped] = outputPositions[i];
            if (includeNulls) {
                nulls[i - skipped] = nulls[i];
            }
            if (!includeNulls || !nulls[i]) {
                // not null
                nestedLengths[inMapIndex - inMapSkipped] = nestedLengths[inMapIndex];
                for (int keyIndex = 0; keyIndex < keyCount; keyIndex++) {
                    inMap[keyIndex][inMapIndex - inMapSkipped] = inMap[keyIndex][inMapIndex];
                    if (inMap[keyIndex][inMapIndex]) {
                        nestedOutputPositions[keyIndex][nestedIndex[keyIndex] - nestedSkipped[keyIndex]] = nestedOutputPositions[keyIndex][nestedIndex[keyIndex]];
                        nestedIndex[keyIndex]++;
                    }
                }
                inMapIndex++;
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }

            nextPosition = positions[positionIndex];
        }

        outputPositionCount = positionCount;
        for (int keyIndex = 0; keyIndex < keyCount; keyIndex++) {
            nestedPositionCounts[keyIndex] = nestedIndex[keyIndex] - nestedSkipped[keyIndex];
        }
    }

    @Override
    public void throwAnyError(int[] positions, int positionCount)
    {
    }

    @Override
    public void close()
    {
        keyIndices = null;
        nestedReadOffsets = null;
        offsets = null;
        nulls = null;
        outputPositions = null;
        nestedLengths = null;
        nestedPositions = null;
        nestedPositionCounts = null;
        nestedOutputPositions = null;
        inMap = null;
        valueStreamReaders.stream().forEach(SelectiveStreamReader::close);

        presentStream = null;
        presentStreamSource = null;

        localMemoryContext.close();
    }

    @Override
    public void startStripe(Stripe stripe)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);

        inMapStreamSources.clear();
        valueStreamDescriptors.clear();
        valueStreamReaders.clear();

        ColumnEncoding encoding = stripe.getColumnEncodings().get(baseValueStreamDescriptor.getStreamId());
        SortedMap<Integer, DwrfSequenceEncoding> additionalSequenceEncodings = Collections.emptySortedMap();
        // encoding or encoding.getAdditionalSequenceEncodings() may not be present when every map is empty or null
        if (encoding != null && encoding.getAdditionalSequenceEncodings().isPresent()) {
            additionalSequenceEncodings = encoding.getAdditionalSequenceEncodings().get();
        }
        keyIndices = ensureCapacity(keyIndices, additionalSequenceEncodings.size());
        keyCount = 0;

        // The ColumnEncoding with sequence ID 0 doesn't have any data associated with it
        int keyIndex = 0;
        for (Map.Entry<Integer, DwrfSequenceEncoding> entry : additionalSequenceEncodings.entrySet()) {
            if (!isRequiredKey(entry.getValue())) {
                keyIndex++;
                continue;
            }

            keyIndices[keyCount] = keyIndex;
            keyCount++;
            keyIndex++;

            int sequence = entry.getKey();

            inMapStreamSources.add(missingStreamSource(BooleanInputStream.class));

            StreamDescriptor valueStreamDescriptor = copyStreamDescriptorWithSequence(baseValueStreamDescriptor, sequence);
            valueStreamDescriptors.add(valueStreamDescriptor);

            SelectiveStreamReader valueStreamReader = SelectiveStreamReaders.createStreamReader(
                    valueStreamDescriptor,
                    ImmutableBiMap.of(),
                    Optional.ofNullable(outputType).map(MapType::getValueType),
                    ImmutableList.of(),
                    hiveStorageTimeZone,
                    options,
                    legacyMapSubscript,
                    systemMemoryContext.newOrcAggregatedMemoryContext());
            valueStreamReader.startStripe(stripe);
            valueStreamReaders.add(valueStreamReader);
        }

        keyBlock = getKeysBlock(ImmutableList.copyOf(additionalSequenceEncodings.values()));
        readOffset = 0;

        presentStream = null;

        rowGroupOpen = false;
    }

    private boolean isRequiredKey(DwrfSequenceEncoding value)
    {
        if (requiredLongKeys != null) {
            return requiredLongKeys.isEmpty() || requiredLongKeys.contains(value.getKey().getIntKey());
        }

        return requiredStringKeys.isEmpty() || requiredStringKeys.contains(value.getKey().getBytesKey().toStringUtf8());
    }

    /**
     * Creates StreamDescriptor which is a copy of this one with the value of sequence changed to
     * the value passed in.  Recursively calls itself on the nested streams.
     */
    private static StreamDescriptor copyStreamDescriptorWithSequence(StreamDescriptor streamDescriptor, int sequence)
    {
        List<StreamDescriptor> streamDescriptors = streamDescriptor.getNestedStreams().stream()
                .map(stream -> copyStreamDescriptorWithSequence(stream, sequence))
                .collect(toImmutableList());

        return new StreamDescriptor(
                streamDescriptor.getStreamName(),
                streamDescriptor.getStreamId(),
                streamDescriptor.getFieldName(),
                streamDescriptor.getOrcType(),
                streamDescriptor.getOrcDataSource(),
                streamDescriptors,
                sequence);
    }

    private Block getKeysBlock(List<DwrfSequenceEncoding> sequenceEncodings)
    {
        switch (keyOrcTypeKind) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return getIntegerKeysBlock(sequenceEncodings);
            case STRING:
            case BINARY:
                return getSliceKeysBlock(sequenceEncodings);
            default:
                throw new IllegalArgumentException("Unsupported flat map key type: " + keyOrcTypeKind);
        }
    }

    private Block getIntegerKeysBlock(List<DwrfSequenceEncoding> sequenceEncodings)
    {
        Type keyType;

        switch (keyOrcTypeKind) {
            case BYTE:
                keyType = TinyintType.TINYINT;
                break;
            case SHORT:
                keyType = SmallintType.SMALLINT;
                break;
            case INT:
                keyType = IntegerType.INTEGER;
                break;
            case LONG:
                keyType = BigintType.BIGINT;
                break;
            default:
                throw new IllegalArgumentException("Unsupported flat map key type: " + keyOrcTypeKind);
        }

        BlockBuilder blockBuilder = keyType.createBlockBuilder(null, sequenceEncodings.size());

        for (int i = 0; i < keyCount; i++) {
            keyType.writeLong(blockBuilder, sequenceEncodings.get(keyIndices[i]).getKey().getIntKey());
        }

        return blockBuilder.build();
    }

    private Block getSliceKeysBlock(List<DwrfSequenceEncoding> sequenceEncodings)
    {
        int bytes = 0;

        for (DwrfSequenceEncoding sequenceEncoding : sequenceEncodings) {
            bytes += sequenceEncoding.getKey().getBytesKey().size();
        }

        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, sequenceEncodings.size(), bytes);

        for (int i = 0; i < keyCount; i++) {
            Slice key = Slices.wrappedBuffer(sequenceEncodings.get(keyIndices[i]).getKey().getBytesKey().toByteArray());
            builder.writeBytes(key, 0, key.length());
            builder.closeEntry();
        }

        return builder.build();
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);

        for (int i = 0; i < keyCount; i++) {
            InputStreamSource<BooleanInputStream> inMapStreamSource = dataStreamSources.getInputStreamSource(valueStreamDescriptors.get(i), IN_MAP, BooleanInputStream.class);
            inMapStreamSources.set(i, inMapStreamSource);
        }

        readOffset = 0;
        nestedReadOffsets = ensureCapacity(nestedReadOffsets, keyCount);
        Arrays.fill(nestedReadOffsets, 0);

        nestedPositions = ensureCapacity(nestedPositions, keyCount);
        nestedPositionCounts = ensureCapacity(nestedPositionCounts, keyCount);
        inMap = ensureCapacity(inMap, keyCount);

        presentStream = null;
        inMapStreams.clear();

        rowGroupOpen = false;

        for (SelectiveStreamReader valueStreamReader : valueStreamReaders) {
            valueStreamReader.startRowGroup(dataStreamSources);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                sizeOf(keyIndices) +
                sizeOf(nestedReadOffsets) +
                sizeOf(offsets) +
                sizeOf(nulls) +
                sizeOf(outputPositions) +
                sizeOf(nestedLengths) +
                (nestedPositions != null ? Arrays.stream(nestedPositions).mapToLong(SizeOf::sizeOf).sum() : 0) +
                sizeOf(nestedPositionCounts) +
                (nestedOutputPositions != null ? Arrays.stream(nestedOutputPositions).mapToLong(SizeOf::sizeOf).sum() : 0) +
                (inMap != null ? Arrays.stream(inMap).mapToLong(SizeOf::sizeOf).sum() : 0) +
                (valueStreamReaders != null ? valueStreamReaders.stream().mapToLong(StreamReader::getRetainedSizeInBytes).sum() : 0);
    }
}
