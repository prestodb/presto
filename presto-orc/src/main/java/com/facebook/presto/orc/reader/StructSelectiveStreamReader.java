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
import com.facebook.presto.common.block.BlockLease;
import com.facebook.presto.common.block.ClosingBlockLease;
import com.facebook.presto.common.block.RowBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.RowType.Field;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.OrcAggregatedMemoryContext;
import com.facebook.presto.orc.OrcLocalMemoryContext;
import com.facebook.presto.orc.OrcRecordReaderOptions;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.Stripe;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NOT_NULL;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NULL;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.SelectiveStreamReaders.initializeOutputPositions;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class StructSelectiveStreamReader
        implements SelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(StructSelectiveStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;
    private final boolean nullsAllowed;
    private final boolean nonNullsAllowed;
    private final boolean outputRequired;
    @Nullable
    private final Type outputType;

    private final Map<String, SelectiveStreamReader> nestedReaders;
    private final SelectiveStreamReader[] orderedNestedReaders;
    private final boolean missingFieldFilterIsFalse;

    private final OrcLocalMemoryContext systemMemoryContext;

    private int readOffset;
    private int nestedReadOffset;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private boolean rowGroupOpen;
    private boolean[] nulls;
    private int[] outputPositions;
    private int outputPositionCount;
    private boolean allNulls;
    private int[] nestedPositions;
    private int[] nestedOutputPositions;
    private int nestedOutputPositionCount;

    private boolean valuesInUse;

    public StructSelectiveStreamReader(
            StreamDescriptor streamDescriptor,
            Map<Subfield, TupleDomainFilter> filters,
            List<Subfield> requiredSubfields,
            Optional<Type> outputType,
            DateTimeZone hiveStorageTimeZone,
            OrcRecordReaderOptions options,
            boolean legacyMapSubscript,
            OrcAggregatedMemoryContext systemMemoryContext)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null").newOrcLocalMemoryContext(StructSelectiveStreamReader.class.getSimpleName());
        this.outputRequired = requireNonNull(outputType, "outputType is null").isPresent();
        this.outputType = outputType.orElse(null);

        if (filters.isEmpty()) {
            nullsAllowed = true;
            nonNullsAllowed = true;
        }
        else {
            Optional<TupleDomainFilter> topLevelFilter = getTopLevelFilter(filters);
            if (topLevelFilter.isPresent()) {
                nullsAllowed = topLevelFilter.get() == IS_NULL;
                nonNullsAllowed = !nullsAllowed;
            }
            else {
                nullsAllowed = filters.values().stream().allMatch(TupleDomainFilter::testNull);
                nonNullsAllowed = true;
            }
        }

        Map<String, StreamDescriptor> nestedStreams = Maps.uniqueIndex(
                streamDescriptor.getNestedStreams(), stream -> stream.getFieldName().toLowerCase(Locale.ENGLISH));

        Optional<Map<String, List<Subfield>>> requiredFields = getRequiredFields(requiredSubfields);

        // TODO streamDescriptor may be missing some fields (due to schema evolution, e.g. add field?)
        // TODO fields in streamDescriptor may be out of order (due to schema evolution, e.g. remove field?)

        Set<String> fieldsWithFilters = filters.keySet().stream()
                .map(Subfield::getPath)
                .filter(path -> path.size() > 0)
                .map(path -> path.get(0))
                .filter(Subfield.NestedField.class::isInstance)
                .map(Subfield.NestedField.class::cast)
                .map(Subfield.NestedField::getName)
                .collect(toImmutableSet());

        if (!checkMissingFieldFilters(nestedStreams.values(), filters)) {
            this.missingFieldFilterIsFalse = true;
            this.nestedReaders = ImmutableMap.of();
            this.orderedNestedReaders = new SelectiveStreamReader[0];
        }
        else if (outputRequired || !fieldsWithFilters.isEmpty()) {
            ImmutableMap.Builder<String, SelectiveStreamReader> nestedReaders = ImmutableMap.builder();
            Map<String, Field> nestedTypes = outputType.isPresent() ? ((RowType) this.outputType).getFields().stream()
                    .collect(toImmutableMap(field -> field.getName()
                            .orElseThrow(() -> new IllegalArgumentException(
                                    "ROW type does not have field names declared: " + this.outputType))
                            .toLowerCase(Locale.ENGLISH), Function.identity()))
                    : ImmutableMap.of();
            Set<String> structFields = outputType.isPresent() ? nestedTypes.keySet() : nestedStreams.keySet();
            for (String fieldName : structFields) {
                StreamDescriptor nestedStream = nestedStreams.get(fieldName);
                boolean requiredField = requiredFields.map(names -> names.containsKey(fieldName)).orElse(outputRequired);
                Optional<Type> fieldOutputType = Optional.ofNullable(nestedTypes.get(fieldName)).map(Field::getType);

                if (nestedStream == null) {
                    verify(fieldOutputType.isPresent(), "Missing output type for subfield " + fieldName);
                    nestedReaders.put(fieldName, new MissingFieldStreamReader(fieldOutputType.get()));
                }
                else {
                    if (requiredField || fieldsWithFilters.contains(fieldName)) {
                        Map<Subfield, TupleDomainFilter> nestedFilters = filters.entrySet().stream()
                                .filter(entry -> entry.getKey().getPath().size() > 0)
                                .filter(entry -> ((Subfield.NestedField) entry.getKey().getPath().get(0)).getName().equalsIgnoreCase(fieldName))
                                .collect(toImmutableMap(entry -> entry.getKey().tail(fieldName), Map.Entry::getValue));
                        List<Subfield> nestedRequiredSubfields = requiredFields.map(names -> names.get(fieldName)).orElse(ImmutableList.of());
                        SelectiveStreamReader nestedReader = SelectiveStreamReaders.createStreamReader(
                                nestedStream,
                                nestedFilters,
                                fieldOutputType,
                                nestedRequiredSubfields,
                                hiveStorageTimeZone,
                                options,
                                legacyMapSubscript,
                                systemMemoryContext.newOrcAggregatedMemoryContext());
                        nestedReaders.put(fieldName, nestedReader);
                    }
                    else {
                        nestedReaders.put(fieldName, new PruningStreamReader(nestedStream, fieldOutputType));
                    }
                }
            }

            this.missingFieldFilterIsFalse = false;
            this.nestedReaders = nestedReaders.build();
            this.orderedNestedReaders = orderNestedReaders(this.nestedReaders, fieldsWithFilters);
        }
        else {
            // No need to read the elements when output is not required and the filter is a simple IS [NOT] NULL
            this.missingFieldFilterIsFalse = false;
            this.nestedReaders = ImmutableMap.of();
            this.orderedNestedReaders = new SelectiveStreamReader[0];
        }
    }

    private boolean checkMissingFieldFilters(Collection<StreamDescriptor> nestedStreams, Map<Subfield, TupleDomainFilter> filters)
    {
        if (filters.isEmpty()) {
            return true;
        }

        Set<String> presentFieldNames = nestedStreams.stream()
                .map(StreamDescriptor::getFieldName)
                .map(name -> name.toLowerCase(Locale.ENGLISH))
                .collect(toImmutableSet());

        for (Map.Entry<Subfield, TupleDomainFilter> entry : filters.entrySet()) {
            Subfield subfield = entry.getKey();
            if (subfield.getPath().isEmpty()) {
                continue;
            }

            String fieldName = ((Subfield.NestedField) subfield.getPath().get(0)).getName();
            if (presentFieldNames.contains(fieldName)) {
                continue;
            }

            // Check out the filter. If filter allows nulls, then all rows pass, otherwise, no row passes.
            TupleDomainFilter filter = entry.getValue();
            checkArgument(filter.isDeterministic(), "Non-deterministic range filters are not supported yet");

            if (!filter.testNull()) {
                return false;
            }
        }

        return true;
    }

    private static SelectiveStreamReader[] orderNestedReaders(Map<String, SelectiveStreamReader> nestedReaders, Set<String> fieldsWithFilters)
    {
        SelectiveStreamReader[] order = new SelectiveStreamReader[nestedReaders.size()];

        int index = 0;
        for (Map.Entry<String, SelectiveStreamReader> entry : nestedReaders.entrySet()) {
            if (fieldsWithFilters.contains(entry.getKey())) {
                order[index++] = entry.getValue();
            }
        }
        for (Map.Entry<String, SelectiveStreamReader> entry : nestedReaders.entrySet()) {
            if (!fieldsWithFilters.contains(entry.getKey())) {
                order[index++] = entry.getValue();
            }
        }

        return order;
    }

    @Override
    public int read(int offset, int[] positions, int positionCount)
            throws IOException
    {
        checkArgument(positionCount > 0, "positionCount must be greater than zero");
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (missingFieldFilterIsFalse) {
            outputPositionCount = 0;
            return 0;
        }

        if (!rowGroupOpen) {
            openRowGroup();
        }

        allNulls = false;

        outputPositions = initializeOutputPositions(outputPositions, positions, positionCount);

        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (presentStream == null) {
            // no nulls
            if (nonNullsAllowed) {
                if (nestedReaders.isEmpty()) {
                    outputPositionCount = positionCount;
                }
                else {
                    readNestedStreams(offset, positions, positionCount);
                    if (nestedOutputPositionCount > 0) {
                        outputPositions = initializeOutputPositions(outputPositions, nestedOutputPositions, positionCount);
                    }
                    outputPositionCount = nestedOutputPositionCount;
                }
                readOffset = offset + positions[positionCount - 1];
            }
            else {
                outputPositionCount = 0;
            }
        }
        else {
            // some or all nulls
            if (readOffset < offset) {
                nestedReadOffset += presentStream.countBitsSet(offset - readOffset);
            }

            nulls = ensureCapacity(nulls, positionCount);
            nestedPositions = ensureCapacity(nestedPositions, positionCount);
            outputPositionCount = 0;

            int streamPosition = 0;
            int nestedPositionCount = 0;
            int nullCount = 0;

            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (position > streamPosition) {
                    int nonNullCount = presentStream.countBitsSet(position - streamPosition);
                    nullCount += position - streamPosition - nonNullCount;
                    streamPosition = position;
                }

                streamPosition++;

                if (presentStream.nextBit()) {
                    // not null
                    if (nonNullsAllowed) {
                        nulls[outputPositionCount] = false;
                        if (!nullsAllowed) {
                            outputPositions[outputPositionCount] = position;
                        }
                        outputPositionCount++;
                        nestedPositions[nestedPositionCount++] = position - nullCount;
                    }
                }
                else {
                    // null
                    if (nullsAllowed) {
                        nulls[outputPositionCount] = true;
                        if (!nonNullsAllowed) {
                            outputPositions[outputPositionCount] = position;
                        }
                        outputPositionCount++;
                    }
                    nullCount++;
                }
            }

            if (!nestedReaders.isEmpty()) {
                if (nestedPositionCount == 0) {
                    allNulls = true;
                }
                else {
                    readNestedStreams(nestedReadOffset, nestedPositions, nestedPositionCount);
                    pruneOutputPositions(nestedPositionCount);
                }
                nestedReadOffset += streamPosition - nullCount;
            }

            readOffset = offset + streamPosition;
        }

        return outputPositionCount;
    }

    private void pruneOutputPositions(int nestedPositionCount)
    {
        if (nestedOutputPositionCount == 0) {
            allNulls = true;
        }

        if (nestedOutputPositionCount < nestedPositionCount) {
            int nestedIndex = 0;
            int skipped = 0;
            int nestedOutputIndex = 0;
            for (int i = 0; i < outputPositionCount; i++) {
                outputPositions[i - skipped] = outputPositions[i];
                if (nullsAllowed) {
                    nulls[i - skipped] = nulls[i];

                    if (nulls[i]) {
                        continue;
                    }
                }

                if (nestedOutputIndex >= nestedOutputPositionCount) {
                    skipped++;
                }
                else if (nestedPositions[nestedIndex] < nestedOutputPositions[nestedOutputIndex]) {
                    skipped++;
                }
                else {
                    nestedOutputIndex++;
                }

                nestedIndex++;
            }
        }
        outputPositionCount -= nestedPositionCount - nestedOutputPositionCount;
    }

    private void readNestedStreams(int offset, int[] positions, int positionCount)
            throws IOException
    {
        int[] readPositions = positions;
        int readPositionCount = positionCount;
        for (SelectiveStreamReader reader : orderedNestedReaders) {
            readPositionCount = reader.read(offset, readPositions, readPositionCount);
            if (readPositionCount == 0) {
                break;
            }

            readPositions = reader.getReadPositions();
        }

        if (readPositionCount > 0) {
            nestedOutputPositions = ensureCapacity(nestedOutputPositions, positionCount);
            System.arraycopy(readPositions, 0, nestedOutputPositions, 0, readPositionCount);
        }
        nestedOutputPositionCount = readPositionCount;
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();

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
            Block block = RowBlock.fromFieldBlocks(positionCount, Optional.ofNullable(includeNulls ? nulls : null), getFieldBlocks());
            nulls = null;
            return block;
        }

        boolean[] nullsCopy = null;
        if (includeNulls) {
            nullsCopy = new boolean[positionCount];
        }

        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        int nestedIndex = 0;
        nestedOutputPositionCount = 0;

        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                if (!includeNulls || !nulls[i]) {
                    nestedIndex++;
                }
                continue;
            }

            assert outputPositions[i] == nextPosition;

            if (!includeNulls || !nulls[i]) {
                nestedOutputPositions[nestedOutputPositionCount++] = nestedOutputPositions[nestedIndex];
                nestedIndex++;
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

        return RowBlock.fromFieldBlocks(positionCount, Optional.ofNullable(includeNulls ? nullsCopy : null), getFieldBlocks());
    }

    private Block[] getFieldBlocks()
    {
        Block[] blocks = new Block[nestedReaders.size()];
        int i = 0;
        for (SelectiveStreamReader reader : nestedReaders.values()) {
            blocks[i++] = reader.getBlock(nestedOutputPositions, nestedOutputPositionCount);
        }
        return blocks;
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

        BlockLease[] fieldBlockLeases = new BlockLease[nestedReaders.size()];
        Block[] fieldBlocks = new Block[nestedReaders.size()];
        int i = 0;
        for (SelectiveStreamReader reader : nestedReaders.values()) {
            fieldBlockLeases[i] = reader.getBlockView(nestedOutputPositions, nestedOutputPositionCount);
            fieldBlocks[i] = fieldBlockLeases[i].get();
            i++;
        }

        return newLease(RowBlock.fromFieldBlocks(positionCount, Optional.ofNullable(includeNulls ? nulls : null), fieldBlocks), fieldBlockLeases);
    }

    private void compactValues(int[] positions, int positionCount, boolean compactNulls)
    {
        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        int nestedIndex = 0;
        nestedOutputPositionCount = 0;
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                if (!compactNulls || !nulls[i]) {
                    nestedIndex++;
                }
                continue;
            }

            assert outputPositions[i] == nextPosition;

            if (!compactNulls || !nulls[i]) {
                nestedOutputPositions[nestedOutputPositionCount++] = nestedOutputPositions[nestedIndex];
                nestedIndex++;
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

    private BlockLease newLease(Block block, BlockLease... fieldBlockLeases)
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
        nestedReaders.values().stream().forEach(SelectiveStreamReader::close);

        outputPositions = null;
        nulls = null;
        nestedOutputPositions = null;
        nestedPositions = null;

        presentStream = null;
        presentStreamSource = null;
        systemMemoryContext.close();
    }

    @Override
    public void startStripe(Stripe stripe)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);

        readOffset = 0;
        nestedReadOffset = 0;

        presentStream = null;

        rowGroupOpen = false;

        for (SelectiveStreamReader reader : nestedReaders.values()) {
            reader.startStripe(stripe);
        }
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);

        readOffset = 0;
        nestedReadOffset = 0;

        presentStream = null;

        rowGroupOpen = false;

        for (SelectiveStreamReader reader : nestedReaders.values()) {
            reader.startRowGroup(dataStreamSources);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(outputPositions) + sizeOf(nestedPositions) + sizeOf(nestedOutputPositions) + sizeOf(nulls) +
                nestedReaders.values().stream()
                        .mapToLong(SelectiveStreamReader::getRetainedSizeInBytes)
                        .sum();
    }

    private static Optional<TupleDomainFilter> getTopLevelFilter(Map<Subfield, TupleDomainFilter> filters)
    {
        Map<Subfield, TupleDomainFilter> topLevelFilters = Maps.filterEntries(filters, entry -> entry.getKey().getPath().isEmpty());
        if (topLevelFilters.isEmpty()) {
            return Optional.empty();
        }

        checkArgument(topLevelFilters.size() == 1, "ROW column may have at most one top-level range filter");
        TupleDomainFilter filter = Iterables.getOnlyElement(topLevelFilters.values());
        checkArgument(filter == IS_NULL || filter == IS_NOT_NULL, "Top-level range filter on ROW column must be IS NULL or IS NOT NULL");
        return Optional.of(filter);
    }

    private static Optional<Map<String, List<Subfield>>> getRequiredFields(List<Subfield> requiredSubfields)
    {
        if (requiredSubfields.isEmpty()) {
            return Optional.empty();
        }

        Map<String, List<Subfield>> fields = new HashMap<>();
        for (Subfield subfield : requiredSubfields) {
            List<Subfield.PathElement> path = subfield.getPath();
            String name = ((Subfield.NestedField) path.get(0)).getName().toLowerCase(Locale.ENGLISH);
            fields.computeIfAbsent(name, k -> new ArrayList<>());
            if (path.size() > 1) {
                fields.get(name).add(new Subfield("c", path.subList(1, path.size())));
            }
        }

        return Optional.of(ImmutableMap.copyOf(fields));
    }

    private static final class PruningStreamReader
            implements SelectiveStreamReader
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(PruningStreamReader.class).instanceSize();

        private final StreamDescriptor streamDescriptor;
        @Nullable
        private final Type outputType;
        private int[] outputPositions;
        private int outputPositionCount;

        private PruningStreamReader(StreamDescriptor streamDescriptor, Optional<Type> outputType)
        {
            this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
            this.outputType = requireNonNull(outputType, "outputType is null").orElse(null);
        }

        @Override
        public int read(int offset, int[] positions, int positionCount)
        {
            outputPositions = initializeOutputPositions(outputPositions, positions, positionCount);
            outputPositionCount = positionCount;
            return outputPositionCount;
        }

        @Override
        public int[] getReadPositions()
        {
            return outputPositions;
        }

        @Override
        public Block getBlock(int[] positions, int positionCount)
        {
            checkState(outputType != null, "This stream reader doesn't produce output");
            return createNullBlock(outputType, positionCount);
        }

        @Override
        public BlockLease getBlockView(int[] positions, int positionCount)
        {
            checkState(outputType != null, "This stream reader doesn't produce output");
            return ClosingBlockLease.newLease(createNullBlock(outputType, positionCount));
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
            outputPositions = null;
        }

        @Override
        public void startStripe(Stripe stripe)
        {
        }

        @Override
        public void startRowGroup(InputStreamSources dataStreamSources)
        {
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + sizeOf(outputPositions);
        }
    }

    private static final class MissingFieldStreamReader
            implements SelectiveStreamReader
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(MissingFieldStreamReader.class).instanceSize();
        private final Type outputType;
        private int[] outputPositions;

        MissingFieldStreamReader(Type type)
        {
            this.outputType = requireNonNull(type, "type is required");
        }

        @Override
        public int read(int offset, int[] positions, int positionCount)
        {
            outputPositions = initializeOutputPositions(outputPositions, positions, positionCount);
            return positionCount;
        }

        @Override
        public int[] getReadPositions()
        {
            return outputPositions;
        }

        @Override
        public Block getBlock(int[] positions, int positionCount)
        {
            return createNullBlock(outputType, positionCount);
        }

        @Override
        public BlockLease getBlockView(int[] positions, int positionCount)
        {
            return ClosingBlockLease.newLease(createNullBlock(outputType, positionCount));
        }

        @Override
        public void throwAnyError(int[] positions, int positionCount)
        {
        }

        @Override
        public String toString()
        {
            return toStringHelper(this).toString();
        }

        @Override
        public void close()
        {
        }

        @Override
        public void startStripe(Stripe stripe)
        {
        }

        @Override
        public void startRowGroup(InputStreamSources dataStreamSources)
        {
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + sizeOf(outputPositions);
        }
    }
}
