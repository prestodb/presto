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
import com.facebook.presto.orc.Filter;
import com.facebook.presto.orc.Filters;
import com.facebook.presto.orc.Filters.StructFilter;
import com.facebook.presto.orc.MissingSubscriptException;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.QualifyingSet;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.PageSourceOptions.ErrorSet;
import com.facebook.presto.spi.SubfieldPath;
import com.facebook.presto.spi.SubfieldPath.LongSubscript;
import com.facebook.presto.spi.SubfieldPath.PathElement;
import com.facebook.presto.spi.SubfieldPath.StringSubscript;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.block.VariableWidthBlock;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.orc.ResizedArrays.newIntArrayForReuse;
import static com.facebook.presto.orc.ResizedArrays.roundupSize;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.StreamReaders.createStreamReader;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.facebook.presto.spi.SubfieldPath.allSubscripts;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class MapDirectStreamReader
        extends RepeatedColumnReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapDirectStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;

    private final StreamReader keyStreamReader;
    private final StreamReader valueStreamReader;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);

    private InputStreamSource<LongInputStream> lengthStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream lengthStream;

    private Type keyType;
    private Type valueType;
    private Set<Long> longSubscripts;
    private Set<Slice> sliceSubscripts;
    private boolean mayPruneKey;
    private boolean filterIsSetup;
    private Filters.PositionalFilter positionalFilter;
    private Filter[] elementFilters;
    // For each map in the inputQualifyingSet, the number of element
    // filters that whose key exists.
    private int[] numElementFilters;
    // Count of elements at the beginning of current call to scan().
    private int initialNumElements;
    private Block keyBlock;
    // Qualifying rows after filter on key. If no filter on key, this
    // is innerQualifyingSet.
    private QualifyingSet keyQualifyingSet;
    // Maps from slice or long key to the filter to apply to the
    // value.  If this has a positional filter, these arrays are
    // indexed on the ordinal of the StructFilter that corresponds to
    // the current element. Otherwise these have one element at index
    // 0. If the key is a string and the keyBlock is a
    // DictionaryBlock, then longKeyToFilter is used with dictionary
    // ids. This will update the mappings as needed as dictionaries
    // changee between stripes and row groups.
    private Long2ObjectOpenHashMap<Filter>[] longKeyToFilter;
    private Map<Slice, Filter>[] sliceKeyToFilter;
    private VariableWidthBlock previousKeyDictionary;
    // If the map has a positional filter, this is the number of
    // element filters for each map. Null if no positional filter.
    private int[] localNumFilters;
    private int globalNumFilters;
    private boolean hasPositionalFilter;

    public MapDirectStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone, AggregatedMemoryContext systemMemoryContext)
    {
        super();
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.keyStreamReader = createStreamReader(streamDescriptor.getNestedStreams().get(0), hiveStorageTimeZone, systemMemoryContext);
        this.valueStreamReader = createStreamReader(streamDescriptor.getNestedStreams().get(1), hiveStorageTimeZone, systemMemoryContext);
    }

    @Override
    public void setReferencedSubfields(List<SubfieldPath> subfields, int depth)
    {
        mayPruneKey = true;
        boolean mayPruneElement = true;
        ImmutableList.Builder<SubfieldPath> pathsForElement = ImmutableList.builder();
        ImmutableSet.Builder<Slice> sliceSubscripts = ImmutableSet.builder();
        ImmutableSet.Builder<Long> longSubscripts = ImmutableSet.builder();
        for (SubfieldPath subfield : subfields) {
            List<PathElement> pathElements = subfield.getPathElements();
            PathElement subscript = pathElements.get(depth + 1);
            checkArgument(subscript.isSubscript(), "Map reader needs a PathElement with a subscript");
            if (subscript == allSubscripts()) {
                mayPruneKey = false;
            }
            else {
                if (subscript instanceof StringSubscript) {
                    sliceSubscripts.add(Slices.copiedBuffer(((StringSubscript) subscript).getIndex(), UTF_8));
                }
                else {
                    longSubscripts.add(((LongSubscript) subscript).getIndex());
                }
            }
            if (pathElements.size() > depth + 2) {
                pathsForElement.add(subfield);
            }
            else {
                mayPruneElement = false;
            }
        }
        if (mayPruneElement) {
            valueStreamReader.setReferencedSubfields(pathsForElement.build(), depth + 1);
        }
        if (mayPruneKey) {
            this.sliceSubscripts = sliceSubscripts.build();
            this.longSubscripts = longSubscripts.build();
        }
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public Block readBlock(Type type)
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the data reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (lengthStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                long entrySkipSize = lengthStream.sum(readOffset);
                keyStreamReader.prepareNextRead(toIntExact(entrySkipSize));
                valueStreamReader.prepareNextRead(
                        toIntExact(entrySkipSize));
            }
        }

        // We will use the offsetVector as the buffer to read the length values from lengthStream,
        // and the length values will be converted in-place to an offset vector.
        int[] offsetVector = new int[nextBatchSize + 1];
        boolean[] nullVector = null;

        if (presentStream == null) {
            if (lengthStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
            }
            lengthStream.nextIntVector(nextBatchSize, offsetVector, 0);
        }
        else {
            nullVector = new boolean[nextBatchSize];
            int nullValues = presentStream.getUnsetBits(nextBatchSize, nullVector);
            if (nullValues != nextBatchSize) {
                if (lengthStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                lengthStream.nextIntVector(nextBatchSize, offsetVector, 0, nullVector);
            }
        }

        MapType mapType = (MapType) type;
        keyType = mapType.getKeyType();
        valueType = mapType.getValueType();

        // Calculate the entryCount. Note that the values in the offsetVector are still length values now.
        int entryCount = 0;
        for (int i = 0; i < offsetVector.length - 1; i++) {
            entryCount += offsetVector[i];
        }

        Block keys;
        Block values;
        if (entryCount > 0) {
            keyStreamReader.prepareNextRead(entryCount);
            valueStreamReader.prepareNextRead(entryCount);
            keys = keyStreamReader.readBlock(keyType);
            values = valueStreamReader.readBlock(valueType);
        }
        else {
            keys = keyType.createBlockBuilder(null, 0).build();
            values = valueType.createBlockBuilder(null, 1).build();
        }

        Block[] keyValueBlock = createKeyValueBlock(nextBatchSize, keys, values, offsetVector);

        // Convert the length values in the offsetVector to offset values in-place
        int currentLength = offsetVector[0];
        offsetVector[0] = 0;
        for (int i = 1; i < offsetVector.length; i++) {
            int lastLength = offsetVector[i];
            offsetVector[i] = offsetVector[i - 1] + currentLength;
            currentLength = lastLength;
        }

        readOffset = 0;
        nextBatchSize = 0;

        return mapType.createBlockFromKeyValue(Optional.ofNullable(nullVector), offsetVector, keyValueBlock[0], keyValueBlock[1]);
    }

    boolean canPruneKeys(Block keys)
    {
        return longSubscripts != null || sliceSubscripts != null;
    }

    private boolean keyIsPruned(Block keys, int position)
    {
        return (longSubscripts != null && !longSubscripts.contains(keyType.getLong(keys, position))) ||
            (sliceSubscripts != null && !sliceSubscripts.contains(keyType.getSlice(keys, position)));
    }

    private Block[] createKeyValueBlock(int positionCount, Block keys, Block values, int[] lengths)
    {
        if (!hasNull(keys) && !canPruneKeys(keys)) {
            return new Block[] {keys, values};
        }

        //
        // Map entries with a null key are skipped in the Hive ORC reader, so skip them here also
        //

        IntArrayList nonNullPositions = new IntArrayList(keys.getPositionCount());

        int position = 0;
        for (int mapIndex = 0; mapIndex < positionCount; mapIndex++) {
            int length = lengths[mapIndex];
            for (int entryIndex = 0; entryIndex < length; entryIndex++) {
                if (keys.isNull(position) || keyIsPruned(keys, position)) {
                    // key is null, so remove this entry from the map
                    lengths[mapIndex]--;
                }
                else {
                    nonNullPositions.add(position);
                }
                position++;
            }
        }

        Block newKeys = keys.copyPositions(nonNullPositions.elements(), 0, nonNullPositions.size());
        Block newValues = values.copyPositions(nonNullPositions.elements(), 0, nonNullPositions.size());
        return new Block[] {newKeys, newValues};
    }

    private static boolean hasNull(Block keys)
    {
        for (int position = 0; position < keys.getPositionCount(); position++) {
            if (keys.isNull(position)) {
                return true;
            }
        }
        return false;
    }

    protected void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        lengthStream = lengthStreamSource.openStream();

        super.openRowGroup();
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        lengthStreamSource = missingStreamSource(LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        lengthStream = null;

        rowGroupOpen = false;

        keyStreamReader.startStripe(dictionaryStreamSources, encoding);
        valueStreamReader.startStripe(dictionaryStreamSources, encoding);
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        lengthStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, LENGTH, LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        lengthStream = null;

        rowGroupOpen = false;

        keyStreamReader.startRowGroup(dataStreamSources);
        valueStreamReader.startRowGroup(dataStreamSources);
    }

    @Override
    protected void eraseContent(int innerEnd)
    {
        keyStreamReader.erase(innerEnd);
        valueStreamReader.erase(innerEnd);
    }

    @Override
    protected void compactContent(int[] innerSurviving, int innerSurvivingDase, int numInnerSurviving)
    {
        keyStreamReader.compactValues(innerSurviving, innerSurvivingBase, numInnerSurviving);
        valueStreamReader.compactValues(innerSurviving, innerSurvivingBase, numInnerSurviving);
    }

    @Override
    public int getResultSizeInBytes()
    {
        if (outputChannel == -1) {
            return 0;
        }
        return keyStreamReader.getResultSizeInBytes() + valueStreamReader.getResultSizeInBytes();
    }

    @Override
    public int getAverageResultSize()
    {
        return (int) (1 + ((keyStreamReader.getAverageResultSize() + valueStreamReader.getAverageResultSize()) * numNestedRowsRead / (1 + numContainerRowsRead)));
    }

    @Override
    public void setResultSizeBudget(long bytes)
    {
        if (!filterIsSetup) {
            setupFilterAndChannel();
        }
        keyStreamReader.setResultSizeBudget(bytes / 2);
        valueStreamReader.setResultSizeBudget(bytes / 2);
    }

    private void setupFilterAndChannel()
    {
        if (filter == Filters.isNull()) {
            return;
        }
        List<Filter> positionFilters = Filters.getDistinctPositionFilters(filter);
        int numFilters = positionFilters.size() + 1;
        hasPositionalFilter = filter instanceof Filters.PositionalFilter;
        longKeyToFilter = new Long2ObjectOpenHashMap[numFilters];
        sliceKeyToFilter = new HashMap[numFilters];
        Filter elementFilter = null;
        for (Filter mapFilter : positionFilters) {
            if (mapFilter == Filters.isNotNull()) {
                continue;
            }
            StructFilter structFilter = (StructFilter) mapFilter;
            int ordinal = structFilter.getOrdinal();
            Map<PathElement, Filter> filters = structFilter.getFilters();
            for (Map.Entry<PathElement, Filter> entry : filters.entrySet()) {
                boolean isStringKey = entry.getKey() instanceof StringSubscript;
                String field = isStringKey ? ((StringSubscript) entry.getKey()).getIndex() : null;
                long subscript = isStringKey ? 0 : ((LongSubscript) entry.getKey()).getIndex();
                Filter valueFilter = entry.getValue();
                if (longKeyToFilter[ordinal] == null) {
                    longKeyToFilter[ordinal] = new Long2ObjectOpenHashMap();
                    if (isStringKey) {
                        sliceKeyToFilter[ordinal] = new HashMap();
                    }
                    if (positionalFilter == null) {
                        positionalFilter = new Filters.PositionalFilter(structFilter);
                        elementFilter = positionalFilter;
                    }
                }
                if (isStringKey) {
                    sliceKeyToFilter[ordinal].put(Slices.copiedBuffer(field, UTF_8), valueFilter);
                }
                else {
                    longKeyToFilter[ordinal].put(subscript, valueFilter);
                }
                globalNumFilters++;
                if (isStringKey && mayPruneKey) {
                    sliceSubscripts.add(Slices.copiedBuffer(field, UTF_8));
                }
                else if (mayPruneKey) {
                    longSubscripts.add(Long.valueOf(subscript));
                }
            }
        }
        valueStreamReader.setFilterAndChannel(elementFilter, outputChannel, -1, type.getTypeParameters().get(1));
        if (mayPruneKey) {
            List<Filter> filters = null;
            if (longSubscripts != null) {
                filters = longSubscripts.stream().map(subscript -> { return new Filters.BigintRange(subscript, subscript, false); }).collect(toList());
            }
            else if (sliceSubscripts != null) {
                filters = sliceSubscripts.stream()
                    .map(subscript -> {
                        byte[] bytes = new byte[subscript.length()];
                        subscript.getBytes(0, bytes);
                        return new Filters.BytesRange(bytes, true, bytes, true, false); })
                    .collect(toList());
            }
            Filter filter = null;
            if (filters != null && filters.size() == 1) {
                filter = filters.get(0);
            }
            else if (filters != null && filters.size() > 1) {
                filter = Filters.createMultiRange(filters, false);
            }
            keyStreamReader.setFilterAndChannel(filter, 0, -1, type.getTypeParameters().get(0));
        }
        else {
            keyStreamReader.setFilterAndChannel(null, 0, -1, type.getTypeParameters().get(0));
        }
        filterIsSetup = true;
    }

    private void setupPositionalFilter(int[] keyInputNumbers)
    {
        int numInput = inputQualifyingSet.getPositionCount();
        int numKeys = keyQualifyingSet.getPositionCount();
        if (numElementFilters == null || numElementFilters.length < numInput) {
            numElementFilters = newIntArrayForReuse(numInput);
        }
        Arrays.fill(numElementFilters, 0, numInput, 0);
        if (elementFilters == null || elementFilters.length < numKeys) {
            elementFilters = new Filter[roundupSize(numKeys)];
        }
        else {
            Arrays.fill(elementFilters, 0, numKeys, null);
        }
        if (hasPositionalFilter) {
            if (localNumFilters == null || localNumFilters.length < numInput) {
                localNumFilters = newIntArrayForReuse(numInput);
            }
            else {
                Arrays.fill(localNumFilters, 0, numInput, 0);
            }
        }
        boolean useDictionary = setupKeyIdToFilterMapping();
        boolean longKeys = keyBlock instanceof LongArrayBlock;
        int keyIndex = 0;
        for (int i = 0; i < numInput; i++) {
            Filter thisFilter = filter.nextFilter();
            if (thisFilter == null || thisFilter == Filters.isNotNull()) {
                continue;
            }
            StructFilter mapFilter = (StructFilter) thisFilter;
            int length = 0;
            int startKeyIndex = keyIndex;
            for (; keyIndex < numKeys; keyIndex++) {
                if (keyInputNumbers[keyIndex] != i) {
                    break;
                }
                length++;
            }
            int ordinal = mapFilter.getOrdinal();
            int filterCount = 0;
            int numDefinedFilters = 0;
            if (longKeys) {
                Long2ObjectOpenHashMap<Filter> keyToFilter = longKeyToFilter[ordinal];
                numDefinedFilters = keyToFilter.size();
                for (int key = startKeyIndex; key < keyIndex; key++) {
                    Filter filterAtPosition = keyToFilter.get(keyBlock.getLong(key + initialNumElements, 0));
                    if (filterAtPosition != null) {
                        filterCount++;
                        elementFilters[key] = filterAtPosition;
                    }
                }
            }
            else if (useDictionary) {
                Long2ObjectOpenHashMap<Filter> keyToFilter = longKeyToFilter[ordinal];
                numDefinedFilters = keyToFilter.size();
                DictionaryBlock keyIds = (DictionaryBlock) keyBlock;
                for (int key = startKeyIndex; key < keyIndex; key++) {
                    Filter filterAtPosition = keyToFilter.get(keyIds.getId(key + initialNumElements));
                    if (filterAtPosition != null) {
                        filterCount++;
                        elementFilters[key] = filterAtPosition;
                    }
                }
            }
            else {
                Map<Slice, Filter> keyToFilter = sliceKeyToFilter[ordinal];
                numDefinedFilters = keyToFilter.size();
                for (int key = startKeyIndex; key < keyIndex; key++) {
                    Filter filterAtPosition = keyToFilter.get(keyBlock.getSlice(key + initialNumElements, 0, keyBlock.getSliceLength(key + initialNumElements)));
                    if (filterAtPosition != null) {
                        filterCount++;
                        elementFilters[key] = filterAtPosition;
                    }
                }
            }
            numElementFilters[i] = filterCount;
            if (hasPositionalFilter) {
                localNumFilters[i] = numDefinedFilters;
            }
        }
        positionalFilter.setFilters(keyQualifyingSet, elementFilters);
    }

    private boolean setupKeyIdToFilterMapping()
    {
        if (!(keyBlock instanceof DictionaryBlock)) {
            previousKeyDictionary = null;
            return false;
        }
        DictionaryBlock keys = (DictionaryBlock) keyBlock;
        VariableWidthBlock newDictionary = (VariableWidthBlock) keys.getDictionary();
        if (previousKeyDictionary == newDictionary) {
            return true;
        }
        previousKeyDictionary = newDictionary;
        // We have a new dictionary. We need to re-establish the
        // mappings from dictionary keys to value filters.
        SliceStreamReader sliceReader = (SliceStreamReader) keyStreamReader;
        SliceDictionaryStreamReader dictionaryReader = (SliceDictionaryStreamReader) sliceReader.getCurrentReader();
        for (Filter filter : Filters.getDistinctPositionFilters(filter)) {
            if (filter == Filters.isNotNull()) {
                continue;
            }
            StructFilter structFilter = (StructFilter) filter;
            int ordinal = structFilter.getOrdinal();
            Long2ObjectOpenHashMap<Filter> map = longKeyToFilter[ordinal];
            map.clear();
            Map<PathElement, Filter> filters = structFilter.getFilters();
            for (Map.Entry<PathElement, Filter> entry : filters.entrySet()) {
                String field = ((StringSubscript) entry.getKey()).getIndex();
                int id = dictionaryReader.getIdForValue(Slices.copiedBuffer(field, UTF_8));
                map.put(id, entry.getValue());
            }
        }
        return true;
    }

    private void adjustElementLengths()
    {
        int numInput = inputQualifyingSet.getPositionCount();
        Arrays.fill(elementLength, 0, numInput, 0);
        int numKeys = keyQualifyingSet.getPositionCount();
        int[] inputNumbers = innerQualifyingSet.getInputNumbers();
        int[] keyInputNumbers = keyQualifyingSet.getInputNumbers();
        for (int i = 0; i < numKeys; i++) {
            keyInputNumbers[i] = inputNumbers[keyInputNumbers[i]];
            elementLength[keyInputNumbers[i]]++;
        }
    }

    @Override
    public void scan()
            throws IOException
    {
        if (!filterIsSetup) {
            setupFilterAndChannel();
        }
        if (!rowGroupOpen) {
            openRowGroup();
        }
        beginScan(presentStream, lengthStream);
        initialNumElements = valueStreamReader.getNumValues();
        int numInput = inputQualifyingSet.getPositionCount();
        int lastElementOffset = numValues == 0 ? 0 : elementOffset[numValues];
        makeInnerQualifyingSet();
        ensureValuesCapacity(inputQualifyingSet.getPositionCount());
        if (innerQualifyingSet.getPositionCount() > 0) {
            keyStreamReader.setInputQualifyingSet(innerQualifyingSet);
            keyStreamReader.scan();
            keyBlock = keyStreamReader.getBlock(keyStreamReader.getNumValues(), true);
            if (positionalFilter != null) {
                // The position in inputQualifyingSet for each value in
                // keyBlock. Positions with the same value in this array
                // belong to the same map.
                int[] keyInputNumbers;
                if (keyStreamReader.getFilter() != null) {
                    keyQualifyingSet = keyStreamReader.getOutputQualifyingSet();
                    int numKeys = keyQualifyingSet.getPositionCount();
                    int[] inputNumbers = innerQualifyingSet.getInputNumbers();
                    keyInputNumbers = keyQualifyingSet.getInputNumbers();
                    for (int i = 0; i < numKeys; i++) {
                        keyInputNumbers[i] = inputNumbers[keyInputNumbers[i]];
                    }
                }
                else {
                    keyQualifyingSet = innerQualifyingSet;
                    keyInputNumbers = innerQualifyingSet.getInputNumbers();
                }
                setupPositionalFilter(keyInputNumbers);
            }
            else {
                if (keyStreamReader.getFilter() == null) {
                    keyQualifyingSet = innerQualifyingSet;
                }
                else {
                    keyQualifyingSet = keyStreamReader.getOutputQualifyingSet();
                    adjustElementLengths();
                }
            }
            if (keyQualifyingSet.getPositionCount() > 0) {
                if (keyQualifyingSet != innerQualifyingSet) {
                    keyQualifyingSet.setParent(inputQualifyingSet);
                }
                valueStreamReader.setInputQualifyingSet(keyQualifyingSet);
                valueStreamReader.scan();
                innerPosInRowGroup = innerQualifyingSet.getEnd();
            }
            else {
                valueStreamReader.getOrCreateOutputQualifyingSet().reset(0);
            }
            if (filter != null) {
                QualifyingSet filterResult = valueStreamReader.getOutputQualifyingSet();
                outputQualifyingSet.reset(inputQualifyingSet.getPositionCount());
                int numValueResults = filterResult.getPositionCount();
                int[] resultInputNumbers = filterResult.getInputNumbers();
                int[] resultRows = filterResult.getPositions();
                if (innerSurviving == null || innerSurviving.length < numValueResults) {
                    innerSurviving = newIntArrayForReuse(numValueResults);
                }
                numInnerSurviving = 0;
                int outputIndex = 0;
                numInnerResults = 0;
                for (int i = 0; i < numInput; i++) {
                    outputIndex = processFilterHits(i, outputIndex, resultRows, resultInputNumbers, numValueResults);
                }
                valueStreamReader.compactValues(innerSurviving, initialNumElements, numInnerSurviving);
                if (filter != null) {
                    for (int i = 0; i < numInnerSurviving; i++) {
                        innerSurviving[i] = resultInputNumbers[innerSurviving[i]];
                    }
                }
                keyStreamReader.compactValues(innerSurviving, initialNumElements, numInnerSurviving);
                numContainerRowsRead += numInnerResults;
            }
            else {
                numInnerResults = inputQualifyingSet.getPositionCount() - numNullsToAdd;
            }
        }
        addNullsAfterScan(filter != null ? outputQualifyingSet : inputQualifyingSet, inputQualifyingSet.getEnd());
        if (filter == null) {
            // The lengths are unchanged by reading the values.
            int valueIndex = numValues;
            for (int i = 0; i < numInput; i++) {
                elementOffset[valueIndex] = lastElementOffset;
                lastElementOffset += elementLength[i];
                valueIndex++;
            }
            elementOffset[valueIndex] = lastElementOffset;
        }
        else {
            if (numNullsToAdd > 0 && outputChannel != -1) {
                // There was a filter and nulls were added by
                // addNullsAfterScan(). Fill null positions in
                // elementOffset with the offset of the next non-null.
                elementOffset[numValues + numResults] = lastElementOffset;
                int nextNonNull = lastElementOffset;
                for (int i = numValues + numResults - 1; i >= numValues; i--) {
                    if (elementOffset[i] == -1) {
                        elementOffset[i] = nextNonNull;
                    }
                    else {
                        nextNonNull = elementOffset[i];
                    }
                }
            }
        }
        endScan(presentStream);
    }

    // Counts how many hits one map has. Adds the map to surviving
    // if all hit and all filters existed. Adds map to errors if all
    // hit but not all subscripts existed. Else the map did not
    // pass. Returns the index to the first element of the next value
    // in the keyQualifyingSet.
    private int processFilterHits(int inputIndex, int outputIndex, int[] resultRows, int[] resultInputNumbers, int numValueResults)
    {
        int filterHits = 0;
        int count = 0;
        int initialOutputIndex = outputIndex;
        if (presentStream != null && !present[inputQualifyingSet.getPositions()[inputIndex] - posInRowGroup]) {
            return outputIndex;
        }
        int[] inputNumbers = keyQualifyingSet.getInputNumbers();
        // Count rows and filter hits from the map corresponding to inputIndex.
        while (outputIndex < numValueResults && inputNumbers[resultInputNumbers[outputIndex]] == inputIndex) {
            count++;
            if (elementFilters[resultInputNumbers[outputIndex]] != null) {
                filterHits++;
            }
            outputIndex++;
        }
        if (filterHits < numElementFilters[inputIndex]) {
            // Some filter did not hit.
            return outputIndex;
        }
        outputQualifyingSet.append(inputQualifyingSet.getPositions()[inputIndex], inputIndex);
        addMapToResult(inputIndex, initialOutputIndex, outputIndex);
        int numDefinedFilters = localNumFilters != null ? localNumFilters[inputIndex] : globalNumFilters;
        if (numElementFilters[inputIndex] < numDefinedFilters) {
            ErrorSet errorSet = outputQualifyingSet.getOrCreateErrorSet();
            errorSet.addError(outputQualifyingSet.getPositionCount() - 1, inputQualifyingSet.getPositionCount(), new MissingSubscriptException());
        }
        return outputIndex;
    }

    private void addMapToResult(int inputIndex, int beginResult, int endResult)
    {
        if (!outputChannelSet) {
            return;
        }
        elementOffset[numValues + numInnerResults] = numInnerSurviving + initialNumElements;
        for (int i = beginResult; i < endResult; i++) {
            innerSurviving[numInnerSurviving++] = i;
        }
        elementOffset[numValues + numInnerResults + 1] = numInnerSurviving + initialNumElements;
        numInnerResults++;
        numNestedRowsRead += endResult - beginResult;
    }

    @Override
    public Block getBlock(int numFirstRows, boolean mayReuse)
    {
        int innerFirstRows = getInnerPosition(numFirstRows);
        // offset is always new since createBlockFromKeyValue does not
        // take a length but uses offset.length.
        int[] offsets = Arrays.copyOf(elementOffset, numFirstRows + 1);
        boolean[] nulls = valueIsNull == null ? null
            : mayReuse ? valueIsNull : Arrays.copyOf(valueIsNull, numFirstRows);
        Block keys;
        Block values;
        if (innerFirstRows == 0) {
            Type keyType = type.getTypeParameters().get(0);
            keys = keyType.createBlockBuilder(null, 0).build();
            Type valueType = type.getTypeParameters().get(0);
            values = valueType.createBlockBuilder(null, 0).build();
        }
        else {
            keys = keyStreamReader.getBlock(innerFirstRows, mayReuse);
            values = valueStreamReader.getBlock(innerFirstRows, mayReuse);
        }
        MapType mapType = (MapType) type;
        return mapType.createBlockFromKeyValue(Optional.ofNullable(nulls), offsets, keys, values);
    }

    @Override
    protected void shiftUp(int from, int to)
    {
        elementOffset[to] = elementOffset[from];
    }

    @Override
    protected void writeNull(int position)
    {
        elementOffset[position] = -1;
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
        try (Closer closer = Closer.create()) {
            closer.register(keyStreamReader::close);
            closer.register(valueStreamReader::close);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + keyStreamReader.getRetainedSizeInBytes() + valueStreamReader.getRetainedSizeInBytes();
    }

    @Override
    public boolean mustExtractValuesBeforeScan(boolean isNewStripe)
    {
        return keyStreamReader.mustExtractValuesBeforeScan(isNewStripe) || valueStreamReader.mustExtractValuesBeforeScan(isNewStripe);
    }
}
