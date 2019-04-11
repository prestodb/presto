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
import com.facebook.presto.spi.SubfieldPath.AllSubscripts;
import com.facebook.presto.spi.SubfieldPath.LongSubscript;
import com.facebook.presto.spi.SubfieldPath.PathElement;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.orc.ResizedArrays.newIntArrayForReuse;
import static com.facebook.presto.orc.ResizedArrays.roundupSize;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.StreamReaders.createStreamReader;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ListStreamReader
        extends RepeatedColumnReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ListStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;

    private final StreamReader elementStreamReader;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<LongInputStream> lengthStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream lengthStream;

    private Filters.PositionalFilter positionalFilter;
    private boolean filterIsSetup;
    private Filter[] elementFilters;
    private int filterOffset;
    // For each array in the inputQualifyingSet, the number of element filters that fit.
    private int[] numElementFilters;
    // For each array, the number of filters intended for it. This is
    // used only if the filter on the array stream is positional,
    // i.e. different arrays have different filters.
    private int[] localNumFilters;
    // Number of filters on the arrays if all arrays have the same filters.
    private int globalNumFilters;
    private boolean hasPositionalFilter;
    // Count of elements at the beginning of current call to scan().
    private int initialNumElements;
    private Long2ObjectOpenHashMap<Filter>[] subscriptToFilter;

    public ListStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone, AggregatedMemoryContext systemMemoryContext)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.elementStreamReader = createStreamReader(streamDescriptor.getNestedStreams().get(0), hiveStorageTimeZone, systemMemoryContext);
    }

    @Override
    public void setReferencedSubfields(List<SubfieldPath> subfields, int depth)
    {
        ImmutableSet.Builder<Integer> referencedSubscripts = ImmutableSet.builder();
        ImmutableList.Builder<SubfieldPath> pathsForElement = ImmutableList.builder();
        boolean mayPruneElement = true;
        for (SubfieldPath subfield : subfields) {
            List<PathElement> pathElements = subfield.getPathElements();
            PathElement subscript = pathElements.get(depth + 1);
            checkArgument(subscript instanceof LongSubscript, "List reader needs a PathElement with a subscript");
            if (pathElements.size() > depth + 2) {
                pathsForElement.add(subfield);
            }
            else {
                mayPruneElement = false;
            }
        }
        if (mayPruneElement) {
            elementStreamReader.setReferencedSubfields(pathsForElement.build(), depth + 1);
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
                long elementSkipSize = lengthStream.sum(readOffset);
                elementStreamReader.prepareNextRead(toIntExact(elementSkipSize));
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

        // Convert the length values in the offsetVector to offset values in place
        int currentLength = offsetVector[0];
        offsetVector[0] = 0;
        for (int i = 1; i < offsetVector.length; i++) {
            int nextLength = offsetVector[i];
            offsetVector[i] = offsetVector[i - 1] + currentLength;
            currentLength = nextLength;
        }

        Type elementType = type.getTypeParameters().get(0);
        int elementCount = offsetVector[offsetVector.length - 1];

        Block elements;
        if (elementCount > 0) {
            elementStreamReader.prepareNextRead(elementCount);
            elements = elementStreamReader.readBlock(elementType);
        }
        else {
            elements = elementType.createBlockBuilder(null, 0).build();
        }
        Block arrayBlock = ArrayBlock.fromElementBlock(nextBatchSize, Optional.ofNullable(nullVector), offsetVector, elements);

        readOffset = 0;
        nextBatchSize = 0;

        return arrayBlock;
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

        elementStreamReader.startStripe(dictionaryStreamSources, encoding);
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

        elementStreamReader.startRowGroup(dataStreamSources);
    }

    @Override
    protected void eraseContent(int innerEnd)
    {
        elementStreamReader.erase(innerEnd);
    }

    @Override
    protected void compactContent(int[] innerSurviving, int innerSurvivingDase, int numInnerSurviving)
    {
        elementStreamReader.compactValues(innerSurviving, innerSurvivingBase, numInnerSurviving);
    }

    @Override
    public int getResultSizeInBytes()
    {
        if (outputChannel == -1) {
            return 0;
        }
        return elementStreamReader.getResultSizeInBytes();
    }

    @Override
    public int getAverageResultSize()
    {
        return (int) (1 + (elementStreamReader.getAverageResultSize() * numNestedRowsRead / (1 + numContainerRowsRead)));
    }

    @Override
    public void setResultSizeBudget(long bytes)
    {
        if (!filterIsSetup) {
            setupFilterAndChannel();
        }
        elementStreamReader.setResultSizeBudget(bytes);
    }

    private void setupFilterAndChannel()
    {
        if (filter == null) {
            elementStreamReader.setFilterAndChannel(null, outputChannel, -1, type.getTypeParameters().get(0));
            filterIsSetup = true;
            return;
        }
        Filter elementFilter = null;
        if (filter == Filters.isNull()) {
            filterIsSetup = true;
            return;
        }
        hasPositionalFilter = filter instanceof Filters.PositionalFilter;
        List<Filter> positionFilters = Filters.getDistinctPositionFilters(filter);
        int numFilters = positionFilters.size() + 1;
        subscriptToFilter = new Long2ObjectOpenHashMap[numFilters];
        setupFilters:
        for (Filter positionFilter : positionFilters) {
            if (positionFilter == Filters.isNotNull()) {
                continue;
            }
            verify(positionFilter instanceof StructFilter);
            Filters.StructFilter listFilter = (Filters.StructFilter) positionFilter;
            int ordinal = listFilter.getOrdinal();
            Map<PathElement, Filter> filters = listFilter.getFilters();
            for (Map.Entry<PathElement, Filter> entry : filters.entrySet()) {
                if (entry.getKey() instanceof AllSubscripts) {
                    verify(filters.size() == 1, "Only one elementFilter is allowed if no subscript is given");
                    elementFilter = entry.getValue();
                    break setupFilters;
                }

                verify(entry.getKey() instanceof LongSubscript);
                long subscript = ((LongSubscript) entry.getKey()).getIndex();
                if (subscriptToFilter[ordinal] == null) {
                    subscriptToFilter[ordinal] = new Long2ObjectOpenHashMap();
                    if (positionalFilter == null) {
                        positionalFilter = new Filters.PositionalFilter(listFilter);
                        elementFilter = positionalFilter;
                    }
                }
                subscriptToFilter[ordinal].put(subscript - 1, entry.getValue());
            }
        }
        if (!hasPositionalFilter) {
            globalNumFilters = subscriptToFilter[0].size();
        }
        elementStreamReader.setFilterAndChannel(elementFilter, outputChannel, -1, type.getTypeParameters().get(0));
        filterIsSetup = true;
    }

    private void setupPositionalFilter()
    {
        int numInput = inputQualifyingSet.getPositionCount();
        if (numElementFilters == null || numElementFilters.length < numInput) {
            numElementFilters = newIntArrayForReuse(numInput);
        }
        Arrays.fill(numElementFilters, 0, numInput, 0);
        int numInner = innerQualifyingSet.getPositionCount();
        if (elementFilters == null || elementFilters.length < numInner) {
            elementFilters = new Filter[roundupSize(numInner)];
        }
        else {
            Arrays.fill(elementFilters, 0, numInner, null);
        }
        if (hasPositionalFilter) {
            if (localNumFilters == null || localNumFilters.length < numInput) {
                localNumFilters = newIntArrayForReuse(numInput);
            }
            else {
                Arrays.fill(localNumFilters, 0, numInput, 0);
            }
        }
        int innerStart = 0;
        for (int i = 0; i < numInput; i++) {
            int length = elementLength[i];
            int filterCount = 0;
            Filter arrayFilter = filter.nextFilter();
            if (filter == Filters.isNotNull()) {
                continue;
            }
            int ordinal = ((StructFilter) arrayFilter).getOrdinal();
            for (Map.Entry<Long, Filter> entry : subscriptToFilter[ordinal].entrySet()) {
                int subscript = toIntExact(entry.getKey().longValue());
                if (subscript < length) {
                    elementFilters[innerStart + subscript] = entry.getValue();
                    filterCount++;
                }
                numElementFilters[i] = filterCount;
                if (hasPositionalFilter) {
                    localNumFilters[i] = subscriptToFilter[ordinal].size();
                }
            }
            innerStart += length;
        }
        positionalFilter.setFilters(innerQualifyingSet, elementFilters);
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
        initialNumElements = elementStreamReader.getNumValues();
        makeInnerQualifyingSet();
        if (positionalFilter != null) {
            setupPositionalFilter();
        }
        if (innerQualifyingSet.getPositionCount() > 0) {
            elementStreamReader.setInputQualifyingSet(innerQualifyingSet);
            elementStreamReader.scan();
            innerPosInRowGroup = innerQualifyingSet.getEnd();
        }
        else {
            elementStreamReader.getOrCreateOutputQualifyingSet().reset(0);
        }
        ensureValuesCapacity(inputQualifyingSet.getPositionCount());
        int lastElementOffset = numValues == 0 ? 0 : elementOffset[numValues];
        int numInput = inputQualifyingSet.getPositionCount();
        if (filter != null) {
            QualifyingSet filterResult = elementStreamReader.getOutputQualifyingSet();
            outputQualifyingSet.reset(inputQualifyingSet.getPositionCount());
            int numElementResults = filterResult.getPositionCount();
            int[] resultInputNumbers = filterResult.getInputNumbers();
            int[] resultRows = filterResult.getPositions();
            if (innerSurviving == null || innerSurviving.length < numElementResults) {
                innerSurviving = newIntArrayForReuse(numElementResults);
            }
            numInnerSurviving = 0;
            int outputIndex = 0;
            numInnerResults = 0;
            filterOffset = 0;
            for (int i = 0; i < numInput; i++) {
                outputIndex = processFilterHits(i, outputIndex, resultRows, resultInputNumbers, numElementResults);
            }
            elementStreamReader.compactValues(innerSurviving, initialNumElements, numInnerSurviving);
            numContainerRowsRead += numInnerResults;
        }
        else {
            numInnerResults = inputQualifyingSet.getPositionCount() - numNullsToAdd;
        }
        addNullsAfterScan(filter != null ? outputQualifyingSet : inputQualifyingSet, inputQualifyingSet.getEnd());
        if (filter == null) {
            // The lengths are unchanged.
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

    // Counts how many hits one array has. Adds the array to surviving
    // if all hit and all filters existed. Adds array to errors if all
    // hit but not all subscripts existed. Else the array did not
    // pass. Returns the index to the first element of the next array
    // in the inner outputQualifyingSet.
    private int processFilterHits(int inputIndex, int outputIndex, int[] resultRows, int[] resultInputNumbers, int numElementResults)
    {
        int filterHits = 0;
        int initialOutputIndex = outputIndex;
        if (presentStream != null && !present[inputQualifyingSet.getPositions()[inputIndex] - posInRowGroup]) {
            return outputIndex;
        }
        int[] inputNumbers = innerQualifyingSet.getInputNumbers();
        // Count rows and filter hits from the array corresponding to inputIndex.
        int startOfArray = elementStart[inputIndex];
        while (outputIndex < numElementResults && inputNumbers[resultInputNumbers[outputIndex]] == inputIndex) {
            int posInArray = resultRows[outputIndex] - startOfArray;
            if (elementFilters[filterOffset + posInArray] != null) {
                filterHits++;
            }
            outputIndex++;
        }
        filterOffset += elementLength[inputIndex];
        if (filterHits < numElementFilters[inputIndex]) {
            // Some filter did not hit.
            return outputIndex;
        }
        outputQualifyingSet.append(inputQualifyingSet.getPositions()[inputIndex], inputIndex);
        addArrayToResult(inputIndex, initialOutputIndex, outputIndex);
        int total = localNumFilters != null ? localNumFilters[inputIndex] : globalNumFilters;
        if (numElementFilters[inputIndex] < total) {
            ErrorSet errorSet = outputQualifyingSet.getOrCreateErrorSet();
            errorSet.addError(outputQualifyingSet.getPositionCount() - 1, inputQualifyingSet.getPositionCount(), new MissingSubscriptException());
        }
        return outputIndex;
    }

    private void addArrayToResult(int inputIndex, int beginResult, int endResult)
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
        int[] offsets = mayReuse ? elementOffset : Arrays.copyOf(elementOffset, numFirstRows + 1);
        boolean[] nulls = valueIsNull == null ? null
            : mayReuse ? valueIsNull : Arrays.copyOf(valueIsNull, numFirstRows);
        Block elements;
        if (innerFirstRows == 0) {
            Type elementType = type.getTypeParameters().get(0);
            elements = elementType.createBlockBuilder(null, 0).build();
        }
        else {
            elements = elementStreamReader.getBlock(innerFirstRows, mayReuse);
        }
        return ArrayBlock.fromElementBlock(numFirstRows, Optional.ofNullable(nulls), offsets, elements);
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
            closer.register(() -> elementStreamReader.close());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + elementStreamReader.getRetainedSizeInBytes();
    }

    @Override
    public boolean mustExtractValuesBeforeScan(boolean isNewStripe)
    {
        return elementStreamReader.mustExtractValuesBeforeScan(isNewStripe);
    }
}
