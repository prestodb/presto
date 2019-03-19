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
package com.facebook.presto.orc;

import com.facebook.presto.orc.reader.StreamReader;
import com.facebook.presto.spi.AriaFlags;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageSourceOptions.FilterFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.type.Type;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ColumnGroupReader
{
    // Number of complete rows in result Blocks in StreamReaders.
    private int numRowsInResult;

    private StreamReader[] sortedStreamReaders;
    // Corresponds pairwise to sortedStreamReaders. A FilterFunction is at
    // index i where i is the index of its last operand in
    // sortedStreamReaders.
    private FilterFunction[][] filterFunctionOrder;
    private QualifyingSet inputQualifyingSet;
    private QualifyingSet outputQualifyingSet;

    private final int ariaFlags;

    private boolean reorderFilters;
    private Block[] reusedPageBlocks;

    private int lastTruncatedStreamIdx = -1;
    private int maxOutputChannel = -1;
    private int targetResultBytes;
    // The number of leading elements in sortedStreamReaders that is subject to reordering.
    private int numFilters;
    private int firstNonFilter;
    // Temporary array for compacting results that have been decimated
    // by subsequent filters.
    private int[] survivingRows;
    private int[] readerBudget;
    private FilterFunction[] filterFunctions;
    private int[] filterResults;
    private Map<Integer, StreamReader> channelToStreamReader;
    private int[] outputChannels;

    public ColumnGroupReader(
            StreamReader[] streamReaders,
            Set<Integer> presentColumns,
            int[] channelColumns,
            List<Type> types,
            int[] internalChannels,
            int[] outputChannels,
            Map<Integer, Filter> filters,
            FilterFunction[] filterFunctions,
            boolean reorderFilters,
            int ariaFlags)
    {
        this.reorderFilters = reorderFilters;
        this.ariaFlags = ariaFlags;
        this.outputChannels = requireNonNull(outputChannels, "outputChannels is null");
        channelToStreamReader = new HashMap();
        for (int i = 0; i < channelColumns.length; i++) {
            int columnIndex = channelColumns[i];
            if (presentColumns != null && !presentColumns.contains(columnIndex)) {
                continue;
            }
            int internalChannel = i < internalChannels.length ? internalChannels[i] : -1;
            Filter filter = filters.get(columnIndex);
            if (internalChannel == -1 && filter == null) {
                if (streamReaders[columnIndex] != null) {
                    throw new IllegalArgumentException("There should be no StreamReader if there is no filter and the value of the column is unreferenced");
                }
                continue;
            }
            StreamReader streamReader = streamReaders[columnIndex];
            streamReader.setFilterAndChannel(filter, internalChannel, columnIndex, types.get(i));
            if (internalChannel != -1) {
                channelToStreamReader.put(internalChannel, streamReader);
            }
        }
        this.filterFunctions = requireNonNull(filterFunctions, "filterFunctions is null");
        for (int i = 0; i < outputChannels.length; i++) {
            maxOutputChannel = Math.max(maxOutputChannel, outputChannels[i]);
        }
        setupStreamOrder(streamReaders);
    }

    // Sets the rows active in the row group. Fully processed rows are
    // removed from this by advance().
    public void setQualifyingSets(QualifyingSet input, QualifyingSet output)
    {
        inputQualifyingSet = input;
        outputQualifyingSet = output;
    }

    public void setResultSizeBudget(long bytes)
    {
        targetResultBytes = (int) bytes;
    }

    private static int compareReaders(StreamReader a, StreamReader b)
    {
        // A stream with filter goes before one without.
        Filter aFilter = a.getFilter();
        Filter bFilter = b.getFilter();
        if (aFilter != null && bFilter == null) {
            return -1;
        }
        if (aFilter == null && bFilter != null) {
            return 1;
        }
        if (aFilter != null) {
            double aScore = aFilter.getTimePerDroppedValue();
            double bScore = bFilter.getTimePerDroppedValue();
            if (aScore != bScore) {
                return aScore < bScore ? -1 : 1;
            }
            // If the score is a draw, e.g., at start of scan, the shorter data type/stricter comparison goes first.
            return aFilter.staticScore() - bFilter.staticScore();
        }
        // Streams that have no filters go longer data type first. This hits maximum batch size sooner.
        return b.getAverageResultSize() - a.getAverageResultSize();
    }

    private void setupStreamOrder(StreamReader[] streamReaders)
    {
        int numReaders = 0;
        for (StreamReader reader : streamReaders) {
            if (reader != null) {
                numReaders++;
                if (reader.getFilter() != null) {
                    numFilters++;
                }
            }
        }
        sortedStreamReaders = new StreamReader[numReaders];
        int fill = 0;
        for (StreamReader reader : streamReaders) {
            if (reader != null) {
                sortedStreamReaders[fill++] = reader;
            }
        }
        Arrays.sort(sortedStreamReaders, (StreamReader a, StreamReader b) -> compareReaders(a, b));
        if (numFilters + filterFunctions.length < 2) {
            reorderFilters = false;
        }
        setupFilterFunctions();
        readerBudget = new int[sortedStreamReaders.length];
    }

    private void installFilterFunction(int idx, FilterFunction function)
    {
        if (idx >= filterFunctionOrder.length) {
            filterFunctionOrder = Arrays.copyOf(filterFunctionOrder, idx + 1);
        }
        if (filterFunctionOrder[idx] == null) {
            filterFunctionOrder[idx] = new FilterFunction[1];
        }
        else {
            FilterFunction[] list = filterFunctionOrder[idx];
            filterFunctionOrder[idx] = Arrays.copyOf(list, list.length + 1);
        }
        filterFunctionOrder[idx][filterFunctionOrder[idx].length - 1] = function;
    }

    private static int compareFilters(FilterFunction a, FilterFunction b)
    {
        if (a.getTimePerDroppedValue() < b.getTimePerDroppedValue()) {
            return -1;
        }
        if (a.getTimePerDroppedValue() > b.getTimePerDroppedValue()) {
            return 1;
        }
        return 0;
    }

    // Sorts the functions most efficient first. Places the functions
    // in filterFunctionOrder most efficient first. If the function
    // depends on non-filter columns, moves the columns right after
    // the filter columns. the best function and its arguments will
    // come before the next best.
    private void setupFilterFunctions()
    {
        filterFunctionOrder = new FilterFunction[0][];
        if (filterFunctions.length == 0) {
            return;
        }
        Arrays.sort(filterFunctions, (FilterFunction a, FilterFunction b) -> compareFilters(a, b));
        firstNonFilter = numFilters;
        for (FilterFunction filter : filterFunctions) {
            placeFunctionAndOperands(filter);
        }
    }

    private void placeFunctionAndOperands(FilterFunction function)
    {
        int functionIdx = 0;
        int[] channels = function.getInputChannels();
        for (int channel : channels) {
            int idx = findChannelIdx(channel);
            if (idx > firstNonFilter) {
                // Move this to position firstNonFilter.
                StreamReader temp = sortedStreamReaders[idx];
                System.arraycopy(sortedStreamReaders, firstNonFilter, sortedStreamReaders, firstNonFilter + 1, idx - firstNonFilter);
                sortedStreamReaders[firstNonFilter] = temp;
            }
            if (idx >= firstNonFilter) {
                idx = firstNonFilter++;
            }
            functionIdx = Math.max(functionIdx, idx);
        }
        installFilterFunction(functionIdx, function);
    }

    private int findChannelIdx(int channel)
    {
        for (int i = 0; i < sortedStreamReaders.length; i++) {
            if (sortedStreamReaders[i].getChannel() == channel) {
                return i;
            }
        }
        throw new IllegalArgumentException(format("Missing stream reader for channel: %s", channel));
    }

    public void maybeReorderFilters()
    {
        if (!reorderFilters) {
            return;
        }

        double time = 0;
        boolean reorder = false;
        boolean reorderFunctions = false;
        for (int i = 0; i < numFilters; i++) {
            Filter filter = sortedStreamReaders[i].getFilter();
            double filterTime = filter.getTimePerDroppedValue();
            if (filterTime < time) {
                reorder = true;
            }
            time = filterTime;
            filter.decayStats();
            sortedStreamReaders[i].maybeReorderFilters();
        }
        time = 0;
        for (int i = 0; i < filterFunctions.length; i++) {
            FilterFunction filter = filterFunctions[i];
            double filterTime = filter.getTimePerDroppedValue();
            if (filterTime < time) {
                reorderFunctions = true;
            }
            time = filterTime;
            filter.decayStats();
        }

        if (!reorder && !reorderFunctions) {
            return;
        }
        Arrays.sort(sortedStreamReaders, 0, numFilters, (StreamReader a, StreamReader b) -> compareReaders(a, b));
        setupFilterFunctions();
    }

    // Divides the space available in the result Page between the
    // streams at firstStreamIdx and to the right of at.
    boolean makeResultBudget(int firstStreamIdx, int numRows, boolean mayReturn)
    {
        int bytesSoFar = 0;
        double selectivity = 1;
        int totalAsk = 0;
        if ((ariaFlags & AriaFlags.noReaderBudget) != 0) {
            for (int i = firstStreamIdx; i < sortedStreamReaders.length; i++) {
                StreamReader reader = sortedStreamReaders[i];
                if (reader.getChannel() != -1) {
                    // Arbitrarily large but no Long overflow
                    reader.setResultSizeBudget(0x7fffffff);
                }
            }
            return false;
        }
        for (int i = 0; i < sortedStreamReaders.length; i++) {
            StreamReader reader = sortedStreamReaders[i];
            bytesSoFar += reader.getResultSizeInBytes();
            if (i >= firstStreamIdx) {
                Filter filter = reader.getFilter();
                int channel = reader.getChannel();
                if (filter != null) {
                    selectivity *= filter.getSelectivity();
                }
                if (channel != -1) {
                    int avgSize = reader.getAverageResultSize();
                    readerBudget[i] = Math.max(8000, (int) (numRows * selectivity * avgSize));
                    totalAsk += readerBudget[i];
                    // A filter function can only be at the position
                    // of a reader with a channel since they are
                    // placed at the latest position that reads an
                    // input.
                    if (i < filterFunctionOrder.length && filterFunctionOrder[i] != null) {
                        for (FilterFunction function : filterFunctionOrder[i]) {
                            selectivity *= function.getSelectivity();
                        }
                    }
                }
                else {
                    readerBudget[i] = 0;
                }
            }
        }
        if (totalAsk == 0) {
            return false;
        }
        int available = targetResultBytes - bytesSoFar;
        if (available < targetResultBytes / 10 && mayReturn) {
            return true;
        }
        if (available < 10000) {
            available = 10000;
        }
        double grantedFraction = (double) available / totalAsk;
        for (int i = firstStreamIdx; i < sortedStreamReaders.length; i++) {
            StreamReader reader = sortedStreamReaders[i];
            if (reader.getChannel() != -1) {
                int budget = (int) (readerBudget[i] * grantedFraction);
                reader.setResultSizeBudget(budget);
            }
        }
        return false;
    }

    public Block[] getBlocks(int numFirstRows, boolean reuseBlocks, boolean fillAbsentWithNulls)
    {
        if (numFirstRows == 0) {
            return null;
        }
        Block[] blocks;
        if (reuseBlocks) {
            blocks = reusedPageBlocks;
            if (blocks == null) {
                blocks = new Block[maxOutputChannel + 1];
                reusedPageBlocks = blocks;
            }
        }
        else {
            blocks = new Block[maxOutputChannel + 1];
        }
        for (int i = 0; i < outputChannels.length; i++) {
            // The ith entry of internalChannels goes to outputChannels[i]th place in the result Page.
            int channel = outputChannels[i];
            if (channel == -1) {
                continue;
            }
            StreamReader reader = channelToStreamReader.get(i);
            if (reader != null) {
                blocks[channel] = reader.getBlock(numFirstRows, reuseBlocks);
            }
        }
        return blocks;
    }

    // Removes the first numValues values from all reader and
    // QualifyingSets. If the last batch was truncated, some of the
    // readers may hold values from last batch.
    public void newBatch(int numValues)
    {
        numRowsInResult -= numValues;
        if (numRowsInResult < 0) {
            throw new IllegalArgumentException("Cannot erase more rows than there are");
        }
        for (int i = sortedStreamReaders.length - 1; i >= 0; i--) {
            sortedStreamReaders[i].erase(numValues);
        }
    }

    public boolean hasUnfetchedRows()
    {
        return findLastTruncatedStreamIdx() != -1;
    }

    public void advance()
            throws IOException
    {
        int firstStreamIdx;
        lastTruncatedStreamIdx = findLastTruncatedStreamIdx();
        QualifyingSet qualifyingSet;
        if (lastTruncatedStreamIdx == -1) {
            firstStreamIdx = 0;
            makeResultBudget(0, inputQualifyingSet.getPositionCount(), false);
            qualifyingSet = inputQualifyingSet;
        }
        else {
            firstStreamIdx = lastTruncatedStreamIdx;
            StreamReader reader = sortedStreamReaders[firstStreamIdx];
            // The last call to compactSparseBlocks() has removed
            // returned rows from the set. If there is a truncation to
            // the left of this truncation, the end of the
            // qualifyingSet that we resume is set to reflect that so
            // we do not read past a truncate of a previous column.
            qualifyingSet = reader.getInputQualifyingSet();
            setNewTruncation(firstStreamIdx, qualifyingSet);
            makeResultBudget(firstStreamIdx, qualifyingSet.getPositionCount(), false);
        }
        int numStreams = sortedStreamReaders.length;
        for (int streamIdx = firstStreamIdx; streamIdx < numStreams; ++streamIdx) {
            long startTime = 0;
            StreamReader reader = sortedStreamReaders[streamIdx];
            Filter filter = reader.getFilter();
            // Link this qualifying set to the input of this reader so
            // that for nested structs we know what rows correspond to
            // top level row boundaries.
            if (qualifyingSet != inputQualifyingSet) {
                qualifyingSet.setFirstOfLevel(inputQualifyingSet);
            }
            reader.setInputQualifyingSet(qualifyingSet);
            if (!hasFilter(streamIdx)) {
                reader.setOutputQualifyingSet(null);
            }
            if (reorderFilters && filter != null) {
                startTime = System.nanoTime();
            }
            reader.scan();
            if (filter != null) {
                QualifyingSet input = qualifyingSet;
                qualifyingSet = reader.getOutputQualifyingSet();
                filter.updateStats(input.getPositionCount(), qualifyingSet.getPositionCount(), reorderFilters ? System.nanoTime() - startTime : 100);
                if (qualifyingSet.isEmpty()) {
                    alignResultsAndRemoveFromQualifyingSet(0, streamIdx);
                    return;
                }
            }
            if (filterFunctionOrder != null && streamIdx < filterFunctionOrder.length && filterFunctionOrder[streamIdx] != null) {
                qualifyingSet = evaluateFilterFunction(streamIdx, qualifyingSet);
                if (qualifyingSet.isEmpty()) {
                    alignResultsAndRemoveFromQualifyingSet(0, streamIdx);
                    return;
                }
            }
        }

        // Number of rows surviving all truncations/filters.
        int numAdded = qualifyingSet.getPositionCount();
        if (sortedStreamReaders.length > 0) {
            alignResultsAndRemoveFromQualifyingSet(numAdded, sortedStreamReaders.length - 1);
        }
        else {
            inputQualifyingSet.eraseBelowRow(inputQualifyingSet.getEnd());
        }
        numRowsInResult += numAdded;
    }

    private QualifyingSet evaluateFilterFunction(int streamIdx, QualifyingSet qualifyingSet)
    {
        boolean isFirstFunction = true;
        for (FilterFunction function : filterFunctionOrder[streamIdx]) {
            int[] channels = function.getInputChannels();
            Block[] blocks = new Block[channels.length];
            int numRows = qualifyingSet.getPositionCount();
            for (int channelIdx = 0; channelIdx < channels.length; channelIdx++) {
                blocks[channelIdx] = makeFilterFunctionInputBlock(channelIdx, streamIdx, numRows, function);
            }
            if (filterResults == null || filterResults.length < numRows) {
                filterResults = new int[numRows + 100];
            }
            StreamReader reader = sortedStreamReaders[streamIdx];
            qualifyingSet = reader.getOrCreateOutputQualifyingSet();
            long start = System.nanoTime();
            int numHits = function.filter(new Page(numRows, blocks), filterResults, qualifyingSet.getOrCreateErrorSet());
            function.updateStats(numRows, numHits, System.nanoTime() - start);
            if (reader.getFilter() == null && isFirstFunction) {
                qualifyingSet.copyFrom(reader.getInputQualifyingSet());
                int end = qualifyingSet.getEnd();
                // If the reader stopped because of space budget, the
                // truncation is recorded in the reader, the output qset
                // just has the output that was produced and ends at the
                // truncation row of the end of the input qset.
                qualifyingSet.clearTruncationPosition();
                qualifyingSet.setEnd(end);
                // inputNumbers[i] is the offset of the qualifying row within the input qualifying set.
                int[] inputNumbers = qualifyingSet.getMutableInputNumbers(numHits);
                System.arraycopy(filterResults, 0, inputNumbers, 0, numHits);
            }
            else {
                qualifyingSet.compactInputNumbers(filterResults, numHits);
            }
            reader.compactValues(filterResults, numRowsInResult, numHits);
            if (numHits == 0) {
                return qualifyingSet;
            }
            isFirstFunction = false;
        }
        return qualifyingSet;
    }

    private Block makeFilterFunctionInputBlock(int channelIdx, int streamIdx, int numRows, FilterFunction function)
    {
        int channel = function.getInputChannels()[channelIdx];
        int[][] rowNumberMaps = function.getChannelRowNumberMaps();
        boolean mustCopyMap = false;
        int[] map = null;
        for (int operandIdx = streamIdx; operandIdx >= 0; operandIdx--) {
            StreamReader reader = sortedStreamReaders[operandIdx];
            if (channel == reader.getChannel()) {
                Block block = reader.getBlock(reader.getNumValues(), true);
                if (map == null) {
                    if (numRowsInResult > 0) {
                        return block.getRegion(numRowsInResult, block.getPositionCount() - numRowsInResult);
                    }
                    return block;
                }
                if (numRowsInResult > 0) {
                    // Offset the map to point to values added in this batch.
                    if (mustCopyMap) {
                        map = copyMap(rowNumberMaps, channelIdx, numRows, map);
                    }
                    for (int i = 0; i < numRows; i++) {
                        map[i] += numRowsInResult;
                    }
                }
                return new DictionaryBlock(numRows, block, map);
            }
            if (needRowNumberMap(operandIdx, function)) {
                QualifyingSet filterSet = sortedStreamReaders[operandIdx].getOutputQualifyingSet();
                int[] inputNumbers = filterSet.getInputNumbers();
                if (map == null) {
                    map = inputNumbers;
                    mustCopyMap = true;
                }
                else {
                    if (mustCopyMap) {
                        map = copyMap(rowNumberMaps, channelIdx, numRows, map);
                        mustCopyMap = false;
                    }
                    for (int i = 0; i < numRows; i++) {
                        map[i] = inputNumbers[map[i]];
                    }
                }
            }
        }
        throw new IllegalArgumentException("Filter function input channel not found");
    }

    // Returns true if the reader at operandIdx should add a row
    // number mapping for an input of filterFunction that is produced
    // by a reader to the left of operandIdx.
    private boolean needRowNumberMap(int operandIdx, FilterFunction filterFunction)
    {
        // If there is a non-function filter, this introduces a row number mapping.
        if (sortedStreamReaders[operandIdx].getFilter() != null) {
            return true;
        }
        // If the filter function is the first of the filter functions
        // at operandIdx and the reader at operandIdx has no filter,
        // there is no qset in effect at operandIdx until the first
        // filter is evaluated. Hence the first filter needs no row
        // number mapping but any non-first filter will use the
        // mapping produced by the previous one.
        if (filterFunctionOrder[operandIdx] != null && filterFunctionOrder[operandIdx][0] != filterFunction) {
            return true;
        }
        return false;
    }

    private static int[] copyMap(int[][] maps, int channelIdx, int numRows, int[] map)
    {
        int[] copy = allocRowNumberMap(maps, channelIdx, numRows);
        System.arraycopy(map, 0, copy, 0, numRows);
        return copy;
    }

    private static int[] allocRowNumberMap(int[][] maps, int mapIdx, int size)
    {
        int[] map = maps[mapIdx];
        if (map == null || map.length < size) {
            maps[mapIdx] = new int[size + 100];
            return maps[mapIdx];
        }
        return map;
    }

    private void setNewTruncation(int streamIdx, QualifyingSet set)
    {
        // Do not read past the end of a column to the left. The new truncation is the end of the closest truncated to the left or the closest filter.
        if (streamIdx == 0) {
            // If leftmost column is truncated, inputQualifyingSet has
            // the new truncation, which is set by the caller. The top
            // level reader always clears the truncation, a struct
            // reader sets it according to the truncation in its input
            // QualifyingSet.
            return;
        }
        int newEnd = -1;
        for (int i = streamIdx - 1; i >= 0; i--) {
            StreamReader reader = sortedStreamReaders[i];
            if (hasFilter(i)) {
                break;
            }
            int row = reader.getTruncationRow();
            if (row != -1) {
                newEnd = row;
                break;
            }
        }
        set.setTruncationRow(newEnd);
    }

    private boolean hasFilter(int i)
    {
        return sortedStreamReaders[i].getFilter() != null || (filterFunctionOrder.length > i && filterFunctionOrder[i] != null);
    }

    /* Compacts Blocks that contain values on rows that subsequent
     * filters have dropped. lastStreamIdx is the position in
     * sortedStreamReaders for the rightmost stream that has values for
     * this batch. */
    private void alignResultsAndRemoveFromQualifyingSet(int numAdded, int lastStreamIdx)
    {
        boolean needCompact = false;
        int numSurviving = numAdded;
        for (int streamIdx = lastStreamIdx; streamIdx >= 0; --streamIdx) {
            StreamReader reader = sortedStreamReaders[streamIdx];
            if (needCompact) {
                reader.compactValues(survivingRows, numRowsInResult, numSurviving);
            }
            QualifyingSet output = reader.getOutputQualifyingSet();
            QualifyingSet input = reader.getInputQualifyingSet();
            int truncationRow = reader.getTruncationRow();
            if (truncationRow == -1 &&
                    (output == null || output.getPositionCount() == input.getPositionCount() && !output.hasErrors())) {
                continue;
            }
            if (streamIdx == 0 && outputQualifyingSet == null) {
                break;
            }
            if (hasFilter(streamIdx)) {
                int[] resultInputNumbers = output.getInputNumbers();
                if (!needCompact) {
                    if (survivingRows == null || survivingRows.length < numSurviving) {
                        survivingRows = Arrays.copyOf(resultInputNumbers, numSurviving);
                    }
                    else {
                        System.arraycopy(resultInputNumbers, 0, survivingRows, 0, numSurviving);
                    }
                    needCompact = true;
                }
                else {
                    for (int i = 0; i < numSurviving; i++) {
                        survivingRows[i] = resultInputNumbers[survivingRows[i]];
                    }
                }
                // All columns will be aligned so that elements of a
                // row are at the same index. Hence inputNumbers will
                // also be consecutive for the positions that are not
                // erased at the end of this function. These will be
                // referenced on subsequent calls when resuming after
                // truncation.
                for (int i = numAdded; i < numSurviving; i++) {
                    resultInputNumbers[i] = i;
                }
            }
            if (truncationRow != -1 && streamIdx > 0) {
                numSurviving = addUnusedInputToSurviving(reader, numSurviving, 0);
            }
        }
        // Record the input rows that made it into the output qualifying set.
        if (outputQualifyingSet != null) {
            outputQualifyingSet.reset(numAdded);
            if (inputQualifyingSet.getTranslateResultToParentRows()) {
                int[] translation = inputQualifyingSet.getInputNumbers();
                QualifyingSet parent = inputQualifyingSet.getParent();
                int[] parentRows = parent.getPositions();
                for (int i = 0; i < numAdded; i++) {
                    int row = needCompact ? survivingRows[i] : i;
                    int parentPos = translation[row];
                    outputQualifyingSet.append(parentRows[parentPos], parentPos);
                }
            }
            else {
                int[] inputRows = inputQualifyingSet.getPositions();
                for (int i = 0; i < numAdded; i++) {
                    int row = needCompact ? survivingRows[i] : i;
                    outputQualifyingSet.append(inputRows[row], row);
                }
            }
        }

        StreamReader lastReader = sortedStreamReaders[lastStreamIdx];
        int endRow = getCurrentRow(lastReader);
        QualifyingSet lastOutput = lastReader.getOutputQualifyingSet();
        if (lastOutput != null) {
            // Signals errors if any left.
            lastOutput.eraseBelowRow(endRow);
        }
        lastReader.getInputQualifyingSet().clearTruncationPosition();
        for (int streamIdx = lastStreamIdx - 1; streamIdx >= 0; --streamIdx) {
            StreamReader reader = sortedStreamReaders[streamIdx];
            if (hasFilter(streamIdx)) {
                reader.getOutputQualifyingSet().clearTruncationPosition();
                reader.getOutputQualifyingSet().eraseBelowRow(endRow);
            }
        }
        inputQualifyingSet.eraseBelowRow(endRow);
    }

    private static int getCurrentRow(StreamReader reader)
    {
        int row = reader.getTruncationRow();
        if (row != -1) {
            return row;
        }
        return reader.getPosition();
    }

    private int addUnusedInputToSurviving(StreamReader reader, int numSurviving, int offset)
    {
        int truncationRow = reader.getTruncationRow();
        QualifyingSet input = reader.getInputQualifyingSet();
        int numIn = input.getTotalPositionCount();
        int[] rows = input.getPositions();
        // Find the place of the truncation row in the input and add
        // this and all above this to surviving.
        for (int i = 0; i < numIn; i++) {
            if (rows[i] == truncationRow) {
                int numAdded = numIn - i;
                if (survivingRows == null) {
                    survivingRows = new int[numSurviving + numAdded];
                }
                if (survivingRows.length < numSurviving + numAdded) {
                    survivingRows = Arrays.copyOf(survivingRows, numSurviving + numAdded + 100);
                }
                for (int counter = 0; counter < numAdded; counter++) {
                    survivingRows[numSurviving + counter] = i + counter + offset;
                }
                return numSurviving + numAdded;
            }
        }
        throw new IllegalArgumentException("Truncation row was not in the input QualifyingSet");
    }

    private int findLastTruncatedStreamIdx()
    {
        for (int i = sortedStreamReaders.length - 1; i >= 0; i--) {
            if (sortedStreamReaders[i].getTruncationRow() != -1) {
                return i;
            }
        }
        return -1;
    }

    public int getTruncationRow()
    {
        int idx = findLastTruncatedStreamIdx();
        if (idx == -1) {
            return -1;
        }
        return sortedStreamReaders[idx].getTruncationRow();
    }

    public int getResultSizeInBytes()
    {
        int sum = 0;
        for (StreamReader reader : sortedStreamReaders) {
            sum += reader.getResultSizeInBytes();
        }
        return sum;
    }

    public int getNumResults()
    {
        return numRowsInResult;
    }

    public int getAverageResultSize()
    {
        int sum = 0;
        for (StreamReader reader : sortedStreamReaders) {
            sum += reader.getAverageResultSize();
        }
        return sum;
    }

    public void compactValues(int[] surviving, int base, int numSurviving)
    {
        // The values under numRowsInResult are aligned and
        // compact. Some of these will be compacted away. Some columns
        // will have more than numRowsInResult rows if truncation of
        // columns to their right has cut the result size. These
        // values are left in a compact and aligned state by
        // align... Qualifying sets will only mention rows above
        // numRowsInResult, since align... erases the information for
        // rows where all columns are processed. The row numbers are
        // left in place, the input numbers are rewritten to be
        // consecutive from 0 onwards.
        if (survivingRows == null || survivingRows.length < numSurviving) {
            survivingRows = new int[numSurviving + 100];
        }
        // Copy the surviving to a local array because this may need to
        // get resized if we have readers that have more data then the
        // last one.
        System.arraycopy(surviving, 0, survivingRows, 0, numSurviving);
        int initialNumSurviving = numSurviving;
        for (int streamIdx = sortedStreamReaders.length - 1; streamIdx >= 0; streamIdx--) {
            StreamReader reader = sortedStreamReaders[streamIdx];
            QualifyingSet output = reader.getOutputQualifyingSet();
            if (output != null) {
                // Save the output QualifyingSet so that compactValues
                // will not disturb this.
                reader.setOutputQualifyingSet(null);
            }
            reader.compactValues(survivingRows, base, numSurviving);
            if (output != null) {
                reader.setOutputQualifyingSet(output);
                if (streamIdx > 0) {
                    // These numbers refer to the positions in the
                    // reader to the left. The values are aligned and
                    // on next use the bottom numRowsInResult will be
                    // erased, so positions start at 0.
                    int numOutput = output.getPositionCount();
                    int[] inputNumbers = output.getMutableInputNumbers(numOutput);
                    for (int i = 0; i < numOutput; i++) {
                        inputNumbers[i] = i;
                    }
                }
            }
            if (streamIdx == 0) {
                break;
            }
            int truncated = reader.getTruncationRow();
            if (truncated != -1) {
                numSurviving = addUnusedInputToSurviving(reader, numSurviving, numRowsInResult);
            }
        }
        numRowsInResult = base + initialNumSurviving;
        // Check.
        // getBlocks(numRowsInResult, true, false);
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder("CGR: rows:")
                .append(numRowsInResult)
                .append(" bytes: ")
                .append(getResultSizeInBytes());
        for (StreamReader reader : sortedStreamReaders) {
            builder.append("C ").append(reader.getColumnIndex());
            int row = reader.getTruncationRow();
            if (row != -1) {
                builder.append("trunc: ").append(row);
            }
        }
        return builder.toString();
    }
}
