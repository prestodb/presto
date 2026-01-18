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
package com.facebook.presto.operator;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.table.TableFunctionDataProcessor;
import com.facebook.presto.spi.function.table.TableFunctionProcessorState;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.concurrent.MoreFutures.toListenableFuture;
import static com.facebook.presto.common.Utils.checkState;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RegularTableFunctionPartition
        implements TableFunctionPartition
{
    private final PagesIndex pagesIndex;
    private final int partitionStart;
    private final int partitionEnd;
    private final Iterator<Page> sortedPages;

    private final TableFunctionDataProcessor tableFunction;
    private final int properChannelsCount;
    private final int passThroughSourcesCount;

    // channels required by the table function, listed by source in order of argument declarations
    private final int[][] requiredChannels;

    // for each input channel, the end position of actual data in that channel (exclusive) relative to partition. The remaining rows are "filler" rows, and should not be passed to table function or passed-through
    private final int[] endOfData;

    // a builder for each pass-through column, in order of argument declarations
    private final PassThroughColumnProvider[] passThroughProviders;

    // number of processed input positions from partition start. all sources have been processed up to this position, except the sources whose partitions ended earlier.
    private int processedPositions;

    public RegularTableFunctionPartition(
            PagesIndex pagesIndex,
            int partitionStart,
            int partitionEnd,
            TableFunctionDataProcessor tableFunction,
            int properChannelsCount,
            int passThroughSourcesCount,
            List<List<Integer>> requiredChannels,
            Optional<Map<Integer, Integer>> markerChannels,
            List<PassThroughColumnSpecification> passThroughSpecifications)

    {
        checkArgument(pagesIndex.getPositionCount() != 0, "PagesIndex is empty for regular table function partition");
        this.pagesIndex = pagesIndex;
        this.partitionStart = partitionStart;
        this.partitionEnd = partitionEnd;
        this.sortedPages = pagesIndex.getSortedPages(partitionStart, partitionEnd);
        this.tableFunction = requireNonNull(tableFunction, "tableFunction is null");
        this.properChannelsCount = properChannelsCount;
        this.passThroughSourcesCount = passThroughSourcesCount;
        this.requiredChannels = requiredChannels.stream()
                .map(Ints::toArray)
                .toArray(int[][]::new);
        this.endOfData = findEndOfData(markerChannels, requiredChannels, passThroughSpecifications);
        for (List<Integer> channels : requiredChannels) {
            checkState(
                    channels.stream()
                            .mapToInt(channel -> endOfData[channel])
                            .distinct()
                            .count() <= 1,
                    "end-of-data position is inconsistent within a table function source");
        }
        this.passThroughProviders = new PassThroughColumnProvider[passThroughSpecifications.size()];
        for (int i = 0; i < passThroughSpecifications.size(); i++) {
            passThroughProviders[i] = createColumnProvider(passThroughSpecifications.get(i));
        }
    }

    @Override
    public WorkProcessor<Page> toOutputPages()
    {
        return WorkProcessor.create(new WorkProcessor.Process<Page>()
        {
            List<Optional<Page>> inputPages = prepareInputPages();

            @Override
            public WorkProcessor.ProcessState<Page> process()
            {
                TableFunctionProcessorState state = tableFunction.process(inputPages);
                boolean functionGotNoData = inputPages == null;
                if (state == FINISHED) {
                    return WorkProcessor.ProcessState.finished();
                }
                if (state instanceof TableFunctionProcessorState.Blocked) {
                    return WorkProcessor.ProcessState.blocked(toListenableFuture(((TableFunctionProcessorState.Blocked) state).getFuture()));
                }
                TableFunctionProcessorState.Processed processed = (TableFunctionProcessorState.Processed) state;
                if (processed.isUsedInput()) {
                    inputPages = prepareInputPages();
                }
                if (processed.getResult() != null) {
                    return WorkProcessor.ProcessState.ofResult(appendPassThroughColumns(processed.getResult()));
                }
                if (functionGotNoData) {
                    throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "When function got no input, it should either produce output or return Blocked state");
                }
                return WorkProcessor.ProcessState.blocked(immediateFuture(null));
            }
        });
    }

    /**
     * Iterate over the partition by page and extract pages for each table function source from the input page.
     * For each source, project the columns required by the table function.
     * If for some source all data in the partition has been consumed, Optional.empty() is returned for that source.
     * It happens when the partition of this source is shorter than the partition of some other source.
     * The overall length of the table function partition is equal to the length of the longest source partition.
     * When all sources are fully consumed, this method returns null.
     * <p>
     * NOTE: There are two types of table function's source semantics: set and row. The two types of sources should be handled
     * by the TableFunctionDataProcessor in different ways. For a source with set semantics, the whole partition can be used for computations,
     * while for a source with row semantics, each row should be processed independently from all other rows.
     * To enforce that behavior, we could pass to the TableFunctionDataProcessor only one row from a table with row semantics.
     * However, for performance reasons, we handle sources with row and set semantics in the same way: the TableFunctionDataProcessor
     * gets a page of data from each source. The TableFunctionDataProcessor is responsible for using the provided data accordingly
     * to the declared source semantics (set or rows).
     *
     * @return A List containing:
     * - Optional Page for every source that is not fully consumed
     * - Optional.empty() for every source that is fully consumed
     * or null if all sources are fully consumed.
     */
    private List<Optional<Page>> prepareInputPages()
    {
        if (!sortedPages.hasNext()) {
            return null;
        }

        Page inputPage = sortedPages.next();
        ImmutableList.Builder<Optional<Page>> sourcePages = ImmutableList.builder();

        for (int[] channelsForSource : requiredChannels) {
            if (channelsForSource.length == 0) {
                sourcePages.add(Optional.of(new Page(inputPage.getPositionCount())));
            }
            else {
                int endOfDataForSource = endOfData[channelsForSource[0]]; // end-of-data position is validated to be consistent for all channels from source
                if (endOfDataForSource <= processedPositions) {
                    // all data for this source was already processed
                    sourcePages.add(Optional.empty());
                }
                else {
                    Block[] sourceBlocks = new Block[channelsForSource.length];
                    if (endOfDataForSource < processedPositions + inputPage.getPositionCount()) {
                        // data for this source ends within the current page
                        for (int i = 0; i < channelsForSource.length; i++) {
                            int inputChannel = channelsForSource[i];
                            sourceBlocks[i] = inputPage.getBlock(inputChannel).getRegion(0, endOfDataForSource - processedPositions);
                        }
                    }
                    else {
                        // data for this source does not end within the current page
                        for (int i = 0; i < channelsForSource.length; i++) {
                            int inputChannel = channelsForSource[i];
                            sourceBlocks[i] = inputPage.getBlock(inputChannel);
                        }
                    }
                    sourcePages.add(Optional.of(new Page(sourceBlocks)));
                }
            }
        }

        processedPositions += inputPage.getPositionCount();

        return sourcePages.build();
    }

    /**
     * There are two types of table function's source semantics: set and row.
     * <p>
     * For a source with row semantics, the table function result depends on the whole partition,
     * so it is not always possible to associate an output row with a specific input row.
     * The TableFunctionDataProcessor can return null as the pass-through index to indicate that
     * the output row is not associated with any row from the given source.
     * <p>
     * For a source with row semantics, the output is determined on a row-by-row basis, so every
     * output row is associated with a specific input row. In such case, the pass-through index
     * should never be null.
     * <p>
     * In our implementation, we handle sources with row and set semantics in the same way.
     * For performance reasons, we do not validate the null pass-through indexes.
     * The TableFunctionDataProcessor is responsible for using the pass-through capability
     * accordingly to the declared source semantics (set or rows).
     */
    private Page appendPassThroughColumns(Page page)
    {
        if (page.getChannelCount() != properChannelsCount + passThroughSourcesCount) {
            throw new PrestoException(
                    FUNCTION_IMPLEMENTATION_ERROR,
                    format(
                            "Table function returned a page containing %s channels. Expected channel number: %s (%s proper columns, %s pass-through index columns)",
                            page.getChannelCount(),
                            properChannelsCount + passThroughSourcesCount,
                            properChannelsCount,
                            passThroughSourcesCount));
        }
        // TODO is it possible to verify types of columns returned by TF?

        Block[] resultBlocks = new Block[properChannelsCount + passThroughProviders.length];

        // proper outputs first
        for (int channel = 0; channel < properChannelsCount; channel++) {
            resultBlocks[channel] = page.getBlock(channel);
        }

        // pass-through columns next
        int channel = properChannelsCount;
        for (PassThroughColumnProvider provider : passThroughProviders) {
            resultBlocks[channel] = provider.getPassThroughColumn(page);
            channel++;
        }

        // pass the position count so that the Page can be successfully created in the case when there are no output channels (resultBlocks is empty)
        return new Page(page.getPositionCount(), resultBlocks);
    }

    private int[] findEndOfData(Optional<Map<Integer, Integer>> markerChannels, List<List<Integer>> requiredChannels, List<PassThroughColumnSpecification> passThroughSpecifications)
    {
        Set<Integer> referencedChannels = ImmutableSet.<Integer>builder()
                .addAll(requiredChannels.stream()
                        .flatMap(Collection::stream)
                        .collect(toImmutableList()))
                .addAll(passThroughSpecifications.stream()
                        .map(PassThroughColumnSpecification::getInputChannel)
                        .collect(toImmutableList()))
                .build();

        if (referencedChannels.isEmpty()) {
            // no required or pass-through channels
            return null;
        }

        int maxInputChannel = referencedChannels.stream()
                .mapToInt(Integer::intValue)
                .max()
                .orElseThrow(NoSuchElementException::new);

        int[] result = new int[maxInputChannel + 1];
        Arrays.fill(result, -1);

        // if table function had one source, adding a marker channel was not necessary.
        // end-of-data position is equal to partition end for each input channel
        if (!markerChannels.isPresent()) {
            referencedChannels.stream()
                    .forEach(channel -> result[channel] = partitionEnd - partitionStart);
            return result;
        }

        // if table function had more than one source, the markers map shall be present, and it shall contain mapping for each input channel
        ImmutableMap.Builder<Integer, Integer> endOfDataPerMarkerBuilder = ImmutableMap.builder();
        for (int markerChannel : ImmutableSet.copyOf(markerChannels.orElseThrow(NoSuchElementException::new).values())) {
            endOfDataPerMarkerBuilder.put(markerChannel, findFirstNullPosition(markerChannel));
        }
        Map<Integer, Integer> endOfDataPerMarker = endOfDataPerMarkerBuilder.buildOrThrow();
        referencedChannels.stream()
                .forEach(channel -> result[channel] = endOfDataPerMarker.get(markerChannels.orElseThrow(NoSuchElementException::new).get(channel)) - partitionStart);

        return result;
    }

    private int findFirstNullPosition(int markerChannel)
    {
        if (pagesIndex.isNull(markerChannel, partitionStart)) {
            return partitionStart;
        }
        if (!pagesIndex.isNull(markerChannel, partitionEnd - 1)) {
            return partitionEnd;
        }

        int start = partitionStart;
        int end = partitionEnd;
        // value at start is not null, value at end is null
        while (end - start > 1) {
            int mid = (start + end) >>> 1;
            if (pagesIndex.isNull(markerChannel, mid)) {
                end = mid;
            }
            else {
                start = mid;
            }
        }
        return end;
    }

    public static class PassThroughColumnSpecification
    {
        private final boolean isPartitioningColumn;
        private final int inputChannel;
        private final int indexChannel;

        public PassThroughColumnSpecification(boolean isPartitioningColumn, int inputChannel, int indexChannel)
        {
            this.isPartitioningColumn = isPartitioningColumn;
            this.inputChannel = inputChannel;
            this.indexChannel = indexChannel;
        }

        public boolean isPartitioningColumn()
        {
            return isPartitioningColumn;
        }

        public int getInputChannel()
        {
            return inputChannel;
        }

        public int getIndexChannel()
        {
            return indexChannel;
        }
    }

    private PassThroughColumnProvider createColumnProvider(PassThroughColumnSpecification specification)
    {
        if (specification.isPartitioningColumn()) {
            return new PartitioningColumnProvider(pagesIndex.getSingleValueBlock(specification.getInputChannel(), partitionStart));
        }
        return new NonPartitioningColumnProvider(specification.getInputChannel(), specification.getIndexChannel());
    }

    private interface PassThroughColumnProvider
    {
        Block getPassThroughColumn(Page page);
    }

    private static class PartitioningColumnProvider
            implements PassThroughColumnProvider
    {
        private final Block partitioningValue;

        private PartitioningColumnProvider(Block partitioningValue)
        {
            this.partitioningValue = requireNonNull(partitioningValue, "partitioningValue is null");
        }

        @Override
        public Block getPassThroughColumn(Page page)
        {
            return new RunLengthEncodedBlock(partitioningValue, page.getPositionCount());
        }

        public Block getPartitioningValue()
        {
            return partitioningValue;
        }
    }

    private final class NonPartitioningColumnProvider
            implements PassThroughColumnProvider
    {
        private final int inputChannel;
        private final Type type;
        private final int indexChannel;

        public NonPartitioningColumnProvider(int inputChannel, int indexChannel)
        {
            this.inputChannel = inputChannel;
            this.type = pagesIndex.getType(inputChannel);
            this.indexChannel = indexChannel;
        }

        @Override
        public Block getPassThroughColumn(Page page)
        {
            Block indexes = page.getBlock(indexChannel);
            BlockBuilder builder = type.createBlockBuilder(null, page.getPositionCount());
            for (int position = 0; position < page.getPositionCount(); position++) {
                if (indexes.isNull(position)) {
                    builder.appendNull();
                }
                else {
                    // table function returns index from partition start
                    long index = BIGINT.getLong(indexes, position);
                    // validate index
                    if (index < 0 || index >= endOfData[inputChannel] || index >= processedPositions) {
                        int end = min(endOfData[inputChannel], processedPositions) - 1;
                        if (end >= 0) {
                            throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, format("Index of a pass-through row: %s out of processed portion of partition [0, %s]", index, end));
                        }
                        else {
                            throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "Index of a pass-through row must be null when no input data from the partition was processed. Actual: " + index);
                        }
                    }
                    // index in PagesIndex
                    long absoluteIndex = partitionStart + index;
                    pagesIndex.appendTo(inputChannel, toIntExact(absoluteIndex), builder);
                }
            }

            return builder.build();
        }
    }
}
