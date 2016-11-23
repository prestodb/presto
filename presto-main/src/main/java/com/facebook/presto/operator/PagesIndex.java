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

import com.facebook.presto.Session;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.JoinCompiler.LookupSourceSupplierFactory;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;

/**
 * PagesIndex a low-level data structure which contains the address of every value position of every channel.
 * This data structure is not general purpose and is designed for a few specific uses:
 * <ul>
 * <li>Sort via the {@link #sort} method</li>
 * <li>Hash build via the {@link #createLookupSourceSupplier} method</li>
 * <li>Positional output via the {@link #appendTo} method</li>
 * </ul>
 */
public class PagesIndex
        implements Swapper
{
    private static final Logger log = Logger.get(PagesIndex.class);

    private final OrderingCompiler orderingCompiler;
    private final JoinCompiler joinCompiler;

    private final List<Type> types;
    private final LongArrayList valueAddresses;
    private final ObjectArrayList<Block>[] channels;

    private int nextBlockToCompact;
    private int positionCount;
    private long pagesMemorySize;
    private long estimatedSize;

    private PagesIndex(OrderingCompiler orderingCompiler, JoinCompiler joinCompiler, List<Type> types, int expectedPositions)
    {
        this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.valueAddresses = new LongArrayList(expectedPositions);

        //noinspection rawtypes
        channels = (ObjectArrayList<Block>[]) new ObjectArrayList[types.size()];
        for (int i = 0; i < channels.length; i++) {
            channels[i] = ObjectArrayList.wrap(new Block[1024], 0);
        }
    }

    public interface Factory
    {
        PagesIndex newPagesIndex(List<Type> types, int expectedPositions);
    }

    public static class TestingFactory
            implements Factory
    {
        private static final OrderingCompiler ORDERING_COMPILER = new OrderingCompiler();
        private static final JoinCompiler JOIN_COMPILER = new JoinCompiler();

        @Override
        public PagesIndex newPagesIndex(List<Type> types, int expectedPositions)
        {
            return new PagesIndex(ORDERING_COMPILER, JOIN_COMPILER, types, expectedPositions);
        }
    }

    public static class DefaultFactory
            implements Factory
    {
        private final OrderingCompiler orderingCompiler;
        private final JoinCompiler joinCompiler;

        @Inject
        public DefaultFactory(OrderingCompiler orderingCompiler, JoinCompiler joinCompiler)
        {
            this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        }

        @Override
        public PagesIndex newPagesIndex(List<Type> types, int expectedPositions)
        {
            return new PagesIndex(orderingCompiler, joinCompiler, types, expectedPositions);
        }
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public LongArrayList getValueAddresses()
    {
        return valueAddresses;
    }

    public ObjectArrayList<Block> getChannel(int channel)
    {
        return channels[channel];
    }

    public void clear()
    {
        for (ObjectArrayList<Block> channel : channels) {
            channel.clear();
        }
        valueAddresses.clear();
        positionCount = 0;
        pagesMemorySize = 0;

        estimatedSize = calculateEstimatedSize();
    }

    public void addPage(Page page)
    {
        // ignore empty pages
        if (page.getPositionCount() == 0) {
            return;
        }

        positionCount += page.getPositionCount();

        int pageIndex = (channels.length > 0) ? channels[0].size() : 0;
        for (int i = 0; i < channels.length; i++) {
            Block block = page.getBlock(i);
            channels[i].add(block);
            pagesMemorySize += block.getRetainedSizeInBytes();
        }

        for (int position = 0; position < page.getPositionCount(); position++) {
            long sliceAddress = encodeSyntheticAddress(pageIndex, position);
            valueAddresses.add(sliceAddress);
        }

        estimatedSize = calculateEstimatedSize();
    }

    public DataSize getEstimatedSize()
    {
        return new DataSize(estimatedSize, BYTE);
    }

    public void compact()
    {
        for (int channel = 0; channel < types.size(); channel++) {
            ObjectArrayList<Block> blocks = channels[channel];
            for (int i = nextBlockToCompact; i < blocks.size(); i++) {
                Block block = blocks.get(i);
                if (block.getSizeInBytes() < block.getRetainedSizeInBytes()) {
                    // Copy the block to compact its size
                    Block compactedBlock = block.copyRegion(0, block.getPositionCount());
                    blocks.set(i, compactedBlock);
                    pagesMemorySize -= block.getRetainedSizeInBytes();
                    pagesMemorySize += compactedBlock.getRetainedSizeInBytes();
                }
            }
        }
        nextBlockToCompact = channels[0].size();
        estimatedSize = calculateEstimatedSize();
    }

    private long calculateEstimatedSize()
    {
        long elementsSize = (channels.length > 0) ? sizeOf(channels[0].elements()) : 0;
        long channelsArraySize = elementsSize * channels.length;
        long addressesArraySize = sizeOf(valueAddresses.elements());
        return pagesMemorySize + channelsArraySize + addressesArraySize;
    }

    public Type getType(int channel)
    {
        return types.get(channel);
    }

    @Override
    public void swap(int a, int b)
    {
        long[] elements = valueAddresses.elements();
        long temp = elements[a];
        elements[a] = elements[b];
        elements[b] = temp;
    }

    public int buildPage(int position, int[] outputChannels, PageBuilder pageBuilder)
    {
        while (!pageBuilder.isFull() && position < positionCount) {
            long pageAddress = valueAddresses.getLong(position);
            int blockIndex = decodeSliceIndex(pageAddress);
            int blockPosition = decodePosition(pageAddress);

            // append the row
            pageBuilder.declarePosition();
            for (int i = 0; i < outputChannels.length; i++) {
                int outputChannel = outputChannels[i];
                Type type = types.get(outputChannel);
                Block block = this.channels[outputChannel].get(blockIndex);
                type.appendTo(block, blockPosition, pageBuilder.getBlockBuilder(i));
            }

            position++;
        }

        return position;
    }

    public void appendTo(int channel, int position, BlockBuilder output)
    {
        long pageAddress = valueAddresses.getLong(position);

        Type type = types.get(channel);
        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        type.appendTo(block, blockPosition, output);
    }

    public boolean isNull(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return block.isNull(blockPosition);
    }

    public boolean getBoolean(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getBoolean(block, blockPosition);
    }

    public long getLong(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getLong(block, blockPosition);
    }

    public double getDouble(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getDouble(block, blockPosition);
    }

    public Slice getSlice(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getSlice(block, blockPosition);
    }

    public Block getSingleValueBlock(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return block.getSingleValueBlock(blockPosition);
    }

    public void sort(List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        sort(sortChannels, sortOrders, 0, getPositionCount());
    }

    public void sort(List<Integer> sortChannels, List<SortOrder> sortOrders, int startPosition, int endPosition)
    {
        createPagesIndexComparator(sortChannels, sortOrders).sort(this, startPosition, endPosition);
    }

    public boolean positionEqualsPosition(PagesHashStrategy partitionHashStrategy, int leftPosition, int rightPosition)
    {
        long leftAddress = valueAddresses.getLong(leftPosition);
        int leftPageIndex = decodeSliceIndex(leftAddress);
        int leftPagePosition = decodePosition(leftAddress);

        long rightAddress = valueAddresses.getLong(rightPosition);
        int rightPageIndex = decodeSliceIndex(rightAddress);
        int rightPagePosition = decodePosition(rightAddress);

        return partitionHashStrategy.positionEqualsPosition(leftPageIndex, leftPagePosition, rightPageIndex, rightPagePosition);
    }

    public boolean positionEqualsRow(PagesHashStrategy pagesHashStrategy, int indexPosition, int rightPosition, Page rightPage)
    {
        long pageAddress = valueAddresses.getLong(indexPosition);
        int pageIndex = decodeSliceIndex(pageAddress);
        int pagePosition = decodePosition(pageAddress);

        return pagesHashStrategy.positionEqualsRow(pageIndex, pagePosition, rightPosition, rightPage);
    }

    private PagesIndexOrdering createPagesIndexComparator(List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        List<Type> sortTypes = sortChannels.stream()
                .map(types::get)
                .collect(toImmutableList());
        return orderingCompiler.compilePagesIndexOrdering(sortTypes, sortChannels, sortOrders);
    }

    public Supplier<LookupSource> createLookupSourceSupplier(Session session, List<Integer> joinChannels)
    {
        return createLookupSourceSupplier(session, joinChannels, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public PagesHashStrategy createPagesHashStrategy(List<Integer> joinChannels, Optional<Integer> hashChannel)
    {
        return createPagesHashStrategy(joinChannels, hashChannel, Optional.empty());
    }

    public PagesHashStrategy createPagesHashStrategy(List<Integer> joinChannels, Optional<Integer> hashChannel, Optional<List<Integer>> outputChannels)
    {
        try {
            return joinCompiler.compilePagesHashStrategyFactory(types, joinChannels, outputChannels)
                    .createPagesHashStrategy(ImmutableList.copyOf(channels), hashChannel);
        }
        catch (Exception e) {
            log.error(e, "Lookup source compile failed for types=%s error=%s", types, e);
        }

        // if compilation fails, use interpreter
        return new SimplePagesHashStrategy(types, outputChannels.orElse(rangeList(types.size())), ImmutableList.copyOf(channels), joinChannels, hashChannel);
    }

    public LookupSourceSupplier createLookupSourceSupplier(
            Session session,
            List<Integer> joinChannels,
            Optional<Integer> hashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory)
    {
        return createLookupSourceSupplier(session, joinChannels, hashChannel, filterFunctionFactory, Optional.empty());
    }

    public LookupSourceSupplier createLookupSourceSupplier(
            Session session,
            List<Integer> joinChannels,
            Optional<Integer> hashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<List<Integer>> outputChannels)
    {
        List<List<Block>> channels = ImmutableList.copyOf(this.channels);
        if (!joinChannels.isEmpty()) {
            // todo compiled implementation of lookup join does not support when we are joining with empty join channels.
            // This code path will trigger only for OUTER joins. To fix that we need to add support for
            //        OUTER joins into NestedLoopsJoin and remove "type == INNER" condition in LocalExecutionPlanner.visitJoin()

            try {
                LookupSourceSupplierFactory lookupSourceFactory = joinCompiler.compileLookupSourceFactory(types, joinChannels, outputChannels);
                return lookupSourceFactory.createLookupSourceSupplier(
                        session.toConnectorSession(),
                        valueAddresses,
                        channels,
                        hashChannel,
                        filterFunctionFactory);
            }
            catch (Exception e) {
                log.error(e, "Lookup source compile failed for types=%s error=%s", types, e);
            }
        }

        // if compilation fails
        PagesHashStrategy hashStrategy = new SimplePagesHashStrategy(
                types,
                outputChannels.orElse(rangeList(types.size())),
                channels,
                joinChannels,
                hashChannel);

        return new JoinHashSupplier(
                session.toConnectorSession(),
                hashStrategy,
                valueAddresses,
                channels,
                filterFunctionFactory);
    }

    private List<Integer> rangeList(int endExclusive)
    {
        return IntStream.range(0, endExclusive)
                .boxed()
                .collect(toImmutableList());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("positionCount", positionCount)
                .add("types", types)
                .add("estimatedSize", estimatedSize)
                .toString();
    }
}
