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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.DataSize;
import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.array.AdaptiveLongBigArray;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.geospatial.Rectangle;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.JoinCompiler.LookupSourceSupplierFactory;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import jakarta.inject.Inject;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.airlift.units.DataSize.Unit.BYTE;
import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.sizeOf;
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
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PagesIndex.class).instanceSize();
    private static final Logger log = Logger.get(PagesIndex.class);

    private final OrderingCompiler orderingCompiler;
    private final JoinCompiler joinCompiler;
    private final FunctionAndTypeManager functionAndTypeManager;

    private final List<Type> types;
    private final AdaptiveLongBigArray valueAddresses;
    private final ObjectArrayList<Block>[] channels;
    private final boolean eagerCompact;

    private int nextBlockToCompact;
    private int positionCount;
    private long pagesMemorySize;
    private long estimatedSize;

    private PagesIndex(
            OrderingCompiler orderingCompiler,
            JoinCompiler joinCompiler,
            FunctionAndTypeManager functionAndTypeManager,
            List<Type> types,
            int expectedPositions,
            boolean eagerCompact)
    {
        this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.valueAddresses = new AdaptiveLongBigArray();
        this.valueAddresses.ensureCapacity(expectedPositions);
        this.eagerCompact = eagerCompact;

        //noinspection rawtypes
        channels = (ObjectArrayList<Block>[]) new ObjectArrayList[types.size()];
        for (int i = 0; i < channels.length; i++) {
            channels[i] = ObjectArrayList.wrap(new Block[1024], 0);
        }

        estimatedSize = calculateEstimatedSize();
    }

    public interface Factory
    {
        PagesIndex newPagesIndex(List<Type> types, int expectedPositions);
    }

    public static class TestingFactory
            implements Factory
    {
        private static final OrderingCompiler ORDERING_COMPILER = new OrderingCompiler();
        private static final JoinCompiler JOIN_COMPILER = new JoinCompiler(MetadataManager.createTestMetadataManager());
        private final boolean eagerCompact;

        public TestingFactory(boolean eagerCompact)
        {
            this.eagerCompact = eagerCompact;
        }

        @Override
        public PagesIndex newPagesIndex(List<Type> types, int expectedPositions)
        {
            return new PagesIndex(ORDERING_COMPILER, JOIN_COMPILER, MetadataManager.createTestMetadataManager().getFunctionAndTypeManager(), types, expectedPositions, eagerCompact);
        }
    }

    public static class DefaultFactory
            implements Factory
    {
        private final OrderingCompiler orderingCompiler;
        private final JoinCompiler joinCompiler;
        private final boolean eagerCompact;
        private final FunctionAndTypeManager functionAndTypeManager;

        @Inject
        public DefaultFactory(OrderingCompiler orderingCompiler, JoinCompiler joinCompiler, FeaturesConfig featuresConfig, Metadata metadata)
        {
            this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
            this.eagerCompact = requireNonNull(featuresConfig, "featuresConfig is null").isPagesIndexEagerCompactionEnabled();
            this.functionAndTypeManager = requireNonNull(metadata, "metadata is null").getFunctionAndTypeManager();
        }

        @Override
        public PagesIndex newPagesIndex(List<Type> types, int expectedPositions)
        {
            return new PagesIndex(orderingCompiler, joinCompiler, functionAndTypeManager, types, expectedPositions, eagerCompact);
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

    public AdaptiveLongBigArray getValueAddresses()
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
            channel.trim();
        }
        valueAddresses.clear();
        positionCount = 0;
        nextBlockToCompact = 0;
        pagesMemorySize = 0;

        estimatedSize = calculateEstimatedSize();
    }

    public void addPage(Page page)
    {
        // ignore empty pages
        if (page.getPositionCount() == 0) {
            return;
        }

        int pageIndex = (channels.length > 0) ? channels[0].size() : 0;
        for (int i = 0; i < channels.length; i++) {
            Block block = page.getBlock(i);
            if (eagerCompact) {
                block = block.copyRegion(0, block.getPositionCount());
            }
            channels[i].add(block);
            pagesMemorySize += block.getRetainedSizeInBytes();
        }

        valueAddresses.ensureCapacity(positionCount + page.getPositionCount());
        for (int position = 0; position < page.getPositionCount(); position++) {
            long sliceAddress = encodeSyntheticAddress(pageIndex, position);
            valueAddresses.set(positionCount, sliceAddress);
            positionCount++;
        }
        estimatedSize = calculateEstimatedSize();
    }

    public DataSize getEstimatedSize()
    {
        return new DataSize(estimatedSize, BYTE);
    }

    public void compact()
    {
        if (eagerCompact) {
            return;
        }
        for (int channel = 0; channel < types.size(); channel++) {
            ObjectArrayList<Block> blocks = channels[channel];
            for (int i = nextBlockToCompact; i < blocks.size(); i++) {
                Block block = blocks.get(i);

                // Copy the block to compact its size
                Block compactedBlock = block.copyRegion(0, block.getPositionCount());
                blocks.set(i, compactedBlock);
                pagesMemorySize -= block.getRetainedSizeInBytes();
                pagesMemorySize += compactedBlock.getRetainedSizeInBytes();
            }
        }
        nextBlockToCompact = channels[0].size();
        estimatedSize = calculateEstimatedSize();
    }

    private long calculateEstimatedSize()
    {
        long elementsSize = (channels.length > 0) ? sizeOf(channels[0].elements()) : 0;
        long channelsArraySize = elementsSize * channels.length;
        long addressesArraySize = valueAddresses.getRetainedSizeInBytes();
        return INSTANCE_SIZE + pagesMemorySize + channelsArraySize + addressesArraySize;
    }

    public Type getType(int channel)
    {
        return types.get(channel);
    }

    @Override
    public void swap(int a, int b)
    {
        valueAddresses.swap(a, b);
    }

    public int buildPage(int position, int endPosition, int[] outputChannels, PageBuilder pageBuilder)
    {
        while (!pageBuilder.isFull() && position < endPosition) {
            long pageAddress = valueAddresses.get(position);
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
        long pageAddress = valueAddresses.get(position);

        Type type = types.get(channel);
        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        type.appendTo(block, blockPosition, output);
    }

    public boolean isNull(int channel, int position)
    {
        long pageAddress = valueAddresses.get(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return block.isNull(blockPosition);
    }

    public boolean getBoolean(int channel, int position)
    {
        long pageAddress = valueAddresses.get(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getBoolean(block, blockPosition);
    }

    public long getLong(int channel, int position)
    {
        long pageAddress = valueAddresses.get(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getLong(block, blockPosition);
    }

    public double getDouble(int channel, int position)
    {
        long pageAddress = valueAddresses.get(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getDouble(block, blockPosition);
    }

    public Slice getSlice(int channel, int position)
    {
        long pageAddress = valueAddresses.get(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getSlice(block, blockPosition);
    }

    public Object getObject(int channel, int position)
    {
        long pageAddress = valueAddresses.get(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getObject(block, blockPosition);
    }

    public Block getSingleValueBlock(int channel, int position)
    {
        long pageAddress = valueAddresses.get(position);

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
        long leftAddress = valueAddresses.get(leftPosition);
        int leftPageIndex = decodeSliceIndex(leftAddress);
        int leftPagePosition = decodePosition(leftAddress);

        long rightAddress = valueAddresses.get(rightPosition);
        int rightPageIndex = decodeSliceIndex(rightAddress);
        int rightPagePosition = decodePosition(rightAddress);

        return partitionHashStrategy.positionEqualsPosition(leftPageIndex, leftPagePosition, rightPageIndex, rightPagePosition);
    }

    public boolean positionEqualsRow(PagesHashStrategy pagesHashStrategy, int indexPosition, int rightPosition, Page rightPage)
    {
        long pageAddress = valueAddresses.get(indexPosition);
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
        return createLookupSourceSupplier(session, joinChannels, OptionalInt.empty(), Optional.empty(), Optional.empty(), ImmutableList.of());
    }

    public PagesHashStrategy createPagesHashStrategy(List<Integer> joinChannels, OptionalInt hashChannel)
    {
        return createPagesHashStrategy(joinChannels, hashChannel, Optional.empty());
    }

    public PagesHashStrategy createPagesHashStrategy(List<Integer> joinChannels, OptionalInt hashChannel, Optional<List<Integer>> outputChannels)
    {
        try {
            return joinCompiler.compilePagesHashStrategyFactory(types, joinChannels, outputChannels)
                    .createPagesHashStrategy(ImmutableList.copyOf(channels), hashChannel);
        }
        catch (Exception e) {
            log.error(e, "Lookup source compile failed for types=%s error=%s", types, e);
        }

        // if compilation fails, use interpreter
        return new SimplePagesHashStrategy(
                types,
                outputChannels.orElseGet(() -> rangeList(types.size())),
                ImmutableList.copyOf(channels),
                joinChannels,
                hashChannel,
                Optional.empty(),
                functionAndTypeManager);
    }

    public PagesIndexComparator createChannelComparator(int leftChannel, int rightChannel, SortOrder sortOrder)
    {
        checkArgument(types.get(leftChannel).equals(types.get(rightChannel)), "comparing channels of different types: %s and %s", types.get(leftChannel), types.get(rightChannel));
        return new SimpleChannelComparator(leftChannel, rightChannel, types.get(leftChannel), sortOrder);
    }

    public LookupSourceSupplier createLookupSourceSupplier(
            Session session,
            List<Integer> joinChannels,
            OptionalInt hashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories)
    {
        return createLookupSourceSupplier(session, joinChannels, hashChannel, filterFunctionFactory, sortChannel, searchFunctionFactories, Optional.empty());
    }

    public PagesSpatialIndexSupplier createPagesSpatialIndex(
            Session session,
            int geometryChannel,
            Optional<Integer> radiusChannel,
            Optional<Integer> partitionChannel,
            SpatialPredicate spatialRelationshipTest,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            List<Integer> outputChannels,
            Map<Integer, Rectangle> partitions,
            LocalMemoryContext localUserMemoryContext)
    {
        // TODO probably shouldn't copy to reduce memory and for memory accounting's sake
        List<List<Block>> channels = ImmutableList.copyOf(this.channels);
        return new PagesSpatialIndexSupplier(session, valueAddresses, positionCount, types, outputChannels, channels, geometryChannel, radiusChannel, partitionChannel, spatialRelationshipTest, filterFunctionFactory, partitions, localUserMemoryContext);
    }

    public LookupSourceSupplier createLookupSourceSupplier(
            Session session,
            List<Integer> joinChannels,
            OptionalInt hashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories,
            Optional<List<Integer>> outputChannels)
    {
        List<List<Block>> channels = ImmutableList.copyOf(this.channels);
        if (!joinChannels.isEmpty()) {
            // todo compiled implementation of lookup join does not support when we are joining with empty join channels.
            // This code path will trigger only for OUTER joins. To fix that we need to add support for
            //        OUTER joins into NestedLoopsJoin and remove "type == INNER" condition in LocalExecutionPlanner.visitJoin()

            try {
                LookupSourceSupplierFactory lookupSourceFactory = joinCompiler.compileLookupSourceFactory(types, joinChannels, sortChannel, outputChannels);
                return lookupSourceFactory.createLookupSourceSupplier(
                        session,
                        valueAddresses,
                        positionCount,
                        channels,
                        hashChannel,
                        filterFunctionFactory,
                        sortChannel,
                        searchFunctionFactories);
            }
            catch (Exception e) {
                log.error(e, "Lookup source compile failed for types=%s error=%s", types, e);
            }
        }

        // if compilation fails
        PagesHashStrategy hashStrategy = new SimplePagesHashStrategy(
                types,
                outputChannels.orElseGet(() -> rangeList(types.size())),
                channels,
                joinChannels,
                hashChannel,
                sortChannel,
                functionAndTypeManager);

        return new JoinHashSupplier(
                session,
                hashStrategy,
                valueAddresses,
                positionCount,
                channels,
                filterFunctionFactory,
                sortChannel,
                searchFunctionFactories);
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

    public Iterator<Page> getPages()
    {
        return new AbstractIterator<Page>()
        {
            private int pageCounter;

            @Override
            protected Page computeNext()
            {
                if (pageCounter == channels[0].size()) {
                    return endOfData();
                }

                Block[] blocks = Stream.of(channels)
                        .map(channel -> channel.get(pageCounter))
                        .toArray(Block[]::new);
                pageCounter++;
                return new Page(blocks);
            }
        };
    }

    public Iterator<Page> getSortedPages()
    {
        return getSortedPagesFromRange(0, positionCount);
    }

    /**
     * Get sorted pages from the specified section of the PagesIndex.
     *
     * @param start start position of the section, inclusive
     * @param end end position of the section, exclusive
     * @return iterator of pages
     */
    public Iterator<Page> getSortedPages(int start, int end)
    {
        checkArgument(start >= 0 && end <= positionCount, "position range out of bounds");
        checkArgument(start <= end, "invalid position range");
        return getSortedPagesFromRange(start, end);
    }

    private Iterator<Page> getSortedPagesFromRange(int start, int end)
    {
        return new AbstractIterator<Page>()
        {
            private int currentPosition = start;
            private final PageBuilder pageBuilder = new PageBuilder(types);
            private final int[] outputChannels = new int[types.size()];

            {
                Arrays.setAll(outputChannels, IntUnaryOperator.identity());
            }

            @Override
            public Page computeNext()
            {
                currentPosition = buildPage(currentPosition, end, outputChannels, pageBuilder);
                if (pageBuilder.isEmpty()) {
                    return endOfData();
                }
                Page page = pageBuilder.build();
                pageBuilder.reset();
                return page;
            }
        };
    }
}
