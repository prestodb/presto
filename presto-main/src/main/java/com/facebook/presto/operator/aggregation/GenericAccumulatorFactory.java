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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.Session;
import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.ArrayBlock;
import com.facebook.presto.common.block.ArrayBlockBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.ColumnarArray;
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.block.RowBlock;
import com.facebook.presto.common.block.RowBlockBuilder;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.MarkDistinctHash;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.UpdateMemory;
import com.facebook.presto.operator.Work;
import com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.spi.function.WindowIndex;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.block.ColumnarArray.toColumnarArray;
import static com.facebook.presto.common.block.ColumnarRow.toColumnarRow;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Long.max;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public class GenericAccumulatorFactory
        implements AccumulatorFactory
{
    private final List<AccumulatorStateDescriptor> stateDescriptors;
    private final Constructor<? extends Accumulator> accumulatorConstructor;
    private final Constructor<? extends GroupedAccumulator> groupedAccumulatorConstructor;
    private final List<LambdaProvider> lambdaProviders;
    private final Optional<Integer> maskChannel;
    private final List<Integer> inputChannels;
    private final List<Type> sourceTypes;
    private final List<Integer> orderByChannels;
    private final List<SortOrder> orderings;

    @Nullable
    private final JoinCompiler joinCompiler;

    @Nullable
    private final Session session;
    private final boolean distinct;
    private final boolean spillEnabled;
    private final PagesIndex.Factory pagesIndexFactory;

    public GenericAccumulatorFactory(
            List<AccumulatorStateDescriptor> stateDescriptors,
            Constructor<? extends Accumulator> accumulatorConstructor,
            Constructor<? extends GroupedAccumulator> groupedAccumulatorConstructor,
            List<LambdaProvider> lambdaProviders,
            List<Integer> inputChannels,
            Optional<Integer> maskChannel,
            List<Type> sourceTypes,
            List<Integer> orderByChannels,
            List<SortOrder> orderings,
            PagesIndex.Factory pagesIndexFactory,
            JoinCompiler joinCompiler,
            Session session,
            boolean distinct,
            boolean spillEnabled)
    {
        this.stateDescriptors = requireNonNull(stateDescriptors, "stateDescriptors is null");
        this.accumulatorConstructor = requireNonNull(accumulatorConstructor, "accumulatorConstructor is null");
        this.groupedAccumulatorConstructor = requireNonNull(groupedAccumulatorConstructor, "groupedAccumulatorConstructor is null");
        this.lambdaProviders = ImmutableList.copyOf(requireNonNull(lambdaProviders, "lambdaProviders is null"));
        this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");
        this.inputChannels = ImmutableList.copyOf(requireNonNull(inputChannels, "inputChannels is null"));
        this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
        this.orderByChannels = ImmutableList.copyOf(requireNonNull(orderByChannels, "orderByChannels is null"));
        this.orderings = ImmutableList.copyOf(requireNonNull(orderings, "orderings is null"));
        checkArgument(orderByChannels.isEmpty() || !isNull(pagesIndexFactory), "No pagesIndexFactory to process ordering");
        this.pagesIndexFactory = pagesIndexFactory;

        checkArgument(!distinct || !isNull(session) && !isNull(joinCompiler), "joinCompiler and session needed when distinct is true");
        this.joinCompiler = joinCompiler;
        this.session = session;
        this.distinct = distinct;
        this.spillEnabled = spillEnabled;
    }

    @Override
    public List<Integer> getInputChannels()
    {
        return inputChannels;
    }

    @Override
    public Accumulator createAccumulator()
    {
        Accumulator accumulator;

        if (hasDistinct()) {
            // channel 0 will contain the distinct mask
            accumulator = instantiateAccumulator(
                    inputChannels.stream()
                            .map(value -> value + 1)
                            .collect(Collectors.toList()),
                    Optional.of(0));

            List<Type> argumentTypes = inputChannels.stream()
                    .map(sourceTypes::get)
                    .collect(Collectors.toList());

            accumulator = new DistinctingAccumulator(accumulator, argumentTypes, inputChannels, maskChannel, session, joinCompiler);
        }
        else {
            accumulator = instantiateAccumulator(inputChannels, maskChannel);
        }

        if (orderByChannels.isEmpty()) {
            return accumulator;
        }

        return new OrderingAccumulator(accumulator, sourceTypes, orderByChannels, orderings, pagesIndexFactory);
    }

    @Override
    public Accumulator createIntermediateAccumulator()
    {
        try {
            return accumulatorConstructor.newInstance(stateDescriptors, ImmutableList.of(), Optional.empty(), lambdaProviders);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public GroupedAccumulator createGroupedAccumulator()
    {
        GroupedAccumulator accumulator = createGenericGroupedAccumulator();
        if (!spillEnabled || (!hasDistinct() && !hasOrderBy())) {
            return accumulator;
        }

        checkState(accumulator instanceof FinalOnlyGroupedAccumulator);
        return new SpillableFinalOnlyGroupedAccumulator(sourceTypes, (FinalOnlyGroupedAccumulator) accumulator);
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAccumulator()
    {
        if (!hasOrderBy() && !hasDistinct()) {
            try {
                return groupedAccumulatorConstructor.newInstance(stateDescriptors, ImmutableList.of(), Optional.empty(), lambdaProviders);
            }
            catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
        return createGroupedAccumulator();
    }

    @Override
    public boolean hasOrderBy()
    {
        return !orderByChannels.isEmpty();
    }

    @Override
    public boolean hasDistinct()
    {
        return distinct;
    }

    private GroupedAccumulator createGenericGroupedAccumulator()
    {
        GroupedAccumulator accumulator;

        if (hasDistinct()) {
            // channel 0 will contain the distinct mask
            accumulator = instantiateGroupedAccumulator(
                    inputChannels.stream()
                            .map(value -> value + 1)
                            .collect(Collectors.toList()),
                    Optional.of(0));

            List<Type> argumentTypes = new ArrayList<>();
            for (int input : inputChannels) {
                argumentTypes.add(sourceTypes.get(input));
            }

            accumulator = new DistinctingGroupedAccumulator(accumulator, argumentTypes, inputChannels, maskChannel, session, joinCompiler);
        }
        else {
            accumulator = instantiateGroupedAccumulator(inputChannels, maskChannel);
        }

        if (orderByChannels.isEmpty()) {
            return accumulator;
        }

        return new OrderingGroupedAccumulator(accumulator, sourceTypes, orderByChannels, orderings, pagesIndexFactory);
    }

    private Accumulator instantiateAccumulator(List<Integer> inputs, Optional<Integer> mask)
    {
        try {
            return accumulatorConstructor.newInstance(stateDescriptors, inputs, mask, lambdaProviders);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private GroupedAccumulator instantiateGroupedAccumulator(List<Integer> inputs, Optional<Integer> mask)
    {
        try {
            return groupedAccumulatorConstructor.newInstance(stateDescriptors, inputs, mask, lambdaProviders);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private static class DistinctingAccumulator
            implements Accumulator
    {
        private final Accumulator accumulator;
        private final MarkDistinctHash hash;
        private final Optional<Integer> maskChannel;

        private DistinctingAccumulator(
                Accumulator accumulator,
                List<Type> inputTypes,
                List<Integer> inputs,
                Optional<Integer> maskChannel,
                Session session,
                JoinCompiler joinCompiler)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");

            hash = new MarkDistinctHash(session, inputTypes, Ints.toArray(inputs), Optional.empty(), joinCompiler, UpdateMemory.NOOP);
        }

        @Override
        public long getEstimatedSize()
        {
            return hash.getEstimatedSize() + accumulator.getEstimatedSize();
        }

        @Override
        public Type getFinalType()
        {
            return accumulator.getFinalType();
        }

        @Override
        public Type getIntermediateType()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addInput(Page page)
        {
            // 1. filter out positions based on mask, if present
            Page filtered = maskChannel
                    .map(channel -> filter(page, page.getBlock(channel)))
                    .orElse(page);

            if (filtered.getPositionCount() == 0) {
                return;
            }

            // 2. compute a distinct mask
            Work<Block> work = hash.markDistinctRows(filtered);
            checkState(work.process());
            Block distinctMask = work.getResult();

            // 3. feed a Page with a new mask to the underlying aggregation
            accumulator.addInput(filtered.prependColumn(distinctMask));
        }

        @Override
        public void addInput(WindowIndex index, List<Integer> channels, int startPosition, int endPosition)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addIntermediate(Block block)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateIntermediate(BlockBuilder blockBuilder)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateFinal(BlockBuilder blockBuilder)
        {
            accumulator.evaluateFinal(blockBuilder);
        }
    }

    private static Page filter(Page page, Block mask)
    {
        int[] ids = new int[mask.getPositionCount()];
        int next = 0;
        for (int i = 0; i < page.getPositionCount(); ++i) {
            if (BOOLEAN.getBoolean(mask, i)) {
                ids[next++] = i;
            }
        }

        return page.getPositions(ids, 0, next);
    }

    private static class DistinctingGroupedAccumulator
            extends FinalOnlyGroupedAccumulator
    {
        private final GroupedAccumulator accumulator;
        private final MarkDistinctHash hash;
        private final Optional<Integer> maskChannel;

        private DistinctingGroupedAccumulator(
                GroupedAccumulator accumulator,
                List<Type> inputTypes,
                List<Integer> inputChannels,
                Optional<Integer> maskChannel,
                Session session,
                JoinCompiler joinCompiler)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");

            List<Type> types = ImmutableList.<Type>builder()
                    .add(BIGINT) // group id column
                    .addAll(inputTypes)
                    .build();

            int[] inputs = new int[inputChannels.size() + 1];
            inputs[0] = 0; // we'll use the first channel for group id column
            for (int i = 0; i < inputChannels.size(); i++) {
                inputs[i + 1] = inputChannels.get(i) + 1;
            }

            hash = new MarkDistinctHash(session, types, inputs, Optional.empty(), joinCompiler, UpdateMemory.NOOP);
        }

        @Override
        public long getEstimatedSize()
        {
            return hash.getEstimatedSize() + accumulator.getEstimatedSize();
        }

        @Override
        public Type getFinalType()
        {
            return accumulator.getFinalType();
        }

        @Override
        public void addInput(GroupByIdBlock groupIdsBlock, Page page)
        {
            Page withGroup = page.prependColumn(groupIdsBlock);

            // 1. filter out positions based on mask, if present
            Page filtered = maskChannel
                    .map(channel -> filter(withGroup, withGroup.getBlock(channel + 1))) // offset by one because of group id in column 0
                    .orElse(withGroup);

            // 2. compute a mask for the distinct rows (including the group id)
            Work<Block> work = hash.markDistinctRows(filtered);
            checkState(work.process());
            Block distinctMask = work.getResult();

            // 3. feed a Page with a new mask to the underlying aggregation
            GroupByIdBlock groupIds = new GroupByIdBlock(groupIdsBlock.getGroupCount(), filtered.getBlock(0));

            // drop the group id column and prepend the distinct mask column
            Block[] columns = new Block[filtered.getChannelCount()];
            columns[0] = distinctMask;
            for (int i = 1; i < filtered.getChannelCount(); i++) {
                columns[i] = filtered.getBlock(i);
            }

            accumulator.addInput(groupIds, new Page(filtered.getPositionCount(), columns));
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            accumulator.evaluateFinal(groupId, output);
        }

        @Override
        public void prepareFinal()
        {
        }
    }

    private static class OrderingAccumulator
            implements Accumulator
    {
        private final Accumulator accumulator;
        private final List<Integer> orderByChannels;
        private final List<SortOrder> orderings;
        private final PagesIndex pagesIndex;

        private OrderingAccumulator(
                Accumulator accumulator,
                List<Type> aggregationSourceTypes,
                List<Integer> orderByChannels,
                List<SortOrder> orderings,
                PagesIndex.Factory pagesIndexFactory)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            this.orderByChannels = ImmutableList.copyOf(requireNonNull(orderByChannels, "orderByChannels is null"));
            this.orderings = ImmutableList.copyOf(requireNonNull(orderings, "orderings is null"));
            this.pagesIndex = pagesIndexFactory.newPagesIndex(aggregationSourceTypes, 10_000);
        }

        @Override
        public long getEstimatedSize()
        {
            return pagesIndex.getEstimatedSize().toBytes() + accumulator.getEstimatedSize();
        }

        @Override
        public Type getFinalType()
        {
            return accumulator.getFinalType();
        }

        @Override
        public Type getIntermediateType()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addInput(Page page)
        {
            pagesIndex.addPage(page);
        }

        @Override
        public void addInput(WindowIndex index, List<Integer> channels, int startPosition, int endPosition)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addIntermediate(Block block)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateIntermediate(BlockBuilder blockBuilder)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateFinal(BlockBuilder blockBuilder)
        {
            pagesIndex.sort(orderByChannels, orderings);
            Iterator<Page> pagesIterator = pagesIndex.getSortedPages();
            pagesIterator.forEachRemaining(accumulator::addInput);
            accumulator.evaluateFinal(blockBuilder);
        }
    }

    private static class OrderingGroupedAccumulator
            extends FinalOnlyGroupedAccumulator
    {
        private final GroupedAccumulator accumulator;
        private final List<Integer> orderByChannels;
        private final List<SortOrder> orderings;
        private final PagesIndex pagesIndex;
        private long groupCount;

        private OrderingGroupedAccumulator(
                GroupedAccumulator accumulator,
                List<Type> aggregationSourceTypes,
                List<Integer> orderByChannels,
                List<SortOrder> orderings,
                PagesIndex.Factory pagesIndexFactory)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            requireNonNull(aggregationSourceTypes, "aggregationSourceTypes is null");
            this.orderByChannels = ImmutableList.copyOf(requireNonNull(orderByChannels, "orderByChannels is null"));
            this.orderings = ImmutableList.copyOf(requireNonNull(orderings, "orderings is null"));
            List<Type> pageIndexTypes = new ArrayList<>(aggregationSourceTypes);
            // Add group id column
            pageIndexTypes.add(BIGINT);
            this.pagesIndex = pagesIndexFactory.newPagesIndex(pageIndexTypes, 10_000);
            this.groupCount = 0;
        }

        @Override
        public long getEstimatedSize()
        {
            return pagesIndex.getEstimatedSize().toBytes() + accumulator.getEstimatedSize();
        }

        @Override
        public Type getFinalType()
        {
            return accumulator.getFinalType();
        }

        @Override
        public void addInput(GroupByIdBlock groupIdsBlock, Page page)
        {
            groupCount = max(groupCount, groupIdsBlock.getGroupCount());
            // Add group id block
            pagesIndex.addPage(page.appendColumn(groupIdsBlock));
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            accumulator.evaluateFinal(groupId, output);
        }

        @Override
        public void prepareFinal()
        {
            pagesIndex.sort(orderByChannels, orderings);
            Iterator<Page> pagesIterator = pagesIndex.getSortedPages();
            pagesIterator.forEachRemaining(page -> {
                // The last channel of the page is the group id
                GroupByIdBlock groupIds = new GroupByIdBlock(groupCount, page.getBlock(page.getChannelCount() - 1));
                // We pass group id together with the other input channels to accumulator. Accumulator knows which input channels
                // to use. Since we did not change the order of original input channels, passing the group id is safe.
                accumulator.addInput(groupIds, page);
            });
        }
    }

    /**
     * {@link SpillableFinalOnlyGroupedAccumulator} enables spilling for {@link FinalOnlyGroupedAccumulator}
     */
    private static class SpillableFinalOnlyGroupedAccumulator
            implements GroupedAccumulator
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SpillableFinalOnlyGroupedAccumulator.class).instanceSize();

        private final FinalOnlyGroupedAccumulator delegate;
        private final List<Type> spillingTypes;

        private ObjectBigArray<GroupIdPage> rawInputs = new ObjectBigArray<>();
        private ObjectBigArray<RowBlockBuilder> blockBuilders;
        private long rawInputsSizeInBytes;
        private long blockBuildersSizeInBytes;
        private long rawInputsLength;

        public SpillableFinalOnlyGroupedAccumulator(List<Type> types, FinalOnlyGroupedAccumulator delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.spillingTypes = requireNonNull(types, "types is null");
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE +
                    delegate.getEstimatedSize() +
                    (rawInputs == null ? 0 : rawInputsSizeInBytes + rawInputs.sizeOf()) +
                    (blockBuilders == null ? 0 : blockBuildersSizeInBytes + blockBuilders.sizeOf());
        }

        @Override
        public Type getFinalType()
        {
            return delegate.getFinalType();
        }

        @Override
        public Type getIntermediateType()
        {
            return new ArrayType(RowType.anonymous(spillingTypes));
        }

        @Override
        public void addInput(GroupByIdBlock groupIdsBlock, Page page)
        {
            checkState(rawInputs != null && blockBuilders == null);
            rawInputs.ensureCapacity(rawInputsLength);
            GroupIdPage groupIdPage = new GroupIdPage(groupIdsBlock, page);
            rawInputsSizeInBytes += groupIdPage.getRetainedSizeInBytes();
            rawInputs.set(rawInputsLength, groupIdPage);
            // TODO(sakshams) deduplicate inputs for DISTINCT accumulator case by doing page compaction
            rawInputsLength++;
        }

        @Override
        public void addIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            checkState(rawInputs != null && blockBuilders == null);
            checkState(block instanceof ArrayBlock);
            ArrayBlock arrayBlock = (ArrayBlock) block;

            // expand array block back into page
            ColumnarArray columnarArray = toColumnarArray(block); // flattens the squashed arrays; so there is no need to flatten block again.
            ColumnarRow columnarRow = toColumnarRow(columnarArray.getElementsBlock()); // contains the flattened array
            int newPositionCount = columnarRow.getPositionCount(); // number of positions in expanded array (since columnarRow is already flattened)
            long[] newGroupIds = new long[newPositionCount];
            boolean[] nulls = new boolean[newPositionCount];
            int currentRowBlockIndex = 0;
            for (int groupIdPosition = 0; groupIdPosition < groupIdsBlock.getPositionCount(); groupIdPosition++) {
                for (int unused = 0; unused < arrayBlock.getBlock(groupIdPosition).getPositionCount(); unused++) {
                    // unused because we are expanding all the squashed values for the same group id
                    newGroupIds[currentRowBlockIndex] = groupIdsBlock.getGroupId(groupIdPosition);
                    nulls[currentRowBlockIndex] = groupIdsBlock.isNull(groupIdPosition);
                    currentRowBlockIndex++;
                }
            }

            Block[] blocks = new Block[spillingTypes.size()];
            for (int channel = 0; channel < spillingTypes.size(); channel++) {
                blocks[channel] = columnarRow.getField(channel);
            }
            Page page = new Page(blocks);
            GroupByIdBlock squashedGroupIds = new GroupByIdBlock(groupIdsBlock.getGroupCount(), new LongArrayBlock(newPositionCount, Optional.of(nulls), newGroupIds));

            rawInputs.ensureCapacity(rawInputsLength);
            GroupIdPage groupIdPage = new GroupIdPage(squashedGroupIds, page);
            rawInputsSizeInBytes += groupIdPage.getRetainedSizeInBytes();
            rawInputs.set(rawInputsLength, groupIdPage);
            rawInputsLength++;
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            checkState(output instanceof ArrayBlockBuilder);
            if (blockBuilders == null) {
                checkState(rawInputs != null);

                blockBuilders = new ObjectBigArray<>();
                for (int i = 0; i < rawInputsLength; i++) {
                    GroupIdPage groupIdPage = rawInputs.get(i);
                    Page page = groupIdPage.getPage();
                    GroupByIdBlock groupIdsBlock = groupIdPage.getGroupByIdBlock();
                    for (int position = 0; position < page.getPositionCount(); position++) {
                        long currentGroupId = groupIdsBlock.getGroupId(position);
                        blockBuilders.ensureCapacity(currentGroupId);

                        RowBlockBuilder rowBlockBuilder = blockBuilders.get(currentGroupId);
                        long currentRowBlockSizeInBytes = 0;
                        if (rowBlockBuilder == null) {
                            rowBlockBuilder = new RowBlockBuilder(spillingTypes, null, (int) groupIdsBlock.getGroupCount());
                        }
                        else {
                            currentRowBlockSizeInBytes = rowBlockBuilder.getRetainedSizeInBytes();
                        }

                        BlockBuilder currentOutput = rowBlockBuilder.beginBlockEntry();
                        for (int channel = 0; channel < spillingTypes.size(); channel++) {
                            spillingTypes.get(channel).appendTo(page.getBlock(channel), position, currentOutput);
                        }
                        rowBlockBuilder.closeEntry();

                        blockBuildersSizeInBytes += (rowBlockBuilder.getRetainedSizeInBytes() - currentRowBlockSizeInBytes);
                        blockBuilders.set(currentGroupId, rowBlockBuilder);
                    }
                    rawInputs.set(i, null);
                }
                rawInputs = null;
                rawInputsSizeInBytes = 0;
                rawInputsLength = 0;
            }

            BlockBuilder singleArrayBlockWriter = output.beginBlockEntry();
            checkState(rawInputs == null && blockBuilders != null);

            // We need to squash the entire page into one array block since we can't spill multiple values for a single group ID during evaluateIntermediate.
            RowBlock rowBlock = (RowBlock) blockBuilders.get(groupId).build();
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                singleArrayBlockWriter.appendStructure(rowBlock.getBlock(i));
            }
            output.closeEntry();

            // We only call evaluateIntermediate when it is time to spill. We never call evaluate intermediate twice for the same groupId.
            // This means we can null our reference to the groupId's corresponding blockBuilder to reduce memory usage
            blockBuilders.set(groupId, null);
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            checkState(rawInputs == null && blockBuilders == null);
            delegate.evaluateFinal(groupId, output);
        }

        @Override
        public void prepareFinal()
        {
            checkState(rawInputs != null && blockBuilders == null);
            for (int i = 0; i < rawInputsLength; i++) {
                GroupIdPage groupIdPage = rawInputs.get(i);
                delegate.addInput(groupIdPage.getGroupByIdBlock(), groupIdPage.getPage());
            }

            rawInputs = null;
            rawInputsSizeInBytes = 0;
            rawInputsLength = 0;
            delegate.prepareFinal();
        }

        private static class GroupIdPage
        {
            private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupIdPage.class).instanceSize();

            private final GroupByIdBlock groupByIdBlock;
            private final Page page;

            public GroupIdPage(GroupByIdBlock groupByIdBlock, Page page)
            {
                this.page = requireNonNull(page, "page is null");
                this.groupByIdBlock = requireNonNull(groupByIdBlock, "groupByIdBlock is null");
            }

            public Page getPage()
            {
                return page;
            }

            public GroupByIdBlock getGroupByIdBlock()
            {
                return groupByIdBlock;
            }

            public long getRetainedSizeInBytes()
            {
                return INSTANCE_SIZE + groupByIdBlock.getRetainedSizeInBytes() + page.getRetainedSizeInBytes();
            }
        }
    }
}
