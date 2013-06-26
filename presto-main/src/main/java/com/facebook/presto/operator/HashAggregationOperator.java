package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.aggregation.FixedWidthAggregationFunction;
import com.facebook.presto.operator.aggregation.VariableWidthAggregationFunction;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import it.unimi.dsi.fastutil.longs.Long2IntOpenCustomHashMap;
import it.unimi.dsi.fastutil.longs.LongHash.Strategy;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceOffset;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class HashAggregationOperator
        implements Operator
{
    private static final int LOOKUP_SLICE_INDEX = 0xFF_FF_FF_FF;

    private final Operator source;
    private final int groupByChannel;
    private final Step step;
    private final List<AggregationFunctionDefinition> functionDefinitions;
    private final List<TupleInfo> tupleInfos;
    private final int expectedGroups;
    private final TaskMemoryManager taskMemoryManager;

    public HashAggregationOperator(Operator source,
            int groupByChannel,
            Step step,
            List<AggregationFunctionDefinition> functionDefinitions,
            int expectedGroups,
            TaskMemoryManager taskMemoryManager)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkArgument(groupByChannel >= 0, "groupByChannel is negative");
        Preconditions.checkNotNull(step, "step is null");
        Preconditions.checkNotNull(functionDefinitions, "functionDefinitions is null");
        Preconditions.checkNotNull(taskMemoryManager, "taskMemoryManager is null");

        this.source = source;
        this.groupByChannel = groupByChannel;
        this.step = step;
        this.functionDefinitions = ImmutableList.copyOf(functionDefinitions);
        this.expectedGroups = expectedGroups;
        this.taskMemoryManager = taskMemoryManager;

        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        tupleInfos.add(source.getTupleInfos().get(groupByChannel));
        for (AggregationFunctionDefinition functionDefinition : functionDefinitions) {
            if (step != Step.PARTIAL) {
                tupleInfos.add(functionDefinition.getFunction().getFinalTupleInfo());
            }
            else {
                tupleInfos.add(functionDefinition.getFunction().getIntermediateTupleInfo());
            }
        }
        this.tupleInfos = tupleInfos.build();
    }

    @Override
    public int getChannelCount()
    {
        return tupleInfos.size();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        return new HashAggregationIterator(tupleInfos, source, groupByChannel, step, expectedGroups, functionDefinitions, taskMemoryManager, operatorStats);
    }

    private static class HashAggregationIterator
            extends AbstractPageIterator
    {
        private final PageIterator iterator;
        private final List<AggregationFunctionDefinition> functionDefinitions;
        private final int groupChannel;
        private final TaskMemoryManager taskMemoryManager;
        private final TupleInfo groupByTupleInfo;
        private final Step step;
        private final int expectedGroups;

        private Iterator<Page> outputIterator;

        private long currentMemoryReservation;

        public HashAggregationIterator(List<TupleInfo> tupleInfos,
                Operator source,
                int groupChannel,
                Step step,
                int expectedGroups,
                List<AggregationFunctionDefinition> functionDefinitions,
                TaskMemoryManager taskMemoryManager,
                OperatorStats operatorStats)
        {
            super(tupleInfos);

            checkNotNull(source, "source is null");
            checkNotNull(operatorStats, "operatorStats is null");

            this.iterator = source.iterator(operatorStats);
            this.functionDefinitions = functionDefinitions;
            this.step = step;
            this.groupChannel = groupChannel;
            this.taskMemoryManager = checkNotNull(taskMemoryManager, "maxSize is null");
            this.expectedGroups = expectedGroups;
            this.groupByTupleInfo = iterator.getTupleInfos().get(groupChannel);
        }

        public Iterator<Page> aggregate()
        {
            if (!iterator.hasNext()) {
                return ImmutableList.<Page>of().iterator();
            }

            // wrapper each function with an aggregator
            ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();
            for (AggregationFunctionDefinition functionDefinition : checkNotNull(functionDefinitions, "functionDefinitions is null")) {
                builder.add(createAggregator(functionDefinition, step, expectedGroups));
            }
            final List<Aggregator> aggregates = builder.build();

            SliceHashStrategy hashStrategy = new SliceHashStrategy(groupByTupleInfo);
            Long2IntOpenCustomHashMap addressToGroupId = new Long2IntOpenCustomHashMap(expectedGroups, hashStrategy);
            addressToGroupId.defaultReturnValue(-1);

            // allocate the first group by (key side) slice
            Slice slice = Slices.allocate((int) BlockBuilder.DEFAULT_MAX_BLOCK_SIZE.toBytes());
            hashStrategy.addSlice(slice);
            BlockBuilder blockBuilder = new BlockBuilder(groupByTupleInfo, slice.length(), slice.getOutput());

            int nextGroupId = 0;

            List<UncompressedBlock> groupByBlocks = new ArrayList<>();
            BlockCursor[] cursors = new BlockCursor[iterator.getChannelCount()];
            while (!isMaxMemoryExceeded(hashStrategy, aggregates) && iterator.hasNext()) {
                Page page = iterator.next();
                Block[] blocks = page.getBlocks();
                Slice groupBySlice = ((UncompressedBlock) blocks[groupChannel]).getSlice();
                hashStrategy.setLookupSlice(groupBySlice);

                for (int i = 0; i < blocks.length; i++) {
                    cursors[i] = blocks[i].cursor();
                }

                int rows = page.getPositionCount();
                for (int position = 0; position < rows; position++) {
                    for (BlockCursor cursor : cursors) {
                        checkState(cursor.advanceNextPosition());
                    }

                    // lookup the group id (row number of the key)
                    int rawOffset = cursors[groupChannel].getRawOffset();
                    int groupId = addressToGroupId.get(encodeSyntheticAddress(LOOKUP_SLICE_INDEX, rawOffset));
                    if (groupId < 0) {
                        // new group

                        // copy group by tuple (key) to hash
                        int length = groupByTupleInfo.size(groupBySlice, rawOffset);
                        if (blockBuilder.writableBytes() < length) {
                            UncompressedBlock block = blockBuilder.build();
                            groupByBlocks.add(block);
                            slice = Slices.allocate(Math.max((int) BlockBuilder.DEFAULT_MAX_BLOCK_SIZE.toBytes(), length));
                            blockBuilder = new BlockBuilder(groupByTupleInfo, slice.length(), slice.getOutput());
                            hashStrategy.addSlice(slice);
                        }
                        int groupByValueRawOffset = blockBuilder.size();
                        blockBuilder.appendTuple(groupBySlice, rawOffset, length);

                        // record group id in hash
                        groupId = nextGroupId++;
                        addressToGroupId.put(encodeSyntheticAddress(groupByBlocks.size(), groupByValueRawOffset), groupId);

                        // initialize the value
                        for (Aggregator aggregate : aggregates) {
                            aggregate.initialize(groupId);
                        }
                    }

                    // process the row
                    for (Aggregator aggregate : aggregates) {
                        aggregate.addValue(cursors, groupId);
                    }
                }

                for (BlockCursor cursor : cursors) {
                    checkState(!cursor.advanceNextPosition());
                }
            }

            // only partial aggregations can flush early
            Preconditions.checkState(step == Step.PARTIAL || !isMaxMemoryExceeded(hashStrategy, aggregates),
                    "Task exceeded max memory size of %s", taskMemoryManager.getMaxMemorySize());

            // Only partial aggregation can flush early. Also, check that we are not flushing tiny bits at a time
            checkState(step == Step.PARTIAL || !isMaxMemoryExceeded(hashStrategy, aggregates), "Task exceeded max memory size of %s", taskMemoryManager.getMaxMemorySize());

            // add the last block if it is not empty
            if (!blockBuilder.isEmpty()) {
                UncompressedBlock block = blockBuilder.build();
                groupByBlocks.add(block);
            }

            return Iterators.transform(groupByBlocks.iterator(), new Function<UncompressedBlock, Page>()
            {
                private int currentPosition = 0;

                @Override
                public Page apply(UncompressedBlock groupByBlock)
                {
                    // build  the page channel at at time
                    Block[] blocks = new Block[aggregates.size() + 1];
                    blocks[0] = groupByBlock;
                    int pagePositionCount = blocks[0].getPositionCount();
                    for (int channel = 1; channel < aggregates.size() + 1; channel++) {
                        Aggregator aggregator = aggregates.get(channel - 1);
                        // todo there is no need to eval for intermediates since buffer is already in block form
                        BlockBuilder blockBuilder = new BlockBuilder(aggregator.getTupleInfo());
                        for (int position = 0; position < pagePositionCount; position++) {
                            aggregator.evaluate(currentPosition + position, blockBuilder);
                        }
                        blocks[channel] = blockBuilder.build();
                    }

                    Page page = new Page(blocks);
                    currentPosition += pagePositionCount;
                    return page;
                }
            });
        }

        private boolean isMaxMemoryExceeded(SliceHashStrategy hashStrategy, List<Aggregator> aggregates)
        {
            long memorySize = hashStrategy.getEstimatedSize();
            for (Aggregator aggregate : aggregates) {
                memorySize += aggregate.getEstimatedSize();
            }

            // remove the pre-allocated memory from this size
            memorySize -= taskMemoryManager.getOperatorPreAllocatedMemory().toBytes();

            long delta = memorySize - currentMemoryReservation;
            if (delta <= 0) {
                return false;
            }

            if (!taskMemoryManager.reserveBytes(delta)) {
                return true;
            }

            // reservation worked, record the reservation
            currentMemoryReservation = Math.max(currentMemoryReservation, memorySize);
            return false;
        }

        @Override
        protected Page computeNext()
        {
            if (outputIterator == null || !outputIterator.hasNext()) {
                outputIterator = aggregate();
            }

            if (!outputIterator.hasNext()) {
                return endOfData();
            }
            return outputIterator.next();
        }

        @Override
        protected void doClose()
        {
            iterator.close();
        }
    }

    @SuppressWarnings("rawtypes")
    private static Aggregator createAggregator(AggregationFunctionDefinition functionDefinition, Step step, int expectedGroups)
    {
        AggregationFunction function = functionDefinition.getFunction();
        if (function instanceof VariableWidthAggregationFunction) {
            return new VariableWidthAggregator((VariableWidthAggregationFunction) functionDefinition.getFunction(), functionDefinition.getInputs(), step, expectedGroups);
        }
        else {
            Input input = null;
            if (!functionDefinition.getInputs().isEmpty()) {
                input = Iterables.getOnlyElement(functionDefinition.getInputs());
            }

            return new FixedWidthAggregator((FixedWidthAggregationFunction) functionDefinition.getFunction(), input, step);
        }
    }

    private interface Aggregator
    {
        long getEstimatedSize();

        TupleInfo getTupleInfo();

        void initialize(int position);

        void addValue(BlockCursor[] cursors, int position);

        void evaluate(int position, BlockBuilder output);
    }

    private static class FixedWidthAggregator
            implements Aggregator
    {
        private final FixedWidthAggregationFunction function;
        private final Input input;
        private final Step step;
        private final int fixedWidthSize;
        private final int sliceSize;
        private final List<Slice> slices = new ArrayList<>();
        private int currentMaxPosition;

        private FixedWidthAggregator(FixedWidthAggregationFunction function, Input input, Step step)
        {
            this.function = function;
            this.input = input;
            this.step = step;
            this.fixedWidthSize = this.function.getFixedSize();
            this.sliceSize = (int) (BlockBuilder.DEFAULT_MAX_BLOCK_SIZE.toBytes() / fixedWidthSize) * fixedWidthSize;
            Slice slice = Slices.allocate(sliceSize);
            slices.add(slice);
            currentMaxPosition = sliceSize / fixedWidthSize;
        }

        @Override
        public long getEstimatedSize()
        {
            return slices.size() * sliceSize;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            // if this is a partial, the output is an intermediate value
            if (step == Step.PARTIAL) {
                return function.getIntermediateTupleInfo();
            }
            else {
                return function.getFinalTupleInfo();
            }
        }

        @Override
        public void initialize(int position)
        {
            // add more slices if necessary
            while (position >= currentMaxPosition) {
                Slice slice = Slices.allocate(sliceSize);
                slices.add(slice);
                currentMaxPosition += sliceSize / fixedWidthSize;
            }

            int globalOffset = position * fixedWidthSize;

            int sliceIndex = globalOffset / sliceSize; // todo do this with shifts?
            Slice slice = slices.get(sliceIndex);
            int sliceOffset = globalOffset - (sliceIndex * sliceSize);
            function.initialize(slice, sliceOffset);
        }

        @Override
        public void addValue(BlockCursor[] cursors, int position)
        {
            BlockCursor cursor;
            int field = -1;
            if (input != null) {
                cursor = cursors[input.getChannel()];
                field = input.getField();
            }
            else {
                cursor = null;
            }

            int globalOffset = position * fixedWidthSize;

            int sliceIndex = globalOffset / sliceSize; // todo do this with shifts?
            Slice slice = slices.get(sliceIndex);
            int sliceOffset = globalOffset - (sliceIndex * sliceSize);

            // if this is a final aggregation, the input is an intermediate value
            if (step == Step.FINAL) {
                function.addIntermediate(cursor, field, slice, sliceOffset);
            }
            else {
                function.addInput(cursor, field, slice, sliceOffset);
            }
        }

        @Override
        public void evaluate(int position, BlockBuilder output)
        {
            int offset = position * fixedWidthSize;

            int sliceIndex = offset / sliceSize; // todo do this with shifts
            Slice slice = slices.get(sliceIndex);
            int sliceOffset = offset - (sliceIndex * sliceSize);

            // if this is a partial, the output is an intermediate value
            if (step == Step.PARTIAL) {
                function.evaluateIntermediate(slice, sliceOffset, output);
            }
            else {
                function.evaluateFinal(slice, sliceOffset, output);
            }
        }
    }

    private static class VariableWidthAggregator<T>
            implements Aggregator
    {
        private final VariableWidthAggregationFunction<T> function;
        private final List<Input> inputs;
        private final Step step;
        private final ObjectArrayList<T> intermediateValues;
        private long totalElementSizeInBytes;

        private final BlockCursor[] blockCursors;
        private final int[] fields;

        private VariableWidthAggregator(VariableWidthAggregationFunction<T> function, List<Input> inputs, Step step, int expectedGroups)
        {
            this.function = function;
            this.inputs = inputs;
            this.step = step;
            this.intermediateValues = new ObjectArrayList<>(expectedGroups);

            this.blockCursors = new BlockCursor[inputs.size()];
            this.fields = new int[inputs.size()];

            for (int i = 0; i < fields.length; i++) {
                fields[i] = inputs.get(i).getField();
            }
        }

        @Override
        public long getEstimatedSize()
        {
            return SizeOf.sizeOf(intermediateValues.elements()) + totalElementSizeInBytes;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            // if this is a partial, the output is an intermediate value
            if (step == Step.PARTIAL) {
                return function.getIntermediateTupleInfo();
            }
            else {
                return function.getFinalTupleInfo();
            }
        }

        @Override
        public void initialize(int position)
        {
            Preconditions.checkState(position == intermediateValues.size(), "expected array to grow by 1");
            intermediateValues.add(function.initialize());
        }

        @Override
        public void addValue(BlockCursor[] cursors, int position)
        {
            for (int i = 0; i < blockCursors.length; i++) {
                blockCursors[i] = cursors[inputs.get(i).getChannel()];
            }

            // if this is a final aggregation, the input is an intermediate value
            T oldValue = intermediateValues.get(position);
            long oldSize = 0;
            if (oldValue != null) {
                oldSize = function.estimateSizeInBytes(oldValue);
            }

            T newValue;
            if (step == Step.FINAL) {
                newValue = function.addIntermediate(blockCursors, fields, oldValue);
            }
            else {
                newValue = function.addInput(blockCursors, fields, oldValue);
            }
            intermediateValues.set(position, newValue);

            long newSize = 0;
            if (newValue != null) {
                newSize = function.estimateSizeInBytes(newValue);
            }
            totalElementSizeInBytes += newSize - oldSize;

        }

        @Override
        public void evaluate(int position, BlockBuilder output)
        {
            T value = intermediateValues.get(position);
            // if this is a partial, the output is an intermediate value
            if (step == Step.PARTIAL) {
                function.evaluateIntermediate(value, output);
            }
            else {
                function.evaluateFinal(value, output);
            }
        }
    }

    public static class SliceHashStrategy
            implements Strategy
    {
        private final TupleInfo tupleInfo;
        private final List<Slice> slices;
        private Slice lookupSlice;
        private long memorySize;

        public SliceHashStrategy(TupleInfo tupleInfo)
        {
            this.tupleInfo = tupleInfo;
            this.slices = ObjectArrayList.wrap(new Slice[1024], 0);
        }

        public long getEstimatedSize()
        {
            return memorySize;
        }

        public void setLookupSlice(Slice lookupSlice)
        {
            this.lookupSlice = lookupSlice;
        }

        public void addSlice(Slice slice)
        {
            memorySize += slice.length();
            slices.add(slice);
        }

        @Override
        public int hashCode(long sliceAddress)
        {
            Slice slice = getSliceForSyntheticAddress(sliceAddress);
            int offset = (int) sliceAddress;
            int length = tupleInfo.size(slice, offset);
            int hashCode = slice.hashCode(offset, length);
            return hashCode;
        }

        @Override
        public boolean equals(long leftSliceAddress, long rightSliceAddress)
        {
            Slice leftSlice = getSliceForSyntheticAddress(leftSliceAddress);
            int leftOffset = decodeSliceOffset(leftSliceAddress);
            int leftLength = tupleInfo.size(leftSlice, leftOffset);

            Slice rightSlice = getSliceForSyntheticAddress(rightSliceAddress);
            int rightOffset = decodeSliceOffset(rightSliceAddress);
            int rightLength = tupleInfo.size(rightSlice, rightOffset);

            return leftSlice.equals(leftOffset, leftLength, rightSlice, rightOffset, rightLength);

        }

        private Slice getSliceForSyntheticAddress(long sliceAddress)
        {
            int sliceIndex = decodeSliceIndex(sliceAddress);
            Slice slice;
            if (sliceIndex == LOOKUP_SLICE_INDEX) {
                slice = lookupSlice;
            }
            else {
                slice = slices.get(sliceIndex);
            }
            return slice;
        }
    }
}
