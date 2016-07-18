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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.operator.GroupByHash.createGroupByHash;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TopNRowNumberOperator
        implements Operator
{
    public static class TopNRowNumberOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;

        private final List<Type> sourceTypes;

        private final List<Integer> outputChannels;
        private final List<Integer> partitionChannels;
        private final List<Type> partitionTypes;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private final int maxRowCountPerPartition;
        private final boolean partial;
        private final Optional<Integer> hashChannel;
        private final int expectedPositions;

        private final List<Type> types;
        private final List<Type> sortTypes;
        private final boolean generateRowNumber;
        private boolean closed;

        public TopNRowNumberOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<Integer> partitionChannels,
                List<? extends Type> partitionTypes,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                int maxRowCountPerPartition,
                boolean partial,
                Optional<Integer> hashChannel,
                int expectedPositions)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(sourceTypes);
            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.partitionChannels = ImmutableList.copyOf(requireNonNull(partitionChannels, "partitionChannels is null"));
            this.partitionTypes = ImmutableList.copyOf(requireNonNull(partitionTypes, "partitionTypes is null"));
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels));
            this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder));
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.partial = partial;
            checkArgument(maxRowCountPerPartition > 0, "maxRowCountPerPartition must be > 0");
            this.maxRowCountPerPartition = maxRowCountPerPartition;
            checkArgument(expectedPositions > 0, "expectedPositions must be > 0");
            this.generateRowNumber = !partial || !partitionChannels.isEmpty();
            this.expectedPositions = expectedPositions;

            this.types = toTypes(sourceTypes, outputChannels, generateRowNumber);
            ImmutableList.Builder<Type> sortTypes = ImmutableList.builder();
            for (int channel : sortChannels) {
                sortTypes.add(types.get(channel));
            }
            this.sortTypes = sortTypes.build();
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, TopNRowNumberOperator.class.getSimpleName());
            return new TopNRowNumberOperator(
                    operatorContext,
                    sourceTypes,
                    outputChannels,
                    partitionChannels,
                    partitionTypes,
                    sortChannels,
                    sortOrder,
                    sortTypes,
                    maxRowCountPerPartition,
                    generateRowNumber,
                    hashChannel,
                    expectedPositions);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TopNRowNumberOperatorFactory(operatorId, planNodeId, sourceTypes, outputChannels, partitionChannels, partitionTypes, sortChannels, sortOrder, maxRowCountPerPartition, partial, hashChannel, expectedPositions);
        }
    }

    private static final DataSize OVERHEAD_PER_VALUE = new DataSize(100, DataSize.Unit.BYTE); // for estimating in-memory size. This is a completely arbitrary number

    private final OperatorContext operatorContext;
    private boolean finishing;
    private final List<Type> types;
    private final int[] outputChannels;

    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;
    private final List<Type> sortTypes;
    private final boolean generateRowNumber;
    private final int maxRowCountPerPartition;

    private final Map<Long, PartitionBuilder> partitionRows;
    private Optional<FlushingPartition> flushingPartition;
    private final PageBuilder pageBuilder;
    private final Optional<GroupByHash> groupByHash;

    public TopNRowNumberOperator(
            OperatorContext operatorContext,
            List<? extends Type> sourceTypes,
            List<Integer> outputChannels,
            List<Integer> partitionChannels,
            List<Type> partitionTypes,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders,
            List<Type> sortTypes,
            int maxRowCountPerPartition,
            boolean generateRowNumber,
            Optional<Integer> hashChannel,
            int expectedPositions)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.outputChannels = Ints.toArray(requireNonNull(outputChannels, "outputChannels is null"));

        this.sortChannels = requireNonNull(sortChannels, "sortChannels is null");
        this.sortOrders = requireNonNull(sortOrders, "sortOrders is null");
        this.sortTypes = requireNonNull(sortTypes, "sortTypes is null");

        checkArgument(maxRowCountPerPartition > 0, "maxRowCountPerPartition must be > 0");
        this.maxRowCountPerPartition = maxRowCountPerPartition;
        this.generateRowNumber = generateRowNumber;
        checkArgument(expectedPositions > 0, "expectedPositions must be > 0");

        this.types = toTypes(sourceTypes, outputChannels, generateRowNumber);
        this.partitionRows = new HashMap<>();
        if (partitionChannels.isEmpty()) {
            this.groupByHash = Optional.empty();
        }
        else {
            this.groupByHash = Optional.of(createGroupByHash(operatorContext.getSession(), partitionTypes, Ints.toArray(partitionChannels), hashChannel, expectedPositions));
        }
        this.flushingPartition = Optional.empty();
        this.pageBuilder = new PageBuilder(types);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && isEmpty() && !isFlushing();
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && !isFlushing();
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        checkState(!isFlushing(), "Cannot add input with the operator is flushing data");
        processPage(page);
    }

    @Override
    public Page getOutput()
    {
        if (finishing && !isFinished()) {
            return getPage();
        }
        return null;
    }

    private void processPage(Page page)
    {
        Optional<GroupByIdBlock> partitionIds = Optional.empty();
        if (groupByHash.isPresent()) {
            GroupByHash hash = groupByHash.get();
            long groupByHashSize = hash.getEstimatedSize();
            partitionIds = Optional.of(hash.getGroupIds(page));
            operatorContext.reserveMemory(hash.getEstimatedSize() - groupByHashSize);
        }

        long sizeDelta = 0;
        Block[] blocks = page.getBlocks();
        for (int position = 0; position < page.getPositionCount(); position++) {
            long partitionId = groupByHash.isPresent() ? partitionIds.get().getGroupId(position) : 0;
            if (!partitionRows.containsKey(partitionId)) {
                partitionRows.put(partitionId, new PartitionBuilder(sortTypes, sortChannels, sortOrders, maxRowCountPerPartition));
            }
            PartitionBuilder partitionBuilder = partitionRows.get(partitionId);
            if (partitionBuilder.getRowCount() < maxRowCountPerPartition) {
                Block[] row = getSingleValueBlocks(page, position);
                sizeDelta += partitionBuilder.addRow(row);
            }
            else if (compare(position, blocks, partitionBuilder.peekLastRow()) < 0) {
                Block[] row = getSingleValueBlocks(page, position);
                sizeDelta += partitionBuilder.replaceRow(row);
            }
        }
        if (sizeDelta > 0) {
            operatorContext.reserveMemory(sizeDelta);
        }
        else {
            operatorContext.freeMemory(-sizeDelta);
        }
    }

    private int compare(int position, Block[] blocks, Block[] currentMax)
    {
        for (int i = 0; i < sortChannels.size(); i++) {
            Type type = sortTypes.get(i);
            int sortChannel = sortChannels.get(i);
            SortOrder sortOrder = sortOrders.get(i);

            Block block = blocks[sortChannel];
            Block currentMaxValue = currentMax[sortChannel];

            int compare = sortOrder.compareBlockValue(type, block, position, currentMaxValue, 0);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    private Page getPage()
    {
        if (!flushingPartition.isPresent()) {
            flushingPartition = getFlushingPartition();
        }

        pageBuilder.reset();
        long sizeDelta = 0;
        while (!pageBuilder.isFull() && flushingPartition.isPresent()) {
            FlushingPartition currentFlushingPartition = flushingPartition.get();

            while (!pageBuilder.isFull() && currentFlushingPartition.hasNext()) {
                Block[] next = currentFlushingPartition.next();
                sizeDelta += sizeOfRow(next);

                pageBuilder.declarePosition();
                for (int i = 0; i < outputChannels.length; i++) {
                    int channel = outputChannels[i];
                    Type type = types.get(i);
                    type.appendTo(next[channel], 0, pageBuilder.getBlockBuilder(i));
                }
                if (generateRowNumber) {
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(outputChannels.length), currentFlushingPartition.getRowNumber());
                }
            }
            if (!currentFlushingPartition.hasNext()) {
                flushingPartition = getFlushingPartition();
            }
        }
        if (pageBuilder.isEmpty()) {
            return null;
        }
        Page page = pageBuilder.build();
        operatorContext.freeMemory(sizeDelta);
        return page;
    }

    private Optional<FlushingPartition> getFlushingPartition()
    {
        int maxPartitionSize = 0;
        PartitionBuilder chosenPartitionBuilder = null;
        long chosenPartitionId = -1;

        for (Map.Entry<Long, PartitionBuilder> entry : partitionRows.entrySet()) {
            if (entry.getValue().getRowCount() > maxPartitionSize) {
                chosenPartitionBuilder = entry.getValue();
                maxPartitionSize = chosenPartitionBuilder.getRowCount();
                chosenPartitionId = entry.getKey();
                if (maxPartitionSize == maxRowCountPerPartition) {
                    break;
                }
            }
        }
        if (chosenPartitionBuilder == null) {
            return Optional.empty();
        }
        FlushingPartition flushingPartition = new FlushingPartition(chosenPartitionBuilder.build());
        partitionRows.remove(chosenPartitionId);
        return Optional.of(flushingPartition);
    }

    public boolean isFlushing()
    {
        return flushingPartition.isPresent();
    }

    public boolean isEmpty()
    {
        return partitionRows.isEmpty();
    }

    private static Block[] getSingleValueBlocks(Page page, int position)
    {
        Block[] blocks = page.getBlocks();
        Block[] row = new Block[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            row[i] = blocks[i].getSingleValueBlock(position);
        }
        return row;
    }

    private static List<Type> toTypes(List<? extends Type> sourceTypes, List<Integer> outputChannels, boolean generateRowNumber)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (int channel : outputChannels) {
            types.add(sourceTypes.get(channel));
        }
        if (generateRowNumber) {
            types.add(BIGINT);
        }
        return types.build();
    }

    private static long sizeOfRow(Block[] row)
    {
        long size = OVERHEAD_PER_VALUE.toBytes();
        for (Block value : row) {
            size += value.getRetainedSizeInBytes();
        }
        return size;
    }

    private static class PartitionBuilder
    {
        private final MinMaxPriorityQueue<Block[]> candidateRows;
        private final int maxRowCountPerPartition;

        private PartitionBuilder(List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders, int maxRowCountPerPartition)
        {
            this.maxRowCountPerPartition = maxRowCountPerPartition;
            Ordering<Block[]> comparator = Ordering.from(new RowComparator(sortTypes, sortChannels, sortOrders));
            this.candidateRows = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(maxRowCountPerPartition).create();
        }

        private long replaceRow(Block[] row)
        {
            checkState(candidateRows.size() == maxRowCountPerPartition);
            Block[] previousRow = candidateRows.removeLast();
            long sizeDelta = addRow(row);
            return sizeDelta - sizeOfRow(previousRow);
        }

        private long addRow(Block[] row)
        {
            checkState(candidateRows.size() < maxRowCountPerPartition);
            long sizeDelta = sizeOfRow(row);
            candidateRows.add(row);
            return sizeDelta;
        }

        private Iterator<Block[]> build()
        {
            ImmutableList.Builder<Block[]> sortedRows = ImmutableList.builder();
            while (!candidateRows.isEmpty()) {
                sortedRows.add(candidateRows.poll());
            }
            return sortedRows.build().iterator();
        }

        private int getRowCount()
        {
            return candidateRows.size();
        }

        private Block[] peekLastRow()
        {
            return candidateRows.peekLast();
        }
    }

    private static class FlushingPartition
            implements Iterator<Block[]>
    {
        private final Iterator<Block[]> outputIterator;
        private int rowNumber;

        private FlushingPartition(Iterator<Block[]> outputIterator)
        {
            this.outputIterator = outputIterator;
        }

        @Override
        public boolean hasNext()
        {
            return outputIterator.hasNext();
        }

        @Override
        public Block[] next()
        {
            rowNumber++;
            return outputIterator.next();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public int getRowNumber()
        {
            return rowNumber;
        }
    }
}
