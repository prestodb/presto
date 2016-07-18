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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class GroupIdOperator
        implements Operator
{
    public static class GroupIdOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> outputTypes;
        private final List<List<Integer>> groupingSetChannels;
        private final List<Integer> groupingChannels;
        private final List<Integer> copyChannels;

        private boolean closed;

        public GroupIdOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> outputTypes,
                List<List<Integer>> groupingSetChannels,
                List<Integer> groupingChannels,
                List<Integer> copyChannels)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes));
            this.groupingSetChannels = ImmutableList.copyOf(requireNonNull(groupingSetChannels));
            this.groupingChannels = ImmutableList.copyOf(requireNonNull(groupingChannels));
            this.copyChannels = ImmutableList.copyOf(requireNonNull(copyChannels));
        }

        @Override
        public List<Type> getTypes()
        {
            return outputTypes;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, GroupIdOperator.class.getSimpleName());

            Set<Integer> allGroupingColumns = groupingSetChannels.stream()
                    .flatMap(Collection::stream)
                    .collect(toImmutableSet());

            // create an array of bitset for fast lookup of which columns are part of a given grouping set
            // will have a 'true' for every channel that should be set to null for each grouping set
            BitSet[] groupingSetNullChannels = new BitSet[groupingSetChannels.size()];
            for (int i = 0; i < groupingSetChannels.size(); i++) {
                groupingSetNullChannels[i] = new BitSet(groupingChannels.size() + copyChannels.size());
                // first set all grouping columns to true
                for (int groupingColumn : allGroupingColumns) {
                    groupingSetNullChannels[i].set(groupingColumn, true);
                }
                // then set all the columns in this grouping set to false
                for (int nonNullGroupingColumn : groupingSetChannels.get(i)) {
                    groupingSetNullChannels[i].set(nonNullGroupingColumn, false);
                }
            }

            // create null blocks for every grouping channel
            Block[] nullBlocks = new Block[groupingChannels.size()];
            for (int i = 0; i < groupingChannels.size(); i++) {
                nullBlocks[i] = outputTypes.get(i).createBlockBuilder(new BlockBuilderStatus(), 1)
                        .appendNull()
                        .build();
            }

            // create groupid blocks for every group
            Block[] groupIdBlocks = new Block[groupingSetNullChannels.length];
            for (int i = 0; i < groupingSetNullChannels.length; i++) {
                BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 1);
                BIGINT.writeLong(builder, i);
                groupIdBlocks[i] = builder.build();
            }

            // create array of input channels for every grouping channel
            int[] groupInputs = groupingChannels.stream().mapToInt(Integer::intValue).toArray();

            // create array of input channels for every copy channel
            int[] copyInputs = copyChannels.stream().mapToInt(Integer::intValue).toArray();

            return new GroupIdOperator(operatorContext, outputTypes, groupingSetNullChannels, nullBlocks, groupIdBlocks, groupInputs, copyInputs);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new GroupIdOperatorFactory(operatorId, planNodeId, outputTypes, groupingSetChannels, groupingChannels, copyChannels);
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final BitSet[] groupingSetNullChannels;
    private final Block[] nullBlocks;
    private final Block[] groupIdBlocks;
    private final int[] groupInputs;
    private final int[] copyInputs;

    private Page currentPage = null;
    private int currentGroupingSet = 0;
    private boolean finishing;

    public GroupIdOperator(
            OperatorContext operatorContext,
            List<Type> types,
            BitSet[] groupingSetNullChannels,
            Block[] nullBlocks,
            Block[] groupIdBlocks,
            int[] groupInputs,
            int[] copyInputs)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = requireNonNull(types, "types is null");
        this.groupingSetNullChannels =  requireNonNull(groupingSetNullChannels, "groupingSetNullChannels is null");
        this.nullBlocks = requireNonNull(nullBlocks);
        this.groupIdBlocks = requireNonNull(groupIdBlocks);
        checkArgument(groupIdBlocks.length == groupingSetNullChannels.length, "groupIdBlocks and groupingSetNullChannels must have the same length");
        this.groupInputs = requireNonNull(groupInputs);
        this.copyInputs = requireNonNull(copyInputs);
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
        return finishing && currentPage == null;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && currentPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        checkState(currentPage == null, "currentPage must be null to add a new page");

        currentPage = requireNonNull(page, "page is null");
    }

    @Override
    public Page getOutput()
    {
        if (currentPage == null) {
            return null;
        }

        return generateNextPage();
    }

    private Page generateNextPage()
    {
        // generate 'n' pages for every input page, where n is the number of grouping sets
        Block[] outputBlocks = new Block[types.size()];

        for (int i = 0; i < groupInputs.length; i++) {
            if (groupingSetNullChannels[currentGroupingSet].get(groupInputs[i])) {
                outputBlocks[i] = new RunLengthEncodedBlock(nullBlocks[i], currentPage.getPositionCount());
            }
            else {
                outputBlocks[i] = currentPage.getBlock(groupInputs[i]);
            }
        }

        for (int i = 0; i < copyInputs.length; i++) {
            outputBlocks[groupInputs.length + i] = currentPage.getBlock(copyInputs[i]);
        }

        outputBlocks[outputBlocks.length - 1] = new RunLengthEncodedBlock(groupIdBlocks[currentGroupingSet], currentPage.getPositionCount());
        currentGroupingSet = (currentGroupingSet + 1) % groupingSetNullChannels.length;
        Page outputPage = new Page(currentPage.getPositionCount(), outputBlocks);

        if (currentGroupingSet == 0) {
            currentPage = null;
        }

        return outputPage;
    }
}
