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
        private final List<Type> inputTypes;
        private final List<Type> outputTypes;
        private final List<List<Integer>> groupingSetChannels;

        private boolean closed;

        public GroupIdOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> inputTypes,
                List<List<Integer>> groupingSetChannels)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.groupingSetChannels = ImmutableList.copyOf(requireNonNull(groupingSetChannels));
            this.inputTypes = ImmutableList.copyOf(requireNonNull(inputTypes));

            // add the groupId channel to the output types
            this.outputTypes = ImmutableList.<Type>builder().addAll(inputTypes).add(BIGINT).build();
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

            // will have a 'true' for every channel that should be set to null for each grouping set
            BitSet[] groupingSetNullChannels = new BitSet[groupingSetChannels.size()];
            for (int i = 0; i < groupingSetChannels.size(); i++) {
                groupingSetNullChannels[i] = new BitSet(inputTypes.size());
                // first set all grouping columns to true
                for (int groupingColumn : allGroupingColumns) {
                    groupingSetNullChannels[i].set(groupingColumn, true);
                }
                // then set all the columns in this grouping set to false
                for (int nonNullGroupingColumn : groupingSetChannels.get(i)) {
                    groupingSetNullChannels[i].set(nonNullGroupingColumn, false);
                }
            }

            Block[] nullBlocks = new Block[inputTypes.size()];
            for (int i = 0; i < nullBlocks.length; i++) {
                nullBlocks[i] = inputTypes.get(i).createBlockBuilder(new BlockBuilderStatus(), 1)
                        .appendNull()
                        .build();
            }

            Block[] groupIdBlocks = new Block[groupingSetNullChannels.length];
            for (int i = 0; i < groupingSetNullChannels.length; i++) {
                BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 1);
                BIGINT.writeLong(builder, i);
                groupIdBlocks[i] = builder.build();
            }

            return new GroupIdOperator(operatorContext, outputTypes, groupingSetNullChannels, nullBlocks, groupIdBlocks);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new GroupIdOperatorFactory(operatorId, planNodeId, inputTypes, groupingSetChannels);
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final BitSet[] groupingSetNullChannels;
    private final Block[] nullBlocks;
    private final Block[] groupIdBlocks;

    private Page currentPage = null;
    private int currentGroupingSet = 0;
    private boolean finishing;

    public GroupIdOperator(
            OperatorContext operatorContext,
            List<Type> types,
            BitSet[] groupingSetNullChannels,
            Block[] nullBlocks,
            Block[] groupIdBlocks)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = requireNonNull(types, "inputTypes is null");
        this.groupingSetNullChannels =  requireNonNull(groupingSetNullChannels, "groupingSetNullChannels is null");
        this.nullBlocks = requireNonNull(nullBlocks);
        checkArgument(nullBlocks.length == (types.size() - 1), "length of nullBlocks must be one plus length of types");
        this.groupIdBlocks = requireNonNull(groupIdBlocks);
        checkArgument(groupIdBlocks.length == groupingSetNullChannels.length, "groupIdBlocks and groupingSetNullChannels must have the same length");
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
        Block[] inputBlocks = currentPage.getBlocks();
        Block[] outputBlocks = new Block[currentPage.getChannelCount() + 1];

        for (int channel = 0; channel < currentPage.getChannelCount(); channel++) {
            if (groupingSetNullChannels[currentGroupingSet].get(channel)) {
                outputBlocks[channel] = new RunLengthEncodedBlock(nullBlocks[channel], currentPage.getPositionCount());
            }
            else {
                outputBlocks[channel] = inputBlocks[channel];
            }
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
