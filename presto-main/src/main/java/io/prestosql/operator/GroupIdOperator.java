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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.type.BigintType.BIGINT;
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
        private final List<Map<Integer, Integer>> groupingSetMappings;

        private boolean closed;

        public GroupIdOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> outputTypes,
                List<Map<Integer, Integer>> groupingSetMappings)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes));
            this.groupingSetMappings = ImmutableList.copyOf(requireNonNull(groupingSetMappings));
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, GroupIdOperator.class.getSimpleName());

            // create an int array for fast lookup of input columns for every grouping set
            int[][] groupingSetInputs = new int[groupingSetMappings.size()][outputTypes.size() - 1];
            for (int i = 0; i < groupingSetMappings.size(); i++) {
                // -1 means the output column is null
                Arrays.fill(groupingSetInputs[i], -1);

                // anything else is an input column to copy
                for (int outputChannel : groupingSetMappings.get(i).keySet()) {
                    groupingSetInputs[i][outputChannel] = groupingSetMappings.get(i).get(outputChannel);
                }
            }

            // it's easier to create null blocks for every output column even though we only null out some grouping column outputs
            Block[] nullBlocks = new Block[outputTypes.size()];
            for (int i = 0; i < outputTypes.size(); i++) {
                nullBlocks[i] = outputTypes.get(i).createBlockBuilder(null, 1)
                        .appendNull()
                        .build();
            }

            // create groupid blocks for every group
            Block[] groupIdBlocks = new Block[groupingSetMappings.size()];
            for (int i = 0; i < groupingSetMappings.size(); i++) {
                BlockBuilder builder = BIGINT.createBlockBuilder(null, 1);
                BIGINT.writeLong(builder, i);
                groupIdBlocks[i] = builder.build();
            }

            return new GroupIdOperator(operatorContext, outputTypes, groupingSetInputs, nullBlocks, groupIdBlocks);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new GroupIdOperatorFactory(operatorId, planNodeId, outputTypes, groupingSetMappings);
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final int[][] groupingSetInputs;
    private final Block[] nullBlocks;
    private final Block[] groupIdBlocks;

    private Page currentPage;
    private int currentGroupingSet;
    private boolean finishing;

    public GroupIdOperator(
            OperatorContext operatorContext,
            List<Type> types,
            int[][] groupingSetInputs,
            Block[] nullBlocks,
            Block[] groupIdBlocks)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.groupingSetInputs = requireNonNull(groupingSetInputs, "groupingSetInputs is null");
        this.nullBlocks = requireNonNull(nullBlocks, "nullBlocks is null");
        this.groupIdBlocks = requireNonNull(groupIdBlocks, "groupIdBlocks is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
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

        for (int i = 0; i < groupingSetInputs[currentGroupingSet].length; i++) {
            if (groupingSetInputs[currentGroupingSet][i] == -1) {
                outputBlocks[i] = new RunLengthEncodedBlock(nullBlocks[i], currentPage.getPositionCount());
            }
            else {
                outputBlocks[i] = currentPage.getBlock(groupingSetInputs[currentGroupingSet][i]);
            }
        }

        outputBlocks[outputBlocks.length - 1] = new RunLengthEncodedBlock(groupIdBlocks[currentGroupingSet], currentPage.getPositionCount());
        currentGroupingSet = (currentGroupingSet + 1) % groupingSetInputs.length;
        Page outputPage = new Page(currentPage.getPositionCount(), outputBlocks);

        if (currentGroupingSet == 0) {
            currentPage = null;
        }

        return outputPage;
    }
}
