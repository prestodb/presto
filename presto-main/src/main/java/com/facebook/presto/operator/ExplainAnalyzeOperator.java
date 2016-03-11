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

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.PlanPrinter.textDistributedPlan;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ExplainAnalyzeOperator
        implements Operator
{
    public static class ExplainAnalyzeOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final QueryManager queryManager;
        private final Metadata metadata;
        private boolean closed;

        public ExplainAnalyzeOperatorFactory(int operatorId, PlanNodeId planNodeId, QueryManager queryManager, Metadata metadata)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.queryManager = requireNonNull(queryManager, "queryManager is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of(VARCHAR);
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, ExplainAnalyzeOperator.class.getSimpleName());
            return new ExplainAnalyzeOperator(operatorContext, queryManager, metadata);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new ExplainAnalyzeOperatorFactory(operatorId, planNodeId, queryManager, metadata);
        }
    }

    private final OperatorContext operatorContext;
    private final QueryManager queryManager;
    private final Metadata metadata;
    private boolean flushing;
    private boolean outputConsumed;

    public ExplainAnalyzeOperator(OperatorContext operatorContext, QueryManager queryManager, Metadata metadata)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.of(VARCHAR);
    }

    @Override
    public void finish()
    {
        flushing = true;
    }

    @Override
    public boolean isFinished()
    {
        return flushing && outputConsumed;
    }

    @Override
    public boolean needsInput()
    {
        return !flushing;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput());

        // Ignore the input
    }

    @Override
    public Page getOutput()
    {
        if (!flushing) {
            return null;
        }

        QueryInfo queryInfo = queryManager.getQueryInfo(operatorContext.getDriverContext().getTaskId().getQueryId());
        if (!haveUpstreamOperatorsFinished(queryInfo)) {
            return null;
        }

        outputConsumed = true;
        // Skip the output stage, since that's the one that has the ExplainAnalyzeOperator itself
        String plan = textDistributedPlan(queryInfo.getOutputStage().getSubStages(), metadata, operatorContext.getSession());
        BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 1);
        VARCHAR.writeString(builder, plan);
        return new Page(builder.build());
    }

    private boolean haveUpstreamOperatorsFinished(QueryInfo queryInfo)
    {
        List<StageInfo> stages = getAllStages(queryInfo.getOutputStage());
        // all but first stage containing explain analyze should be done
        for (StageInfo stageInfo : stages.subList(1, stages.size())) {
            if (!stageInfo.getTasks().stream().allMatch(task -> task.getState().isDone())) {
                return false;
            }
        }
        return true;
    }
}
