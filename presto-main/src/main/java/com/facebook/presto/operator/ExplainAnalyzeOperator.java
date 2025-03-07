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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryPerformanceFetcher;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.tree.ExplainFormat;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.jsonDistributedPlan;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.textDistributedPlan;
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
        private final QueryPerformanceFetcher queryPerformanceFetcher;
        private final FunctionAndTypeManager functionAndTypeManager;
        private final boolean verbose;
        private final ExplainFormat.Type format;
        private boolean closed;

        public ExplainAnalyzeOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                QueryPerformanceFetcher queryPerformanceFetcher,
                FunctionAndTypeManager functionAndTypeManager,
                boolean verbose,
                ExplainFormat.Type format)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.queryPerformanceFetcher = requireNonNull(queryPerformanceFetcher, "queryPerformanceFetcher is null");
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
            this.verbose = verbose;
            this.format = requireNonNull(format, "format is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, ExplainAnalyzeOperator.class.getSimpleName());
            return new ExplainAnalyzeOperator(operatorContext, queryPerformanceFetcher, functionAndTypeManager, verbose, format);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new ExplainAnalyzeOperatorFactory(operatorId, planNodeId, queryPerformanceFetcher, functionAndTypeManager, verbose, format);
        }
    }

    private final OperatorContext operatorContext;
    private final QueryPerformanceFetcher queryPerformanceFetcher;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final boolean verbose;
    private final ExplainFormat.Type format;
    private boolean finishing;
    private boolean outputConsumed;

    public ExplainAnalyzeOperator(
            OperatorContext operatorContext,
            QueryPerformanceFetcher queryPerformanceFetcher,
            FunctionAndTypeManager functionAndTypeManager,
            boolean verbose,
            ExplainFormat.Type format)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.queryPerformanceFetcher = requireNonNull(queryPerformanceFetcher, "queryPerformanceFetcher is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
        this.verbose = verbose;
        this.format = requireNonNull(format, "format is null");
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
        return finishing && outputConsumed;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing;
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
        if (!finishing) {
            return null;
        }

        QueryInfo queryInfo = queryPerformanceFetcher.getQueryInfo(operatorContext.getDriverContext().getTaskId().getQueryId());
        checkState(queryInfo.getOutputStage().isPresent(), "Output stage is missing");

        if (!hasFinalStageInfo(queryInfo.getOutputStage().get())) {
            return null;
        }
        String plan;
        switch (format) {
            case TEXT:
                StringBuilder planStringBuilder = new StringBuilder();
                for (StageInfo stageInfo : queryInfo.getOutputStage().get().getSubStages()) {
                    planStringBuilder.append("Stage ID: ").append(stageInfo.getStageId().toString()).append('\n')
                            .append(textDistributedPlan(stageInfo, functionAndTypeManager, operatorContext.getSession(),
                                    verbose));
                }
                plan = planStringBuilder.toString();
                break;
            case JSON:
                plan = jsonDistributedPlan(queryInfo.getOutputStage().get().getSubStages(), functionAndTypeManager,
                        operatorContext.getSession());
                break;
            default:
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Explain format not supported: " + format);
        }

        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 1);
        VARCHAR.writeString(builder, plan);

        outputConsumed = true;
        return new Page(builder.build());
    }

    private boolean hasFinalStageInfo(StageInfo stageInfo)
    {
        boolean isFinalStageInfo = isFinalStageInfo(stageInfo);
        if (!isFinalStageInfo) {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return isFinalStageInfo(stageInfo);
    }

    private boolean isFinalStageInfo(StageInfo stageInfo)
    {
        List<StageInfo> subStages = getSubStagesOf(operatorContext.getDriverContext().getTaskId().getStageExecutionId().getStageId(), stageInfo);
        return subStages.stream().allMatch(StageInfo::isFinalStageInfo);
    }

    private static List<StageInfo> getSubStagesOf(StageId stageId, StageInfo rootStage)
    {
        ImmutableList.Builder<StageInfo> collector = ImmutableList.builder();
        getSubStages(stageId, rootStage, collector, false);
        return collector.build();
    }

    private static void getSubStages(StageId stageId, StageInfo rootStage, ImmutableList.Builder<StageInfo> collector, boolean add)
    {
        if (rootStage.getStageId().equals(stageId)) {
            add = true;
        }
        List<StageInfo> subStages = rootStage.getSubStages();
        for (StageInfo subStage : subStages) {
            getSubStages(stageId, subStage, collector, add);
        }

        if (add && !rootStage.getStageId().equals(stageId)) {
            collector.add(rootStage);
        }
    }
}
