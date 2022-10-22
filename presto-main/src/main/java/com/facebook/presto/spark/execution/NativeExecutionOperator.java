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
package com.facebook.presto.spark.execution;

import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.operator.SplitOperatorInfo;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.NativeExecutionNode;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.isExchangeChecksumEnabled;
import static com.facebook.presto.SystemSessionProperties.isExchangeCompressionEnabled;
import static com.facebook.presto.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * NativeExecutionOperator is responsible for launching the external native process and managing the communication
 * between Java process and native process (by using the {@Link NativeExecutionTask}). The NativeExecutionOperator will send  necessary meta information
 * (e.g, plan fragment, session properties etc) will be sent to native process and collect the execution results (data, metrics etc) back and propagate out as
 * the operator output through the operator's getOutput method.
 * <p>
 * TODO: The lifecycle of NativeExecutionOperator is (will be added in next PR):
 *  1. Launch the native engine external process when initializing the operator.
 *  2. Serialize and pass the planFragment, tableWriteInfo and taskSource to the external process through {@link NativeExecutionTask} APIs.
 *  3. Call {@link NativeExecutionTask}'s pollResult() to retrieve {@link SerializedPage} back from external process.
 *  4. Deserialize {@link SerializedPage} to {@link Page} and return it back to driver from the getOutput method.
 *  5. The close() will be called by the driver when {@link NativeExecutionTask} completes and pollResult() returns an empty result.
 *  6. Shut down the external process upon calling of close() method
 */
public class NativeExecutionOperator
        implements SourceOperator
{
    private final PlanNodeId sourceId;
    private final OperatorContext operatorContext;
    private final LocalMemoryContext systemMemoryContext;
    // TODO: will be used to create the {@link NativeExecutionTask}
    private final PlanFragment planFragment;
    // TODO: will be used to create the {@link NativeExecutionTask}
    private final TableWriteInfo tableWriteInfo;
    // TODO: will be used to deserialize the  {@link SerializedPage}
    private final PagesSerde serde;
    private TaskSource taskSource;
    private boolean finished;

    public NativeExecutionOperator(
            PlanNodeId sourceId,
            OperatorContext operatorContext,
            PlanFragment planFragment,
            TableWriteInfo tableWriteInfo,
            PagesSerde serde)
    {
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.systemMemoryContext = operatorContext.localSystemMemoryContext();
        this.planFragment = requireNonNull(planFragment, "planFragment is null");
        this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
        this.serde = requireNonNull(serde, "serde is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * TODO: The getOutput method will call NativeExecutionTask APIs to:
     *  1. Submit the plan to the external process
     *  2. Call pollResult method to get latest buffered result.
     *  3. Call getTaskInfo method to get the TaskInfo and propagate it
     *  4. Deserialize the polled {@link SerializedPage} to {@link Page} and return it back
     */
    @Override
    public Page getOutput()
    {
        finished = true;
        return null;
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(ScheduledSplit split)
    {
        requireNonNull(split, "split is null");
        checkState(this.taskSource == null, "NativeEngine operator split already set");

        if (finished) {
            return Optional::empty;
        }

        this.taskSource = new TaskSource(split.getPlanNodeId(), ImmutableSet.of(split), true);

        Object splitInfo = split.getSplit().getInfo();
        Map<String, String> infoMap = split.getSplit().getInfoMap();

        //Make the implicit assumption that if infoMap is populated we can use that instead of the raw object.
        if (infoMap != null && !infoMap.isEmpty()) {
            operatorContext.setInfoSupplier(Suppliers.ofInstance(new SplitOperatorInfo(infoMap)));
        }
        else if (splitInfo != null) {
            operatorContext.setInfoSupplier(Suppliers.ofInstance(new SplitOperatorInfo(splitInfo)));
        }

        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
    }

    /*
     * TODO: the close() method will shutdown the external process
     */
    @Override
    public void close()
    {
        systemMemoryContext.setBytes(0);
    }

    public static class NativeExecutionOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PlanFragment planFragment;
        private final TableWriteInfo tableWriteInfo;
        private final PagesSerdeFactory serdeFactory;
        private boolean closed;

        public NativeExecutionOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PlanFragment planFragment,
                TableWriteInfo tableWriteInfo,
                PagesSerdeFactory serdeFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.planFragment = requireNonNull(planFragment, "planFragment is null");
            this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
            this.serdeFactory = requireNonNull(serdeFactory, "serdeFactory is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return planNodeId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "operator factory is closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, NativeExecutionOperator.class.getSimpleName());
            return new NativeExecutionOperator(
                    planNodeId,
                    operatorContext,
                    planFragment,
                    tableWriteInfo,
                    serdeFactory.createPagesSerde());
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        public PlanFragment getPlanFragment()
        {
            return planFragment;
        }
    }

    public static class NativeExecutionOperatorTranslator
            extends LocalExecutionPlanner.CustomVisitor
    {
        private final PlanFragment fragment;
        private final Session session;
        private final BlockEncodingSerde blockEncodingSerde;

        public NativeExecutionOperatorTranslator(Session session, PlanFragment fragment, BlockEncodingSerde blockEncodingSerde)
        {
            this.fragment = requireNonNull(fragment, "fragment is null");
            this.session = requireNonNull(session, "session is null");
            this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        }

        @Override
        public Optional<LocalExecutionPlanner.PhysicalOperation> translate(PlanNode node, LocalExecutionPlanner.LocalExecutionPlanContext context, InternalPlanVisitor visitor)
        {
            if (node instanceof NativeExecutionNode) {
                OperatorFactory operatorFactory = new NativeExecutionOperator.NativeExecutionOperatorFactory(
                        context.getNextOperatorId(),
                        node.getId(),
                        fragment.withSubPlan(((NativeExecutionNode) node).getSubPlan()),
                        context.getTableWriteInfo(),
                        new PagesSerdeFactory(blockEncodingSerde, isExchangeCompressionEnabled(session), isExchangeChecksumEnabled(session)));
                return Optional.of(
                        new LocalExecutionPlanner.PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION));
            }
            return Optional.empty();
        }
    }
}
