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
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.spi.function.table.TableFunctionProcessorProvider;
import com.facebook.presto.spi.function.table.TableFunctionProcessorState;
import com.facebook.presto.spi.function.table.TableFunctionProcessorState.Blocked;
import com.facebook.presto.spi.function.table.TableFunctionProcessorState.Processed;
import com.facebook.presto.spi.function.table.TableFunctionSplitProcessor;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.airlift.concurrent.MoreFutures.toListenableFuture;
import static com.facebook.presto.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LeafTableFunctionOperator
        implements SourceOperator
{
    public static class LeafTableFunctionOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final TableFunctionProcessorProvider tableFunctionProvider;
        private final ConnectorTableFunctionHandle functionHandle;
        private boolean closed;

        public LeafTableFunctionOperatorFactory(int operatorId, PlanNodeId sourceId, TableFunctionProcessorProvider tableFunctionProvider, ConnectorTableFunctionHandle functionHandle)
        {
            this.operatorId = operatorId;
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.tableFunctionProvider = requireNonNull(tableFunctionProvider, "tableFunctionProvider is null");
            this.functionHandle = requireNonNull(functionHandle, "functionHandle is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId, LeafTableFunctionOperator.class.getSimpleName());
            return new LeafTableFunctionOperator(operatorContext, sourceId, tableFunctionProvider, functionHandle);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final TableFunctionProcessorProvider tableFunctionProvider;
    private final ConnectorTableFunctionHandle functionHandle;

    private ConnectorSplit currentSplit;
    private final List<ConnectorSplit> pendingSplits = new ArrayList<>();
    private boolean noMoreSplits;

    private TableFunctionSplitProcessor processor;
    private boolean processorUsedData;
    private boolean processorFinishedSplit = true;
    private ListenableFuture<?> processorBlocked = NOT_BLOCKED;

    public LeafTableFunctionOperator(OperatorContext operatorContext, PlanNodeId sourceId, TableFunctionProcessorProvider tableFunctionProvider, ConnectorTableFunctionHandle functionHandle)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.tableFunctionProvider = requireNonNull(tableFunctionProvider, "tableFunctionProvider is null");
        this.functionHandle = requireNonNull(functionHandle, "functionHandle is null");
    }

    private void resetProcessor()
    {
        this.processor = tableFunctionProvider.getSplitProcessor(functionHandle);
        this.processorUsedData = false;
        this.processorFinishedSplit = false;
        this.processorBlocked = NOT_BLOCKED;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " does not take input");
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(ScheduledSplit split)
    {
        Split curSplit = requireNonNull(split, "split is null").getSplit();
        checkState(!noMoreSplits, "no more splits expected");
        ConnectorSplit curConnectorSplit = curSplit.getConnectorSplit();
        pendingSplits.add(curConnectorSplit);
        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
        noMoreSplits = true;
    }

    @Override
    public Page getOutput()
    {
        if (processorFinishedSplit) {
            // start processing a new split
            if (pendingSplits.isEmpty()) {
                // no more splits to process at the moment
                return null;
            }
            currentSplit = pendingSplits.remove(0);
            resetProcessor();
        }
        else {
            // a split is being processed
            requireNonNull(currentSplit, "currentSplit is null");
        }

        TableFunctionProcessorState state = processor.process(processorUsedData ? null : currentSplit);
        if (state == FINISHED) {
            processorFinishedSplit = true;
        }
        if (state instanceof Blocked) {
            Blocked blocked = (Blocked) state;
            processorBlocked = toListenableFuture(blocked.getFuture());
        }
        if (state instanceof Processed) {
            Processed processed = (Processed) state;
            if (processed.isUsedInput()) {
                processorUsedData = true;
            }
            if (processed.getResult() != null) {
                return processed.getResult();
            }
        }
        return null;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return processorBlocked;
    }

    @Override
    public void finish()
    {
        // this method is redundant. the operator takes no input at all. noMoreSplits() should be called instead.
    }

    @Override
    public boolean isFinished()
    {
        return processorFinishedSplit && pendingSplits.isEmpty() && noMoreSplits;
    }
}
