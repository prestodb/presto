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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.SqlFunctionResult;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RemoteProjectOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final List<RowExpression> projections;

    private final CompletableFuture<SqlFunctionResult>[] result;

    private boolean finishing;

    private RemoteProjectOperator(OperatorContext operatorContext, FunctionAndTypeManager functionAndTypeManager, List<RowExpression> projections)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
        this.projections = ImmutableList.copyOf(requireNonNull(projections, "projections is null"));
        this.result = new CompletableFuture[projections.size()];
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && !processingPage();
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        checkState(!processingPage(), "Still processing previous input");
        requireNonNull(page, "page is null");
        for (int channel = 0; channel < projections.size(); channel++) {
            RowExpression projection = projections.get(channel);
            if (projection instanceof InputReferenceExpression) {
                result[channel] = completedFuture(new SqlFunctionResult(page.getBlock(((InputReferenceExpression) projection).getField()), 0));
            }
            else if (projection instanceof CallExpression) {
                CallExpression remoteCall = (CallExpression) projection;
                result[channel] = functionAndTypeManager.executeFunction(
                        operatorContext.getDriverContext().getTaskId().toString(),
                        remoteCall.getFunctionHandle(),
                        page,
                        remoteCall.getArguments().stream()
                                .map(InputReferenceExpression.class::cast)
                                .map(InputReferenceExpression::getField)
                                .collect(toImmutableList()));
            }
            else {
                checkState(projection instanceof ConstantExpression, format("Does not expect expression type %s", projection.getClass()));
            }
        }
    }

    @Override
    public Page getOutput()
    {
        if (resultReady()) {
            Block[] blocks = new Block[result.length];
            Page output;
            try {
                for (int i = 0; i < blocks.length; i++) {
                    blocks[i] = result[i].get().getResult();
                    operatorContext.recordAdditionalCpu(MILLISECONDS.toNanos(result[i].get().getCpuTimeMs()));
                }
                output = new Page(blocks);
                Arrays.fill(result, null);
                return output;
            }
            catch (InterruptedException ie) {
                currentThread().interrupt();
                throw new RuntimeException(ie);
            }
            catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause != null) {
                    throwIfUnchecked(cause);
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, cause);
                }
                throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
            }
        }
        return null;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && !processingPage();
    }

    private boolean processingPage()
    {
        // Array result will be filled with nulls when getOutput() produce output.
        // If result has non-null values that means input page is in processing.
        for (int i = 0; i < result.length; i++) {
            if (result[i] != null) {
                return true;
            }
        }
        return false;
    }

    private boolean resultReady()
    {
        for (int i = 0; i < result.length; i++) {
            if (result[i] == null || !result[i].isDone()) {
                return false;
            }
        }
        return true;
    }

    public static class RemoteProjectOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final FunctionAndTypeManager functionAndTypeManager;
        private final List<RowExpression> projections;
        private boolean closed;

        public RemoteProjectOperatorFactory(int operatorId, PlanNodeId planNodeId, FunctionAndTypeManager functionAndTypeManager, List<RowExpression> projections)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
            this.projections = ImmutableList.copyOf(requireNonNull(projections, "projections is null"));
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, RemoteProjectOperator.class.getSimpleName());
            return new RemoteProjectOperator(operatorContext, functionAndTypeManager, projections);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new RemoteProjectOperatorFactory(operatorId, planNodeId, functionAndTypeManager, projections);
        }
    }
}
