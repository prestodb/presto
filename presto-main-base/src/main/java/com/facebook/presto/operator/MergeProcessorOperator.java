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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableWriterNode.MergeParadigmAndTypes;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * This operator is used by operations like SQL MERGE.  It is used
 * for all {@link com.facebook.presto.spi.connector.RowChangeParadigm}s.  This operator
 * creates the {@link MergeRowChangeProcessor}.
 */
public class MergeProcessorOperator
        implements Operator
{
    public static class MergeProcessorOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final MergeRowChangeProcessor rowChangeProcessor;
        private boolean closed;

        private MergeProcessorOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                MergeRowChangeProcessor rowChangeProcessor)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.rowChangeProcessor = requireNonNull(rowChangeProcessor, "rowChangeProcessor is null");
        }

        public MergeProcessorOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                MergeParadigmAndTypes merge,
                int rowIdChannel,
                int mergeRowChannel,
                List<Integer> redistributionColumns,
                List<Integer> dataColumnChannels)
        {
            MergeRowChangeProcessor rowChangeProcessor = createRowChangeProcessor(merge, rowIdChannel, mergeRowChannel, redistributionColumns, dataColumnChannels);

            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.rowChangeProcessor = requireNonNull(rowChangeProcessor, "rowChangeProcessor is null");
        }

        private static MergeRowChangeProcessor createRowChangeProcessor(MergeParadigmAndTypes merge, int rowIdChannel, int mergeRowChannel, List<Integer> redistributionColumnChannels, List<Integer> dataColumnChannels)
        {
            switch (merge.getParadigm()) {
                case DELETE_ROW_AND_INSERT_ROW:
                    return new DeleteAndInsertMergeProcessor(
                            merge.getColumnTypes(),
                            merge.getRowIdType(),
                            rowIdChannel,
                            mergeRowChannel,
                            redistributionColumnChannels,
                            dataColumnChannels);
                case CHANGE_ONLY_UPDATED_COLUMNS:
                    return new ChangeOnlyUpdatedColumnsMergeProcessor(
                            rowIdChannel,
                            mergeRowChannel,
                            dataColumnChannels,
                            redistributionColumnChannels);
                default:
                    throw new PrestoException(NOT_SUPPORTED, "Merge paradigm not supported: " + merge.getParadigm());
            }
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, MergeProcessorOperator.class.getSimpleName());
            return new MergeProcessorOperator(context, rowChangeProcessor);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new MergeProcessorOperatorFactory(operatorId, planNodeId, rowChangeProcessor);
        }
    }

    private final OperatorContext operatorContext;
    private final MergeRowChangeProcessor rowChangeProcessor;

    private Page currentPage;
    private boolean finishing;

    public MergeProcessorOperator(
            OperatorContext operatorContext,
            MergeRowChangeProcessor rowChangeProcessor)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.rowChangeProcessor = requireNonNull(rowChangeProcessor, "rowChangeProcessor is null");
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

        Page transformedPage = rowChangeProcessor.transformPage(currentPage);
        currentPage = null;

        return transformedPage;
    }
}
