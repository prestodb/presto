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

import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ScanFilterAndProjectOperator
        extends AbstractScanFilterAndProjectOperator
{
    public static class ScanFilterAndProjectOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final DataStreamProvider dataStreamProvider;
        private final List<ColumnHandle> columns;
        private final FilterFunction filterFunction;
        private final List<ProjectionFunction> projections;
        private final List<Type> types;
        private boolean closed;

        public ScanFilterAndProjectOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                DataStreamProvider dataStreamProvider,
                Iterable<ColumnHandle> columns,
                FilterFunction filterFunction,
                Iterable<? extends ProjectionFunction> projections)
        {
            this.operatorId = operatorId;
            this.sourceId = checkNotNull(sourceId, "sourceId is null");
            this.dataStreamProvider = checkNotNull(dataStreamProvider, "dataStreamProvider is null");
            this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
            this.filterFunction = checkNotNull(filterFunction, "filterFunction is null");
            this.projections = ImmutableList.copyOf(checkNotNull(projections, "projections is null"));
            this.types = toTypes(this.projections);
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, FilterAndProjectOperator.class.getSimpleName());
            return new ScanFilterAndProjectOperator(operatorContext, sourceId, dataStreamProvider, columns, filterFunction, projections);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final FilterFunction filterFunction;
    private final List<ProjectionFunction> projections;

    public ScanFilterAndProjectOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            DataStreamProvider dataStreamProvider,
            Iterable<ColumnHandle> columns,
            FilterFunction filterFunction,
            Iterable<? extends ProjectionFunction> projections)
    {
        super(operatorContext,
                sourceId,
                dataStreamProvider,
                columns,
                toTypes(ImmutableList.copyOf(checkNotNull(projections, "projections is null"))));
        this.filterFunction = checkNotNull(filterFunction, "filterFunction is null");
        this.projections = ImmutableList.copyOf(checkNotNull(projections, "projections is null"));
    }

    protected void filterAndProjectRowOriented(Page page, PageBuilder pageBuilder)
    {
        int rows = page.getPositionCount();

        BlockCursor[] cursors = new BlockCursor[page.getChannelCount()];
        for (int i = 0; i < page.getChannelCount(); i++) {
            cursors[i] = page.getBlock(i).cursor();
        }

        for (int position = 0; position < rows; position++) {
            for (BlockCursor cursor : cursors) {
                checkState(cursor.advanceNextPosition());
            }

            if (filterFunction.filter(cursors)) {
                pageBuilder.declarePosition();
                for (int i = 0; i < projections.size(); i++) {
                    // todo: if the projection function increases the size of the data significantly, this could cause the servers to OOM
                    projections.get(i).project(cursors, pageBuilder.getBlockBuilder(i));
                }
            }
        }

        for (BlockCursor cursor : cursors) {
            checkState(!cursor.advanceNextPosition());
        }
    }

    @Override
    protected int filterAndProjectRowOriented(RecordCursor cursor, PageBuilder pageBuilder)
    {
        int completedPositions = 0;
        for (; completedPositions < 16384; completedPositions++) {
            if (pageBuilder.isFull()) {
                break;
            }

            if (!cursor.advanceNextPosition()) {
                break;
            }

            if (filterFunction.filter(cursor)) {
                pageBuilder.declarePosition();
                for (int channel = 0; channel < projections.size(); channel++) {
                    // todo: if the projection function increases the size of the data significantly, this could cause the servers to OOM
                    projections.get(channel).project(cursor, pageBuilder.getBlockBuilder(channel));
                }
            }
        }
        return completedPositions;
    }

    private static List<Type> toTypes(List<ProjectionFunction> projections)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ProjectionFunction projection : projections) {
            types.add(projection.getType());
        }
        return types.build();
    }
}
