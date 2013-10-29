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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class FilterAndProjectOperator
        extends AbstractFilterAndProjectOperator
{
    public static class FilterAndProjectOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final FilterFunction filterFunction;
        private final List<ProjectionFunction> projections;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public FilterAndProjectOperatorFactory(int operatorId, FilterFunction filterFunction, Iterable<? extends ProjectionFunction> projections)
        {
            this.operatorId = operatorId;
            this.filterFunction = checkNotNull(filterFunction, "filterFunction is null");
            this.projections = ImmutableList.copyOf(projections);
            this.tupleInfos = toTupleInfos(checkNotNull(projections, "projections is null"));
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, FilterAndProjectOperator.class.getSimpleName());
            return new FilterAndProjectOperator(operatorContext, filterFunction, projections);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final FilterFunction filterFunction;
    private final List<ProjectionFunction> projections;

    public FilterAndProjectOperator(OperatorContext operatorContext, FilterFunction filterFunction, Iterable<? extends ProjectionFunction> projections)
    {
        super(operatorContext, toTupleInfos(checkNotNull(projections, "projections is null")));
        this.filterFunction = checkNotNull(filterFunction, "filterFunction is null");
        this.projections = ImmutableList.copyOf(projections);
    }

    protected void filterAndProjectRowOriented(Block[] blocks, PageBuilder pageBuilder)
    {
        int rows = blocks[0].getPositionCount();

        BlockCursor[] cursors = new BlockCursor[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            cursors[i] = blocks[i].cursor();
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

    private static List<TupleInfo> toTupleInfos(Iterable<? extends ProjectionFunction> projections)
    {
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (ProjectionFunction projection : projections) {
            tupleInfos.add(projection.getTupleInfo());
        }
        return tupleInfos.build();
    }
}
