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

import com.facebook.presto.spi.type.Type;
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
        private final List<Type> types;
        private boolean closed;

        public FilterAndProjectOperatorFactory(int operatorId, FilterFunction filterFunction, Iterable<? extends ProjectionFunction> projections)
        {
            this.operatorId = operatorId;
            this.filterFunction = checkNotNull(filterFunction, "filterFunction is null");
            this.projections = ImmutableList.copyOf(projections);
            this.types = toTypes(checkNotNull(projections, "projections is null"));
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
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
        super(operatorContext, toTypes(checkNotNull(projections, "projections is null")));
        this.filterFunction = checkNotNull(filterFunction, "filterFunction is null");
        this.projections = ImmutableList.copyOf(projections);
    }

    @Override
    protected void filterAndProjectRowOriented(Page page, PageBuilder pageBuilder)
    {
        int rows = page.getPositionCount();

        for (int position = 0; position < rows; position++) {
            if (filterFunction.filter(position, page.getBlocks())) {
                pageBuilder.declarePosition();
                for (int i = 0; i < projections.size(); i++) {
                    // todo: if the projection function increases the size of the data significantly, this could cause the servers to OOM
                    projections.get(i).project(position, page.getBlocks(), pageBuilder.getBlockBuilder(i));
                }
            }
        }
    }

    private static List<Type> toTypes(Iterable<? extends ProjectionFunction> projections)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ProjectionFunction projection : projections) {
            types.add(projection.getType());
        }
        return types.build();
    }
}
