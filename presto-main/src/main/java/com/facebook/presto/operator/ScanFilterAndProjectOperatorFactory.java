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

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ScanFilterAndProjectOperatorFactory
        implements SourceOperatorFactory
{
    private final int operatorId;
    private final CursorProcessor cursorProcessor;
    private final PageProcessor pageProcessor;
    private final PlanNodeId sourceId;
    private final DataStreamProvider dataStreamProvider;
    private final List<ColumnHandle> columns;
    private final List<Type> types;
    private boolean closed;

    public ScanFilterAndProjectOperatorFactory(
            int operatorId,
            PlanNodeId sourceId,
            DataStreamProvider dataStreamProvider,
            CursorProcessor cursorProcessor,
            PageProcessor pageProcessor,
            Iterable<ColumnHandle> columns,
            List<Type> types)
    {
        this.operatorId = operatorId;
        this.cursorProcessor = cursorProcessor;
        this.pageProcessor = pageProcessor;
        this.sourceId = checkNotNull(sourceId, "sourceId is null");
        this.dataStreamProvider = checkNotNull(dataStreamProvider, "dataStreamProvider is null");
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
        this.types = types;
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
        return new ScanFilterAndProjectOperator(
                operatorContext,
                sourceId,
                dataStreamProvider,
                cursorProcessor,
                pageProcessor,
                columns,
                types);
    }

    @Override
    public void close()
    {
        closed = true;
    }
}
