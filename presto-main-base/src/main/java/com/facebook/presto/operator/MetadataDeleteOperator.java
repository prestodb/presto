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

import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.OptionalLong;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MetadataDeleteOperator
        implements Operator
{
    public static final List<Type> TYPES = ImmutableList.of(BIGINT);

    public static class MetadataDeleteOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Metadata metadata;
        private final Session session;
        private final TableHandle tableHandle;
        private boolean closed;

        public MetadataDeleteOperatorFactory(int operatorId, PlanNodeId planNodeId, Metadata metadata, Session session, TableHandle tableHandle)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, MetadataDeleteOperator.class.getSimpleName());
            return new MetadataDeleteOperator(context, metadata, session, tableHandle);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new MetadataDeleteOperatorFactory(operatorId, planNodeId, metadata, session, tableHandle);
        }
    }

    private final OperatorContext operatorContext;
    private final Metadata metadata;
    private final Session session;
    private final TableHandle tableHandle;

    private boolean finished;

    public MetadataDeleteOperator(OperatorContext operatorContext, Metadata metadata, Session session, TableHandle tableHandle)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
    }

    @Override
    public boolean isFinished()
    {
        return finished;
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

    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }
        finished = true;

        OptionalLong rowsDeletedCount = metadata.metadataDelete(session, tableHandle);

        // output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        PageBuilder page = new PageBuilder(1, TYPES);
        BlockBuilder rowsBuilder = page.getBlockBuilder(0);
        page.declarePosition();
        if (rowsDeletedCount.isPresent()) {
            BIGINT.writeLong(rowsBuilder, rowsDeletedCount.getAsLong());
        }
        else {
            rowsBuilder.appendNull();
        }
        return page.build();
    }
}
