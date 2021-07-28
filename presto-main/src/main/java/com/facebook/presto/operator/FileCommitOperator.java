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
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class FileCommitOperator
        implements Operator
{
    public static class FileFinishOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Session session;
        private final Optional<String> schema;
        private boolean closed;

        public FileFinishOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                Optional<String> schema,
                Session session)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.schema = requireNonNull(schema, "schema is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, TableFinishOperator.class.getSimpleName());
            return new FileCommitOperator(context, schema);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new FileFinishOperatorFactory(operatorId, planNodeId, schema, session);
        }
    }

    private enum State
    {
        RUNNING, FINISHING, FINISHED
    }

    private final OperatorContext operatorContext;
    private final Optional<String> schema;
    private State state = State.RUNNING;
    private long rowCount;

    public FileCommitOperator(OperatorContext operatorContext, Optional<String> schema)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.schema = requireNonNull(schema, "schema is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.RUNNING;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(state == State.RUNNING, "Operator is %s", state);
        rowCount += BIGINT.getLong(page.getBlock(0), 0);
    }

    @Override
    public Page getOutput()
    {
        if (state != State.FINISHING) {
            return null;
        }
        state = State.FINISHED;
        PageBuilder page;
        if (schema.isPresent()) {
            page = new PageBuilder(1, ImmutableList.of(BIGINT, VARCHAR));
            page.declarePosition();
            BIGINT.writeLong(page.getBlockBuilder(0), rowCount);
            VARCHAR.writeSlice(page.getBlockBuilder(1), Slices.utf8Slice(schema.get()));
        }
        else {
            page = new PageBuilder(1, ImmutableList.of(BIGINT));
            page.declarePosition();
            BIGINT.writeLong(page.getBlockBuilder(0), rowCount);
        }
        return page.build();
    }

    @Override
    public void finish()
    {
        if (state == State.RUNNING) {
            state = State.FINISHING;
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }
}
