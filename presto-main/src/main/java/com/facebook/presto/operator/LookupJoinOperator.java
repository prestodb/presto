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

import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.List;

import static com.facebook.presto.operator.LookupJoinOperators.JoinType.FULL_OUTER;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static java.util.Objects.requireNonNull;

public class LookupJoinOperator
        implements Operator, Closeable
{
    private final LookupJoiner lookupJoiner;
    private final OperatorContext operatorContext;
    private final List<Type> types;

    private final Runnable onClose;

    private boolean closed;

    public LookupJoinOperator(
            OperatorContext operatorContext,
            List<Type> types,
            JoinType joinType,
            ListenableFuture<LookupSource> lookupSourceFuture,
            JoinProbeFactory joinProbeFactory,
            Runnable onClose)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));

        requireNonNull(joinType, "joinType is null");

        this.onClose = requireNonNull(onClose, "onClose is null");

        this.lookupJoiner = new LookupJoiner(types, lookupSourceFuture, joinProbeFactory, joinType == PROBE_OUTER || joinType == FULL_OUTER);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        lookupJoiner.finish();
    }

    @Override
    public boolean isFinished()
    {
        boolean finished = lookupJoiner.isFinished();

        // if finished drop references so memory is freed early
        if (finished) {
            close();
        }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return lookupJoiner.isBlocked();
    }

    @Override
    public boolean needsInput()
    {
        return lookupJoiner.needsInput();
    }

    @Override
    public void addInput(Page page)
    {
        lookupJoiner.addInput(page);
    }

    @Override
    public Page getOutput()
    {
        return lookupJoiner.getOutput();
    }

    @Override
    public void close()
    {
        // Closing the lookupSource is always safe to do, but we don't want to release the supplier multiple times, since its reference counted
        if (closed) {
            return;
        }
        closed = true;
        lookupJoiner.close();
        onClose.run();
    }
}
