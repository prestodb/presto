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
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.List;

import static com.facebook.presto.operator.LookupJoinOperators.JoinType.FULL_OUTER;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static java.util.Objects.requireNonNull;

public class LookupJoinOperator
        implements Operator, Closeable
{
    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final ListenableFuture<? extends LookupSource> lookupSourceFuture;
    private final JoinProbeFactory joinProbeFactory;
    private final Runnable onClose;

    private final PageBuilder pageBuilder;

    private final boolean probeOnOuterSide;

    private LookupSource lookupSource;
    private JoinProbe probe;

    private boolean closed;
    private boolean finishing;
    private long joinPosition = -1;

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
        // Cannot use switch case here, because javac will synthesize an inner class and cause IllegalAccessError
        probeOnOuterSide = joinType == PROBE_OUTER || joinType == FULL_OUTER;

        this.lookupSourceFuture = requireNonNull(lookupSourceFuture, "lookupSourceFuture is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.onClose = requireNonNull(onClose, "onClose is null");

        this.pageBuilder = new PageBuilder(types);
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
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        boolean finished = finishing && probe == null && pageBuilder.isEmpty();

        // if finished drop references so memory is freed early
        if (finished) {
            close();
        }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return lookupSourceFuture;
    }

    @Override
    public boolean needsInput()
    {
        if (finishing) {
            return false;
        }

        if (lookupSource == null) {
            lookupSource = tryGetFutureValue(lookupSourceFuture).orElse(null);
        }
        return lookupSource != null && probe == null;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(lookupSource != null, "Lookup source has not been built yet");
        checkState(probe == null, "Current page has not been completely processed yet");

        // create probe
        probe = joinProbeFactory.createJoinProbe(lookupSource, page);

        // initialize to invalid join position to force output code to advance the cursors
        joinPosition = -1;
    }

    @Override
    public Page getOutput()
    {
        if (lookupSource == null) {
            return null;
        }

        // join probe page with the lookup source
        if (probe != null) {
            while (joinCurrentPosition()) {
                if (!advanceProbePosition()) {
                    break;
                }
                if (!outerJoinCurrentPosition()) {
                    break;
                }
            }
        }

        // only flush full pages unless we are done
        if (pageBuilder.isFull() || (finishing && !pageBuilder.isEmpty() && probe == null)) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }

        return null;
    }

    @Override
    public void close()
    {
        // Closing the lookupSource is always safe to do, but we don't want to release the supplier multiple times, since its reference counted
        if (closed) {
            return;
        }
        closed = true;
        probe = null;
        pageBuilder.reset();
        // closing lookup source is only here for index join
        if (lookupSource != null) {
            lookupSource.close();
        }
        onClose.run();
    }

    private boolean joinCurrentPosition()
    {
        // while we have a position to join against...
        while (joinPosition >= 0) {
            pageBuilder.declarePosition();

            // write probe columns
            probe.appendTo(pageBuilder);

            // write build columns
            lookupSource.appendTo(joinPosition, pageBuilder, probe.getOutputChannelCount());

            // get next join position for this row
            joinPosition = lookupSource.getNextJoinPosition(joinPosition, probe.getPosition(), probe.getPage());
            if (pageBuilder.isFull()) {
                return false;
            }
        }
        return true;
    }

    private boolean advanceProbePosition()
    {
        if (!probe.advanceNextPosition()) {
            probe = null;
            return false;
        }

        // update join position
        joinPosition = probe.getCurrentJoinPosition();
        return true;
    }

    private boolean outerJoinCurrentPosition()
    {
        if (probeOnOuterSide && joinPosition < 0) {
            // write probe columns
            pageBuilder.declarePosition();
            probe.appendTo(pageBuilder);

            // write nulls into build columns
            int outputIndex = probe.getOutputChannelCount();
            for (int buildChannel = 0; buildChannel < lookupSource.getChannelCount(); buildChannel++) {
                pageBuilder.getBlockBuilder(outputIndex).appendNull();
                outputIndex++;
            }
            if (pageBuilder.isFull()) {
                return false;
            }
        }
        return true;
    }
}
