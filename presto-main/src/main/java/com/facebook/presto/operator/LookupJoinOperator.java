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
import it.unimi.dsi.fastutil.ints.IntArrayList;

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
    private static final int MAX_POSITIONS_EVALUATED_PER_CALL = 10000;

    private final OperatorContext operatorContext;
    private final List<Type> probeTypes;
    private final List<Type> buildTypes;
    private final ListenableFuture<? extends LookupSource> lookupSourceFuture;
    private final JoinProbeFactory joinProbeFactory;
    private final Runnable onClose;

    private final IntArrayList probeIndexBuilder = new IntArrayList();
    private final PageBuilder buildPageBuilder;

    private final boolean probeOnOuterSide;

    private LookupSource lookupSource;
    private JoinProbe probe;
    private boolean reachedProbeEnd = true;

    private boolean closed;
    private boolean finishing;
    private long joinPosition = -1;

    private boolean currentProbePositionProducedRow;

    public LookupJoinOperator(
            OperatorContext operatorContext,
            List<Type> probeTypes,
            List<Type> buildTypes,
            JoinType joinType,
            ListenableFuture<LookupSource> lookupSourceFuture,
            JoinProbeFactory joinProbeFactory,
            Runnable onClose)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
        this.buildTypes = ImmutableList.copyOf(requireNonNull(buildTypes, "buildTypes is null"));

        requireNonNull(joinType, "joinType is null");
        // Cannot use switch case here, because javac will synthesize an inner class and cause IllegalAccessError
        probeOnOuterSide = joinType == PROBE_OUTER || joinType == FULL_OUTER;

        this.lookupSourceFuture = requireNonNull(lookupSourceFuture, "lookupSourceFuture is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.onClose = requireNonNull(onClose, "onClose is null");

        this.buildPageBuilder = new PageBuilder(buildTypes);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.<Type>builder()
                .addAll(probeTypes)
                .addAll(buildTypes)
                .build();
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        boolean finished = finishing && reachedProbeEnd && buildPageBuilder.isEmpty();

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
        return lookupSource != null && reachedProbeEnd;
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
        reachedProbeEnd = false;

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
        Counter lookupPositionsConsidered = new Counter();
        if (probe != null) {
            while (true) {
                if (probe.getPosition() >= 0) {
                    if (!joinCurrentPosition(lookupPositionsConsidered)) {
                        break;
                    }
                    if (!currentProbePositionProducedRow) {
                        currentProbePositionProducedRow = true;
                        if (!outerJoinCurrentPosition()) {
                            break;
                        }
                    }
                }
                currentProbePositionProducedRow = false;
                if (!advanceProbePosition()) {
                    break;
                }
            }

            // only flush full pages unless we are done
            if (readyToBuildOutputPage()) {
                Page outputPage = probe.buildDictionaryPage(probeIndexBuilder.toIntArray(null), buildPageBuilder);
                reset();
                return outputPage;
            }
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
        reachedProbeEnd = true;
        reset();
        // closing lookup source is only here for index join
        if (lookupSource != null) {
            lookupSource.close();
        }
        onClose.run();
    }

    private boolean readyToBuildOutputPage()
    {
        // Because of dictionary, always output when probe reached the end. Indexes cannot span multiple probe pages.
        return buildPageBuilder.isFull() || reachedProbeEnd || (finishing && !buildPageBuilder.isEmpty());
    }

    /**
     * Produce rows matching join condition for the current probe position. If this method was called previously
     * for the current probe position, calling this again will produce rows that wasn't been produced in previous
     * invocations.
     *
     * @return true if all eligible rows have been produced; false otherwise (because pageBuilder became full)
     */
    private boolean joinCurrentPosition(Counter lookupPositionsConsidered)
    {
        // while we have a position on lookup side to join against...
        while (joinPosition >= 0) {
            lookupPositionsConsidered.increment();
            if (lookupSource.isJoinPositionEligible(joinPosition, probe.getPosition(), probe.getPage())) {
                currentProbePositionProducedRow = true;

                probeIndexBuilder.add(probe.getPosition());
                // write build columns
                buildPageBuilder.declarePosition();
                lookupSource.appendTo(joinPosition, buildPageBuilder, 0);
            }

            // get next position on lookup side for this probe row
            joinPosition = lookupSource.getNextJoinPosition(joinPosition, probe.getPosition(), probe.getPage());

            if (lookupPositionsConsidered.get() >= MAX_POSITIONS_EVALUATED_PER_CALL) {
                return false;
            }
            if (buildPageBuilder.isFull()) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return whether there are more positions on probe side
     */
    private boolean advanceProbePosition()
    {
        if (!probe.advanceNextPosition()) {
            reachedProbeEnd = true;
            return false;
        }

        // update join position
        joinPosition = probe.getCurrentJoinPosition();
        return true;
    }

    /**
     * Produce a row for the current probe position, if it doesn't match any row on lookup side and this is an outer join.
     *
     * @return whether pageBuilder became full
     */
    private boolean outerJoinCurrentPosition()
    {
        if (probeOnOuterSide && joinPosition < 0) {
            // write probe columns
            probeIndexBuilder.add(probe.getPosition());

            // write nulls into build columns
            buildPageBuilder.declarePosition();
            for (int buildChannel = 0; buildChannel < lookupSource.getChannelCount(); buildChannel++) {
                buildPageBuilder.getBlockBuilder(buildChannel).appendNull();
            }
            if (buildPageBuilder.isFull()) {
                return false;
            }
        }
        return true;
    }

    private void reset()
    {
        probeIndexBuilder.clear();
        buildPageBuilder.reset();
        // Reset source page when probe reached the end.
        if (reachedProbeEnd) {
            probe = null;
        }
    }

    // This class needs to be public because LookupJoinOperator is isolated.
    public static class Counter
    {
        private int count;

        public void increment()
        {
            count++;
        }

        public int get()
        {
            return count;
        }
    }
}
