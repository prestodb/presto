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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.transform;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static java.util.Objects.requireNonNull;

public class LookupOuterOperator
        implements Operator
{
    public static class LookupOuterOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final OuterLookupSourceSupplier lookupSourceSupplier;
        private final List<Type> probeTypes;
        private final List<Type> types;
        private boolean closed;

        public LookupOuterOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                OuterLookupSourceSupplier lookupSourceSupplier,
                List<Type> probeTypes)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.lookupSourceSupplier = requireNonNull(lookupSourceSupplier, "lookupSourceSupplier is null");
            this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));

            this.types = ImmutableList.<Type>builder()
                    .addAll(probeTypes)
                    .addAll(lookupSourceSupplier.getTypes())
                    .build();
        }

        public int getOperatorId()
        {
            return operatorId;
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LookupOuterOperator.class.getSimpleName());
            return new LookupOuterOperator(operatorContext, lookupSourceSupplier, probeTypes);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new LookupOuterOperatorFactory(operatorId, planNodeId, lookupSourceSupplier, probeTypes);
        }
    }

    private final OperatorContext operatorContext;
    private final ListenableFuture<OuterPositionIterator> outerPositionsFuture;

    private final List<Type> types;
    private final List<Type> probeTypes;

    private final PageBuilder pageBuilder;

    private OuterPositionIterator outerPositions;

    public LookupOuterOperator(
            OperatorContext operatorContext,
            OuterLookupSourceSupplier lookupSourceSupplier,
            List<Type> probeTypes)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");

        requireNonNull(lookupSourceSupplier, "lookupSourceSupplier is null");
        this.outerPositionsFuture = lookupSourceSupplier.getOuterPositions(operatorContext);

        this.types = ImmutableList.<Type>builder()
                .addAll(probeTypes)
                .addAll(lookupSourceSupplier.getTypes())
                .build();
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
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
    public ListenableFuture<?> isBlocked()
    {
        return outerPositionsFuture;
    }

    @Override
    public void finish()
    {
    }

    @Override
    public boolean isFinished()
    {
        return outerPositions != null && !outerPositions.hasNext() && pageBuilder.isEmpty();
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
        if (outerPositions == null) {
            outerPositions = tryGetFutureValue(outerPositionsFuture).orElse(null);
            if (outerPositions == null) {
                return null;
            }
        }

        while (!pageBuilder.isFull() && outerPositions.hasNext()) {
            long buildSideOuterJoinPosition = outerPositions.next();
            pageBuilder.declarePosition();

            // write nulls into probe columns
            // todo use RLE blocks
            for (int probeChannel = 0; probeChannel < probeTypes.size(); probeChannel++) {
                pageBuilder.getBlockBuilder(probeChannel).appendNull();
            }

            // write build columns
            outerPositions.appendTo(buildSideOuterJoinPosition, pageBuilder, probeTypes.size());
        }

        // only flush full pages unless we are done
        if (pageBuilder.isFull() || (!outerPositions.hasNext() && !pageBuilder.isEmpty())) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }

        return null;
    }

    @Override
    public void close()
    {
        Futures.addCallback(outerPositionsFuture, new FutureCallback<OuterPositionIterator>()
        {
            @Override
            public void onSuccess(OuterPositionIterator outerPositions)
            {
                outerPositions.close();
            }

            @Override
            public void onFailure(Throwable t)
            {
            }
        });

        pageBuilder.reset();
    }

    public static class OuterLookupSourceSupplier
            implements LookupSourceSupplier
    {
        private final LookupSourceSupplier lookupSourceSupplier;
        private final AtomicInteger referenceCount = new AtomicInteger(1);

        private final SettableFuture<OuterPositionIterator> outerPositionsFuture = SettableFuture.create();

        @GuardedBy("this")
        private ListenableFuture<LookupSource> outerLookupFuture;

        @GuardedBy("this")
        private OuterPositionTracker positionTracker;

        public OuterLookupSourceSupplier(LookupSourceSupplier lookupSourceSupplier)
        {
            this.lookupSourceSupplier = requireNonNull(lookupSourceSupplier, "lookupSourceSupplier is null");
        }

        @Override
        public List<Type> getTypes()
        {
            return lookupSourceSupplier.getTypes();
        }

        @Override
        public ListenableFuture<LookupSource> getLookupSource(OperatorContext operatorContext)
        {
            // wrapper lookup source to track used positions
            return transform(lookupSourceSupplier.getLookupSource(operatorContext),
                    (Function<LookupSource, LookupSource>) input -> new OuterLookupSource(getPositionTracker(input)));
        }

        public synchronized SettableFuture<OuterPositionIterator> getOuterPositions(OperatorContext operatorContext)
        {
            checkState(outerLookupFuture == null, "Outer positions can only be fetched once");
            outerLookupFuture = getLookupSource(operatorContext);

            updateOuterPositionState();

            return outerPositionsFuture;
        }

        private synchronized OuterPositionTracker getPositionTracker(LookupSource lookupSource)
        {
            if (positionTracker == null) {
                positionTracker = new OuterPositionTracker(checkType(lookupSource, SharedLookupSource.class, "lookupSource"));
            }
            return positionTracker;
        }

        @Override
        public void retain()
        {
            referenceCount.incrementAndGet();
        }

        @Override
        public void release()
        {
            if (referenceCount.decrementAndGet() == 0) {
                updateOuterPositionState();
            }
        }

        private synchronized void updateOuterPositionState()
        {
            // final release may happen before get outer position is called
            if (referenceCount.get() != 0 || outerLookupFuture == null) {
                return;
            }

            // in the case of no probe splits (i.e., an empty table), the lookup source may
            // not have finished building yet.  When it does finish, set the positions future
            // so the build-outer rows can be emitted.
            Futures.addCallback(outerLookupFuture, new FutureCallback<LookupSource>()
            {
                @Override
                public void onSuccess(LookupSource result)
                {
                    outerPositionsFuture.set(getPositionTracker(result).getOuterPositions());
                }

                @Override
                public void onFailure(Throwable t)
                {
                    outerPositionsFuture.setException(t);
                }
            });
        }
    }

    private static class OuterLookupSource
            implements LookupSource
    {
        private final OuterPositionTracker positionTracker;
        private final LookupSource lookupSource;

        public OuterLookupSource(OuterPositionTracker positionTracker)
        {
            this.positionTracker = positionTracker;
            this.lookupSource = positionTracker.getLookupSource();
        }

        @Override
        public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
        {
            lookupSource.appendTo(position, pageBuilder, outputChannelOffset);
            positionTracker.visit(Ints.checkedCast(position));
        }

        @Override
        public int getChannelCount()
        {
            return lookupSource.getChannelCount();
        }

        @Override
        public int getJoinPositionCount()
        {
            return lookupSource.getJoinPositionCount();
        }

        @Override
        public long getJoinPosition(int position, Page page)
        {
            return lookupSource.getJoinPosition(position, page);
        }

        @Override
        public long getInMemorySizeInBytes()
        {
            return lookupSource.getInMemorySizeInBytes();
        }

        @Override
        public long getNextJoinPosition(long currentPosition)
        {
            return lookupSource.getNextJoinPosition(currentPosition);
        }

        @Override
        public long getJoinPosition(int position, Page page, long rawHash)
        {
            return lookupSource.getJoinPosition(position, page, rawHash);
        }

        @Override
        public void close()
        {
            lookupSource.close();
        }
    }

    private static class OuterPositionTracker
    {
        private final SharedLookupSource lookupSource;
        private final boolean[] visitedPositions;
        private boolean closed;

        public OuterPositionTracker(SharedLookupSource lookupSource)
        {
            this.lookupSource = requireNonNull(lookupSource, "lookupSource is null");
            this.visitedPositions = new boolean[lookupSource.getJoinPositionCount()];
        }

        public SharedLookupSource getLookupSource()
        {
            return lookupSource;
        }

        public synchronized void visit(int position)
        {
            checkState(!closed, "Position tracker is closed");
            visitedPositions[position] = true;
        }

        public synchronized OuterPositionIterator getOuterPositions()
        {
            closed = true;
            return new OuterPositionIterator(lookupSource, visitedPositions);
        }
    }

    private static class OuterPositionIterator
    {
        private final SharedLookupSource lookupSource;
        private final boolean[] visitedPositions;
        private int currentPosition;
        private boolean closed;

        public OuterPositionIterator(SharedLookupSource lookupSource, boolean[] visitedPositions)
        {
            this.lookupSource = requireNonNull(lookupSource, "lookupSource is null");
            this.visitedPositions = requireNonNull(visitedPositions, "visitedPositions is null");
        }

        public boolean hasNext()
        {
            while (currentPosition < visitedPositions.length) {
                if (!visitedPositions[currentPosition]) {
                    return true;
                }
                currentPosition++;
            }
            return false;
        }

        public int next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            int result = currentPosition;
            currentPosition++;
            return result;
        }

        public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
        {
            lookupSource.appendTo(position, pageBuilder, outputChannelOffset);
        }

        public void close()
        {
            // memory can only be freed once
            if (!closed) {
                closed = true;
                lookupSource.freeMemory();
            }
        }
    }
}
