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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.SingleStreamSpiller;
import com.facebook.presto.spiller.SingleStreamSpillerFactory;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.checkSuccess;
import static io.airlift.concurrent.MoreFutures.getDone;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class HashBuilderOperator
        implements Operator
{
    public static class HashBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PartitionedLookupSourceFactory lookupSourceFactory;
        private final List<Integer> outputChannels;
        private final List<Integer> hashChannels;
        private final OptionalInt preComputedHashChannel;
        private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
        private final Optional<Integer> sortChannel;
        private final List<JoinFilterFunctionFactory> searchFunctionFactories;
        private final PagesIndex.Factory pagesIndexFactory;

        private final int expectedPositions;
        private final boolean spillEnabled;
        private final SingleStreamSpillerFactory singleStreamSpillerFactory;

        private int partitionIndex;
        private boolean closed;

        public HashBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> types,
                List<Integer> outputChannels,
                Map<Symbol, Integer> layout,
                List<Integer> hashChannels,
                OptionalInt preComputedHashChannel,
                boolean outer,
                Optional<JoinFilterFunctionFactory> filterFunctionFactory,
                Optional<Integer> sortChannel,
                List<JoinFilterFunctionFactory> searchFunctionFactories,
                int expectedPositions,
                int partitionCount,
                PagesIndex.Factory pagesIndexFactory,
                boolean spillEnabled,
                SingleStreamSpillerFactory singleStreamSpillerFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            requireNonNull(sortChannel, "sortChannel can not be null");
            requireNonNull(searchFunctionFactories, "searchFunctionFactories is null");
            checkArgument(sortChannel.isPresent() != searchFunctionFactories.isEmpty(), "both or none sortChannel and searchFunctionFactories must be set");
            checkArgument(Integer.bitCount(partitionCount) == 1, "partitionCount must be a power of 2");
            lookupSourceFactory = new PartitionedLookupSourceFactory(
                    types,
                    outputChannels.stream()
                            .map(types::get)
                            .collect(toImmutableList()),
                    hashChannels,
                    partitionCount,
                    requireNonNull(layout, "layout is null"),
                    outer);

            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
            this.preComputedHashChannel = requireNonNull(preComputedHashChannel, "preComputedHashChannel is null");
            this.filterFunctionFactory = requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");
            this.sortChannel = sortChannel;
            this.searchFunctionFactories = ImmutableList.copyOf(searchFunctionFactories);
            this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
            this.spillEnabled = spillEnabled;
            this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");

            this.expectedPositions = expectedPositions;
        }

        public LookupSourceFactory getLookupSourceFactory()
        {
            return lookupSourceFactory;
        }

        @Override
        public List<Type> getTypes()
        {
            return lookupSourceFactory.getTypes();
        }

        @Override
        public HashBuilderOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashBuilderOperator.class.getSimpleName());
            HashBuilderOperator operator = new HashBuilderOperator(
                    operatorContext,
                    lookupSourceFactory,
                    partitionIndex,
                    outputChannels,
                    hashChannels,
                    preComputedHashChannel,
                    filterFunctionFactory,
                    sortChannel,
                    searchFunctionFactories,
                    expectedPositions,
                    pagesIndexFactory,
                    spillEnabled,
                    singleStreamSpillerFactory);

            partitionIndex++;
            return operator;
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Parallel hash build can not be duplicated");
        }
    }

    @VisibleForTesting
    public enum State
    {
        /**
         * Operator accepts input
         */
        CONSUMING_INPUT,

        /**
         * Memory revoking occurred during {@link #CONSUMING_INPUT}. Operator accepts input and spills it
         */
        SPILLING_INPUT,

        /**
         * LookupSource has been built and passed on without any spill occurring
         */
        LOOKUP_SOURCE_BUILT,

        /**
         * Input has been finished and spilled
         */
        INPUT_SPILLED,

        /**
         * Spilled input is being unspilled
         */
        INPUT_UNSPILLING,

        /**
         * Spilled input has been unspilled, LookupSource built from it
         */
        INPUT_UNSPILLED_AND_BUILT,

        /**
         * No longer needed
         */
        DISPOSED
    }

    private static final double INDEX_COMPACTION_ON_REVOCATION_TARGET = 0.8;

    private final OperatorContext operatorContext;
    private final PartitionedLookupSourceFactory lookupSourceFactory;
    private final ListenableFuture<?> lookupSourceFactoryDestroyed;
    private final int partitionIndex;

    private final List<Integer> outputChannels;
    private final List<Integer> hashChannels;
    private final OptionalInt preComputedHashChannel;
    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
    private final Optional<Integer> sortChannel;
    private final List<JoinFilterFunctionFactory> searchFunctionFactories;

    private final PagesIndex index;

    private final boolean spillEnabled;
    private final SingleStreamSpillerFactory singleStreamSpillerFactory;

    private final HashCollisionsCounter hashCollisionsCounter;

    private State state = State.CONSUMING_INPUT;
    private Optional<ListenableFuture<?>> lookupSourceNotNeeded = Optional.empty();
    private SpilledLookupSourceHandle spilledLookupSourceHandle = new SpilledLookupSourceHandle();
    private Optional<SingleStreamSpiller> spiller = Optional.empty();
    private ListenableFuture<?> spillInProgress = NOT_BLOCKED;
    private Optional<ListenableFuture<List<Page>>> unspillInProgress = Optional.empty();
    @Nullable
    private LookupSourceSupplier lookupSourceSupplier;
    private OptionalLong lookupSourceChecksum = OptionalLong.empty();

    private Optional<Runnable> finishMemoryRevoke = Optional.empty();

    public HashBuilderOperator(
            OperatorContext operatorContext,
            PartitionedLookupSourceFactory lookupSourceFactory,
            int partitionIndex,
            List<Integer> outputChannels,
            List<Integer> hashChannels,
            OptionalInt preComputedHashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories,
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory,
            boolean spillEnabled,
            SingleStreamSpillerFactory singleStreamSpillerFactory)
    {
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");

        this.operatorContext = operatorContext;
        this.partitionIndex = partitionIndex;
        this.filterFunctionFactory = filterFunctionFactory;
        this.sortChannel = sortChannel;
        this.searchFunctionFactories = searchFunctionFactories;

        this.index = pagesIndexFactory.newPagesIndex(lookupSourceFactory.getTypes(), expectedPositions);
        this.lookupSourceFactory = lookupSourceFactory;
        lookupSourceFactoryDestroyed = lookupSourceFactory.isDestroyed();

        this.outputChannels = outputChannels;
        this.hashChannels = hashChannels;
        this.preComputedHashChannel = preComputedHashChannel;

        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);

        this.spillEnabled = spillEnabled;
        this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return lookupSourceFactory.getTypes();
    }

    @VisibleForTesting
    public State getState()
    {
        return state;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        switch (state) {
            case CONSUMING_INPUT:
                return NOT_BLOCKED;

            case SPILLING_INPUT:
                return spillInProgress;

            case LOOKUP_SOURCE_BUILT:
                return lookupSourceNotNeeded.orElseThrow(() -> new IllegalStateException("Lookup source built, but disposal future not set"));

            case INPUT_SPILLED:
                return spilledLookupSourceHandle.getUnspillingOrDisposeRequested();

            case INPUT_UNSPILLING:
                return unspillInProgress.orElseThrow(() -> new IllegalStateException("Unspilling in progress, but unspilling future not set"));

            case INPUT_UNSPILLED_AND_BUILT:
                return spilledLookupSourceHandle.getDisposeRequested();

            case DISPOSED:
                return lookupSourceFactoryDestroyed;
        }
        throw new IllegalStateException("Unhandled state: " + state);
    }

    @Override
    public boolean needsInput()
    {
        boolean stateNeedsInput = (state == State.CONSUMING_INPUT)
                || (state == State.SPILLING_INPUT && spillInProgress.isDone());

        return stateNeedsInput && !lookupSourceFactoryDestroyed.isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");

        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        if (state == State.SPILLING_INPUT) {
            spillInput(page);
            return;
        }

        checkState(state == State.CONSUMING_INPUT);
        updateIndex(page);
    }

    private void updateIndex(Page page)
    {
        index.addPage(page);

        if (spillEnabled) {
            operatorContext.setRevocableMemoryReservation(index.getEstimatedSize().toBytes());
        }
        else {
            if (!operatorContext.trySetMemoryReservation(index.getEstimatedSize().toBytes())) {
                index.compact();
                operatorContext.setMemoryReservation(index.getEstimatedSize().toBytes());
            }
        }
        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    private void spillInput(Page page)
    {
        checkState(spillInProgress.isDone(), "Previous spill still in progress");
        checkSuccess(spillInProgress, "spilling failed");
        spillInProgress = getSpiller().spill(page);
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        checkState(spillEnabled, "Spill not enabled, no revokable memory should be reserved");

        if (state == State.CONSUMING_INPUT) {
            long indexSizeBeforeCompaction = index.getEstimatedSize().toBytes();
            index.compact();
            long indexSizeAfterCompaction = index.getEstimatedSize().toBytes();
            if (indexSizeAfterCompaction < indexSizeBeforeCompaction * INDEX_COMPACTION_ON_REVOCATION_TARGET) {
                finishMemoryRevoke = Optional.of(() -> {});
                return immediateFuture(null);
            }

            finishMemoryRevoke = Optional.of(() -> {
                index.clear();
                operatorContext.setMemoryReservation(index.getEstimatedSize().toBytes());
                operatorContext.setRevocableMemoryReservation(0L);
                lookupSourceFactory.setPartitionSpilledLookupSourceHandle(partitionIndex, spilledLookupSourceHandle);
                state = State.SPILLING_INPUT;
            });
            return spillIndex();
        }
        else if (state == State.LOOKUP_SOURCE_BUILT) {
            finishMemoryRevoke = Optional.of(() -> {
                lookupSourceFactory.setPartitionSpilledLookupSourceHandle(partitionIndex, spilledLookupSourceHandle);
                lookupSourceNotNeeded = Optional.empty();
                index.clear();
                operatorContext.setMemoryReservation(index.getEstimatedSize().toBytes());
                operatorContext.setRevocableMemoryReservation(0L);
                lookupSourceChecksum = OptionalLong.of(lookupSourceSupplier.checksum());
                lookupSourceSupplier = null;
                state = State.INPUT_SPILLED;
            });
            return spillIndex();
        }
        else if (operatorContext.getReservedRevocableBytes() == 0) {
            // Probably stale revoking request
            finishMemoryRevoke = Optional.of(() -> {});
            return immediateFuture(null);
        }

        throw new IllegalStateException(format("State %s can not have revocable memory, but has %s revocable bytes", state, operatorContext.getReservedRevocableBytes()));
    }

    private ListenableFuture<?> spillIndex()
    {
        checkState(!spiller.isPresent(), "Spiller already created");
        spiller = Optional.of(singleStreamSpillerFactory.create(
                index.getTypes(),
                operatorContext.getSpillContext().newLocalSpillContext(),
                operatorContext.getSystemMemoryContext().newLocalMemoryContext()));
        return getSpiller().spill(index.getPages());
    }

    @Override
    public void finishMemoryRevoke()
    {
        checkState(finishMemoryRevoke.isPresent(), "Cannot finish unknown revoking");
        finishMemoryRevoke.get().run();
        finishMemoryRevoke = Optional.empty();
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void finish()
    {
        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        if (finishMemoryRevoke.isPresent()) {
            return;
        }

        switch (state) {
            case CONSUMING_INPUT:
                finishInput();
                return;

            case LOOKUP_SOURCE_BUILT:
                disposeLookupSourceIfRequested();
                return;

            case SPILLING_INPUT:
                finishSpilledInput();
                return;

            case INPUT_SPILLED:
                if (spilledLookupSourceHandle.getDisposeRequested().isDone()) {
                    close();
                }
                else {
                    unspillLookupSourceIfRequested();
                }
                return;

            case INPUT_UNSPILLING:
                finishLookupSourceUnspilling();
                return;

            case INPUT_UNSPILLED_AND_BUILT:
                disposeUnspilledLookupSourceIfRequested();
                return;

            case DISPOSED:
                // no-op
                return;
        }

        throw new IllegalStateException("Unhandled state: " + state);
    }

    private void finishInput()
    {
        checkState(state == State.CONSUMING_INPUT);
        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        LookupSourceSupplier partition = buildLookupSource();
        if (spillEnabled) {
            operatorContext.setRevocableMemoryReservation(partition.get().getInMemorySizeInBytes());
        }
        else {
            operatorContext.setMemoryReservation(partition.get().getInMemorySizeInBytes());
        }
        lookupSourceNotNeeded = Optional.of(lookupSourceFactory.lendPartitionLookupSource(partitionIndex, partition));

        state = State.LOOKUP_SOURCE_BUILT;
    }

    private void disposeLookupSourceIfRequested()
    {
        checkState(state == State.LOOKUP_SOURCE_BUILT);
        verify(lookupSourceNotNeeded.isPresent());
        if (!lookupSourceNotNeeded.get().isDone()) {
            return;
        }

        index.clear();
        operatorContext.setRevocableMemoryReservation(0L);
        operatorContext.setMemoryReservation(index.getEstimatedSize().toBytes());
        lookupSourceSupplier = null;
        state = State.DISPOSED;
    }

    private void finishSpilledInput()
    {
        checkState(state == State.SPILLING_INPUT);
        if (!spillInProgress.isDone()) {
            // Not ready to handle finish() yet
            return;
        }
        checkSuccess(spillInProgress, "spilling failed");
        state = State.INPUT_SPILLED;
    }

    private void unspillLookupSourceIfRequested()
    {
        checkState(state == State.INPUT_SPILLED);
        if (!spilledLookupSourceHandle.getUnspillingRequested().isDone()) {
            // Nothing to do yet.
            return;
        }

        verify(spiller.isPresent());
        verify(!unspillInProgress.isPresent());

        operatorContext.setMemoryReservation(getSpiller().getSpilledPagesInMemorySize() + index.getEstimatedSize().toBytes());
        unspillInProgress = Optional.of(getSpiller().getAllSpilledPages());

        state = State.INPUT_UNSPILLING;
    }

    private void finishLookupSourceUnspilling()
    {
        checkState(state == State.INPUT_UNSPILLING);
        if (!unspillInProgress.get().isDone()) {
            // Pages have not be unspilled yet.
            return;
        }

        // Use Queue so that Pages already consumed by Index are not retained by us.
        Queue<Page> pages = new ArrayDeque<>(getDone(unspillInProgress.get()));
        long memoryRetainedByRemainingPages = pages.stream()
                .mapToLong(Page::getRetainedSizeInBytes)
                .sum();
        operatorContext.setMemoryReservation(memoryRetainedByRemainingPages + index.getEstimatedSize().toBytes());

        while (!pages.isEmpty()) {
            Page next = pages.remove();
            index.addPage(next);
            // There is no attempt to compact index, since unspilled pages are unlikely to have blocks with retained size > logical size.
            memoryRetainedByRemainingPages -= next.getRetainedSizeInBytes();
            operatorContext.setMemoryReservation(memoryRetainedByRemainingPages + index.getEstimatedSize().toBytes());
        }

        LookupSourceSupplier partition = buildLookupSource();
        lookupSourceChecksum.ifPresent(checksum ->
                checkState(partition.checksum() == checksum, "Unspilled lookupSource checksum does not match original one"));
        operatorContext.setMemoryReservation(partition.get().getInMemorySizeInBytes());

        spilledLookupSourceHandle.setLookupSource(partition);

        state = State.INPUT_UNSPILLED_AND_BUILT;
    }

    private void disposeUnspilledLookupSourceIfRequested()
    {
        checkState(state == State.INPUT_UNSPILLED_AND_BUILT);
        if (!spilledLookupSourceHandle.getDisposeRequested().isDone()) {
            return;
        }

        index.clear();
        operatorContext.setMemoryReservation(index.getEstimatedSize().toBytes());

        state = State.DISPOSED;
    }

    private LookupSourceSupplier buildLookupSource()
    {
        LookupSourceSupplier partition = index.createLookupSourceSupplier(operatorContext.getSession(), hashChannels, preComputedHashChannel, filterFunctionFactory, sortChannel, searchFunctionFactories, Optional.of(outputChannels));
        hashCollisionsCounter.recordHashCollision(partition.getHashCollisions(), partition.getExpectedHashCollisions());
        checkState(lookupSourceSupplier == null, "lookupSourceSupplier is already set");
        this.lookupSourceSupplier = partition;
        return partition;
    }

    @Override
    public boolean isFinished()
    {
        if (lookupSourceFactoryDestroyed.isDone()) {
            // Finish early when the probe side is empty
            close();
            return true;
        }

        return state == State.DISPOSED;
    }

    private SingleStreamSpiller getSpiller()
    {
        return spiller.orElseThrow(() -> new IllegalStateException("Spiller not created"));
    }

    @Override
    public void close()
    {
        // close() can be called in any state, due for example to query failure, and must clean resource up unconditionally

        lookupSourceSupplier = null;
        state = State.DISPOSED;
        finishMemoryRevoke = finishMemoryRevoke.map(ifPresent -> () -> {});

        try (Closer closer = Closer.create()) {
            closer.register(index::clear);
            spiller.ifPresent(closer::register);
            closer.register(() -> operatorContext.setMemoryReservation(0));
            closer.register(() -> operatorContext.setRevocableMemoryReservation(0));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
