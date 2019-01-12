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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.operator.JoinProbe.JoinProbeFactory;
import io.prestosql.operator.LookupJoinOperators.JoinType;
import io.prestosql.operator.LookupSourceProvider.LookupSourceLease;
import io.prestosql.operator.PartitionedConsumption.Partition;
import io.prestosql.operator.exchange.LocalPartitionGenerator;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.PartitioningSpiller;
import io.prestosql.spiller.PartitioningSpiller.PartitioningSpillResult;
import io.prestosql.spiller.PartitioningSpillerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.checkSuccess;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.prestosql.operator.LookupJoinOperators.JoinType.FULL_OUTER;
import static io.prestosql.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static java.lang.String.format;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

public class LookupJoinOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final List<Type> probeTypes;
    private final JoinProbeFactory joinProbeFactory;
    private final Runnable afterClose;
    private final OptionalInt lookupJoinsCount;
    private final HashGenerator hashGenerator;
    private final LookupSourceFactory lookupSourceFactory;
    private final PartitioningSpillerFactory partitioningSpillerFactory;

    private final JoinStatisticsCounter statisticsCounter;

    private final LookupJoinPageBuilder pageBuilder;

    private final boolean probeOnOuterSide;

    private final ListenableFuture<LookupSourceProvider> lookupSourceProviderFuture;
    private LookupSourceProvider lookupSourceProvider;
    private JoinProbe probe;

    private Page outputPage;

    private Optional<PartitioningSpiller> spiller = Optional.empty();
    private Optional<LocalPartitionGenerator> partitionGenerator = Optional.empty();
    private ListenableFuture<?> spillInProgress = NOT_BLOCKED;
    private long inputPageSpillEpoch;
    private boolean closed;
    private boolean finishing;
    private boolean unspilling;
    private boolean finished;
    private long joinPosition = -1;
    private int joinSourcePositions;

    private boolean currentProbePositionProducedRow;

    private final Map<Integer, SavedRow> savedRows = new HashMap<>();
    @Nullable
    private ListenableFuture<PartitionedConsumption<Supplier<LookupSource>>> partitionedConsumption;
    @Nullable
    private Iterator<Partition<Supplier<LookupSource>>> lookupPartitions;
    private Optional<Partition<Supplier<LookupSource>>> currentPartition = Optional.empty();
    private Optional<ListenableFuture<Supplier<LookupSource>>> unspilledLookupSource = Optional.empty();
    private Iterator<Page> unspilledInputPages = emptyIterator();

    public LookupJoinOperator(
            OperatorContext operatorContext,
            List<Type> probeTypes,
            List<Type> buildOutputTypes,
            JoinType joinType,
            LookupSourceFactory lookupSourceFactory,
            JoinProbeFactory joinProbeFactory,
            Runnable afterClose,
            OptionalInt lookupJoinsCount,
            HashGenerator hashGenerator,
            PartitioningSpillerFactory partitioningSpillerFactory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));

        requireNonNull(joinType, "joinType is null");
        // Cannot use switch case here, because javac will synthesize an inner class and cause IllegalAccessError
        probeOnOuterSide = joinType == PROBE_OUTER || joinType == FULL_OUTER;

        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.afterClose = requireNonNull(afterClose, "afterClose is null");
        this.lookupJoinsCount = requireNonNull(lookupJoinsCount, "lookupJoinsCount is null");
        this.hashGenerator = requireNonNull(hashGenerator, "hashGenerator is null");
        this.lookupSourceFactory = requireNonNull(lookupSourceFactory, "lookupSourceFactory is null");
        this.partitioningSpillerFactory = requireNonNull(partitioningSpillerFactory, "partitioningSpillerFactory is null");
        this.lookupSourceProviderFuture = lookupSourceFactory.createLookupSourceProvider();

        this.statisticsCounter = new JoinStatisticsCounter(joinType);
        operatorContext.setInfoSupplier(this.statisticsCounter);

        this.pageBuilder = new LookupJoinPageBuilder(buildOutputTypes);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (finishing) {
            return;
        }

        if (!spillInProgress.isDone()) {
            return;
        }

        checkSuccess(spillInProgress, "spilling failed");
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        boolean finished = this.finished && probe == null && pageBuilder.isEmpty() && outputPage == null;

        // if finished drop references so memory is freed early
        if (finished) {
            close();
        }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!spillInProgress.isDone()) {
            // Input spilling can happen only after lookupSourceProviderFuture was done.
            return spillInProgress;
        }
        if (unspilledLookupSource.isPresent()) {
            // Unspilling can happen only after lookupSourceProviderFuture was done.
            return unspilledLookupSource.get();
        }

        if (finishing) {
            return NOT_BLOCKED;
        }

        return lookupSourceProviderFuture;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing
                && lookupSourceProviderFuture.isDone()
                && spillInProgress.isDone()
                && probe == null
                && outputPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(probe == null, "Current page has not been completely processed yet");

        checkState(tryFetchLookupSourceProvider(), "Not ready to handle input yet");

        SpillInfoSnapshot spillInfoSnapshot = lookupSourceProvider.withLease(SpillInfoSnapshot::from);
        addInput(page, spillInfoSnapshot);
    }

    private void addInput(Page page, SpillInfoSnapshot spillInfoSnapshot)
    {
        requireNonNull(spillInfoSnapshot, "spillInfoSnapshot is null");

        if (spillInfoSnapshot.hasSpilled()) {
            page = spillAndMaskSpilledPositions(page, spillInfoSnapshot.getSpillMask());
            if (page.getPositionCount() == 0) {
                return;
            }
        }

        // create probe
        inputPageSpillEpoch = spillInfoSnapshot.getSpillEpoch();
        probe = joinProbeFactory.createJoinProbe(page);

        // initialize to invalid join position to force output code to advance the cursors
        joinPosition = -1;
    }

    private boolean tryFetchLookupSourceProvider()
    {
        if (lookupSourceProvider == null) {
            if (!lookupSourceProviderFuture.isDone()) {
                return false;
            }
            lookupSourceProvider = requireNonNull(getDone(lookupSourceProviderFuture));
            statisticsCounter.updateLookupSourcePositions(lookupSourceProvider.withLease(lookupSourceLease -> lookupSourceLease.getLookupSource().getJoinPositionCount()));
        }
        return true;
    }

    private Page spillAndMaskSpilledPositions(Page page, IntPredicate spillMask)
    {
        checkState(spillInProgress.isDone(), "Previous spill still in progress");
        checkSuccess(spillInProgress, "spilling failed");

        if (!spiller.isPresent()) {
            spiller = Optional.of(partitioningSpillerFactory.create(
                    probeTypes,
                    getPartitionGenerator(),
                    operatorContext.getSpillContext().newLocalSpillContext(),
                    operatorContext.newAggregateSystemMemoryContext()));
        }

        PartitioningSpillResult result = spiller.get().partitionAndSpill(page, spillMask);
        spillInProgress = result.getSpillingFuture();
        return result.getRetained();
    }

    public LocalPartitionGenerator getPartitionGenerator()
    {
        if (!partitionGenerator.isPresent()) {
            partitionGenerator = Optional.of(new LocalPartitionGenerator(hashGenerator, lookupSourceFactory.partitions()));
        }
        return partitionGenerator.get();
    }

    @Override
    public Page getOutput()
    {
        // TODO introduce explicit state (enum), like in HBO

        if (!spillInProgress.isDone()) {
            /*
             * We cannot produce output when there is some previous input spilling. This is because getOutput() may result in additional portion of input being spilled
             * (when spilling state has changed in partitioned lookup source since last time) and spiller does not allow multiple concurrent spills.
             */
            return null;
        }
        checkSuccess(spillInProgress, "spilling failed");

        if (probe == null && pageBuilder.isEmpty() && !finishing) {
            return null;
        }

        if (!tryFetchLookupSourceProvider()) {
            if (!finishing) {
                return null;
            }

            verify(finishing);
            // We are no longer interested in the build side (the lookupSourceProviderFuture's value).
            addSuccessCallback(lookupSourceProviderFuture, LookupSourceProvider::close);
            lookupSourceProvider = new StaticLookupSourceProvider(new EmptyLookupSource());
        }

        if (probe == null && finishing && !unspilling) {
            /*
             * We do not have input probe and we won't have any, as we're finishing.
             * Let LookupSourceFactory know LookupSources can be disposed as far as we're concerned.
             */
            verify(partitionedConsumption == null, "partitioned consumption already started");
            partitionedConsumption = lookupSourceFactory.finishProbeOperator(lookupJoinsCount);
            unspilling = true;
        }

        if (probe == null && unspilling && !finished) {
            /*
             * If no current partition or it was exhausted, unspill next one.
             * Add input there when it needs one, produce output. Be Happy.
             */
            tryUnspillNext();
        }

        if (probe != null) {
            processProbe();
        }

        if (outputPage != null) {
            verify(pageBuilder.isEmpty());
            Page output = outputPage;
            outputPage = null;
            return output;
        }

        // It is impossible to have probe == null && !pageBuilder.isEmpty(),
        // because we will flush a page whenever we reach the probe end
        verify(probe != null || pageBuilder.isEmpty());
        return null;
    }

    private void tryUnspillNext()
    {
        verify(probe == null);

        if (!partitionedConsumption.isDone()) {
            return;
        }

        if (lookupPartitions == null) {
            lookupPartitions = getDone(partitionedConsumption).beginConsumption();
        }

        if (unspilledInputPages.hasNext()) {
            addInput(unspilledInputPages.next());
            return;
        }

        if (unspilledLookupSource.isPresent()) {
            if (!unspilledLookupSource.get().isDone()) {
                // Not unspilled yet
                return;
            }
            LookupSource lookupSource = getDone(unspilledLookupSource.get()).get();
            unspilledLookupSource = Optional.empty();

            // Close previous lookupSourceProvider (either supplied initially or for the previous partition)
            lookupSourceProvider.close();
            lookupSourceProvider = new StaticLookupSourceProvider(lookupSource);
            // If the partition was spilled during processing, its position count will be considered twice.
            statisticsCounter.updateLookupSourcePositions(lookupSource.getJoinPositionCount());

            int partition = currentPartition.get().number();
            unspilledInputPages = spiller.map(spiller -> spiller.getSpilledPages(partition))
                    .orElse(emptyIterator());

            Optional.ofNullable(savedRows.remove(partition)).ifPresent(savedRow -> {
                restoreProbe(
                        savedRow.row,
                        savedRow.joinPositionWithinPartition,
                        savedRow.currentProbePositionProducedRow,
                        savedRow.joinSourcePositions,
                        SpillInfoSnapshot.noSpill());
            });

            return;
        }

        if (lookupPartitions.hasNext()) {
            currentPartition.ifPresent(Partition::release);
            currentPartition = Optional.of(lookupPartitions.next());
            unspilledLookupSource = Optional.of(currentPartition.get().load());

            return;
        }

        currentPartition.ifPresent(Partition::release);
        if (lookupSourceProvider != null) {
            // There are no more partitions to process, so clean up everything
            lookupSourceProvider.close();
            lookupSourceProvider = null;
        }
        spiller.ifPresent(PartitioningSpiller::verifyAllPartitionsRead);
        finished = true;
    }

    private void processProbe()
    {
        verify(probe != null);

        Optional<SpillInfoSnapshot> spillInfoSnapshotIfSpillChanged = lookupSourceProvider.withLease(lookupSourceLease -> {
            if (lookupSourceLease.spillEpoch() == inputPageSpillEpoch) {
                // Spill state didn't change, so process as usual.
                processProbe(lookupSourceLease.getLookupSource());
                return Optional.empty();
            }

            return Optional.of(SpillInfoSnapshot.from(lookupSourceLease));
        });

        if (!spillInfoSnapshotIfSpillChanged.isPresent()) {
            return;
        }
        SpillInfoSnapshot spillInfoSnapshot = spillInfoSnapshotIfSpillChanged.get();
        long joinPositionWithinPartition;
        if (joinPosition >= 0) {
            joinPositionWithinPartition = lookupSourceProvider.withLease(lookupSourceLease -> lookupSourceLease.getLookupSource().joinPositionWithinPartition(joinPosition));
        }
        else {
            joinPositionWithinPartition = -1;
        }

        /*
         * Spill state changed. All probe rows that were not processed yet should be treated as regular input (and be partially spilled).
         * If current row maps to the now-spilled partition, it needs to be saved for later. If it maps to a partition still in memory, it
         * should be added together with not-yet-processed rows. In either case we need to resume processing the row starting at its
         * current position in the lookup source.
         */
        verify(spillInfoSnapshot.hasSpilled());
        verify(spillInfoSnapshot.getSpillEpoch() > inputPageSpillEpoch);

        Page currentPage = probe.getPage();
        int currentPosition = probe.getPosition();
        long currentJoinPosition = this.joinPosition;
        boolean currentProbePositionProducedRow = this.currentProbePositionProducedRow;

        clearProbe();

        if (currentPosition < 0) {
            // Processing of the page hasn't been started yet.
            addInput(currentPage, spillInfoSnapshot);
        }
        else {
            int currentRowPartition = getPartitionGenerator().getPartition(currentPage, currentPosition);
            boolean currentRowSpilled = spillInfoSnapshot.getSpillMask().test(currentRowPartition);

            if (currentRowSpilled) {
                savedRows.merge(
                        currentRowPartition,
                        new SavedRow(currentPage, currentPosition, joinPositionWithinPartition, currentProbePositionProducedRow, joinSourcePositions),
                        (oldValue, newValue) -> {
                            throw new IllegalStateException(format("Partition %s is already spilled", currentRowPartition));
                        });
                joinSourcePositions = 0;
                Page unprocessed = pageTail(currentPage, currentPosition + 1);
                addInput(unprocessed, spillInfoSnapshot);
            }
            else {
                Page remaining = pageTail(currentPage, currentPosition);
                restoreProbe(remaining, currentJoinPosition, currentProbePositionProducedRow, joinSourcePositions, spillInfoSnapshot);
            }
        }
    }

    private void processProbe(LookupSource lookupSource)
    {
        verify(probe != null);

        DriverYieldSignal yieldSignal = operatorContext.getDriverContext().getYieldSignal();
        while (!yieldSignal.isSet()) {
            if (probe.getPosition() >= 0) {
                if (!joinCurrentPosition(lookupSource, yieldSignal)) {
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
            if (!advanceProbePosition(lookupSource)) {
                break;
            }
            statisticsCounter.recordProbe(joinSourcePositions);
            joinSourcePositions = 0;
        }
    }

    private void restoreProbe(Page probePage, long joinPosition, boolean currentProbePositionProducedRow, int joinSourcePositions, SpillInfoSnapshot spillInfoSnapshot)
    {
        verify(probe == null);

        addInput(probePage, spillInfoSnapshot);
        verify(probe.advanceNextPosition());
        this.joinPosition = joinPosition;
        this.currentProbePositionProducedRow = currentProbePositionProducedRow;
        this.joinSourcePositions = joinSourcePositions;
    }

    private Page pageTail(Page currentPage, int startAtPosition)
    {
        verify(currentPage.getPositionCount() - startAtPosition >= 0);
        return currentPage.getRegion(startAtPosition, currentPage.getPositionCount() - startAtPosition);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        probe = null;

        try (Closer closer = Closer.create()) {
            // `afterClose` must be run last.
            // Closer is documented to mimic try-with-resource, which implies close will happen in reverse order.
            closer.register(afterClose::run);

            closer.register(pageBuilder::reset);
            closer.register(() -> Optional.ofNullable(lookupSourceProvider).ifPresent(LookupSourceProvider::close));
            spiller.ifPresent(closer::register);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Produce rows matching join condition for the current probe position. If this method was called previously
     * for the current probe position, calling this again will produce rows that wasn't been produced in previous
     * invocations.
     *
     * @return true if all eligible rows have been produced; false otherwise
     */
    private boolean joinCurrentPosition(LookupSource lookupSource, DriverYieldSignal yieldSignal)
    {
        // while we have a position on lookup side to join against...
        while (joinPosition >= 0) {
            if (lookupSource.isJoinPositionEligible(joinPosition, probe.getPosition(), probe.getPage())) {
                currentProbePositionProducedRow = true;

                pageBuilder.appendRow(probe, lookupSource, joinPosition);
                joinSourcePositions++;
            }

            // get next position on lookup side for this probe row
            joinPosition = lookupSource.getNextJoinPosition(joinPosition, probe.getPosition(), probe.getPage());

            if (yieldSignal.isSet() || tryBuildPage()) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return whether there are more positions on probe side
     */
    private boolean advanceProbePosition(LookupSource lookupSource)
    {
        if (!probe.advanceNextPosition()) {
            clearProbe();
            return false;
        }

        // update join position
        joinPosition = probe.getCurrentJoinPosition(lookupSource);
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
            pageBuilder.appendNullForBuild(probe);
            if (tryBuildPage()) {
                return false;
            }
        }
        return true;
    }

    // This class must be public because LookupJoinOperator is isolated.
    public static class SpillInfoSnapshot
    {
        private final boolean hasSpilled;
        private final long spillEpoch;
        private final IntPredicate spillMask;

        public SpillInfoSnapshot(boolean hasSpilled, long spillEpoch, IntPredicate spillMask)
        {
            this.hasSpilled = hasSpilled;
            this.spillEpoch = spillEpoch;
            this.spillMask = requireNonNull(spillMask, "spillMask is null");
        }

        public static SpillInfoSnapshot from(LookupSourceLease lookupSourceLease)
        {
            return new SpillInfoSnapshot(
                    lookupSourceLease.hasSpilled(),
                    lookupSourceLease.spillEpoch(),
                    lookupSourceLease.getSpillMask());
        }

        public static SpillInfoSnapshot noSpill()
        {
            return new SpillInfoSnapshot(false, 0, i -> false);
        }

        public boolean hasSpilled()
        {
            return hasSpilled;
        }

        public long getSpillEpoch()
        {
            return spillEpoch;
        }

        public IntPredicate getSpillMask()
        {
            return spillMask;
        }
    }

    // This class must be public because LookupJoinOperator is isolated.
    public static class SavedRow
    {
        /**
         * A page with exactly one {@link Page#getPositionCount}, representing saved row.
         */
        public final Page row;

        /**
         * A snapshot of {@link LookupJoinOperator#joinPosition} "de-partitioned", i.e. {@link LookupJoinOperator#joinPosition} is a join position
         * with respect to (potentially) partitioned lookup source, while this value is a join position with respect to containing partition.
         */
        public final long joinPositionWithinPartition;

        /**
         * A snapshot of {@link LookupJoinOperator#currentProbePositionProducedRow}
         */
        public final boolean currentProbePositionProducedRow;

        /**
         * A snapshot of {@link LookupJoinOperator#joinSourcePositions}
         */
        public final int joinSourcePositions;

        public SavedRow(Page page, int position, long joinPositionWithinPartition, boolean currentProbePositionProducedRow, int joinSourcePositions)
        {
            this.row = page.getSingleValuePage(position);

            this.joinPositionWithinPartition = joinPositionWithinPartition;
            this.currentProbePositionProducedRow = currentProbePositionProducedRow;
            this.joinSourcePositions = joinSourcePositions;
        }
    }

    private boolean tryBuildPage()
    {
        if (pageBuilder.isFull()) {
            buildPage();
            return true;
        }
        return false;
    }

    private void buildPage()
    {
        verify(outputPage == null);
        verify(probe != null);

        if (pageBuilder.isEmpty()) {
            return;
        }

        outputPage = pageBuilder.build(probe);
        pageBuilder.reset();
    }

    private void clearProbe()
    {
        // Before updating the probe flush the current page
        buildPage();
        probe = null;
    }
}
