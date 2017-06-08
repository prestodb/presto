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
import com.facebook.presto.operator.LookupSourceProvider.LookupSourceLease;
import com.facebook.presto.operator.PartitionedConsumption.Partition;
import com.facebook.presto.operator.exchange.LocalPartitionGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.PartitioningSpiller;
import com.facebook.presto.spiller.PartitioningSpiller.PartitioningSpillResult;
import com.facebook.presto.spiller.PartitioningSpillerFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

import static com.facebook.presto.operator.LookupJoinOperators.JoinType.FULL_OUTER;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static com.facebook.presto.operator.Operators.checkSuccess;
import static com.facebook.presto.operator.Operators.getDone;
import static com.facebook.presto.spi.Page.mask;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

public class LookupJoinOperator
        implements Operator
{
    private static final int MAX_POSITIONS_EVALUATED_PER_CALL = 10000;

    private final OperatorContext operatorContext;
    private final List<Type> allTypes;
    private final List<Type> probeTypes;
    private final JoinProbeFactory joinProbeFactory;
    private final Runnable onClose;
    private final OptionalInt lookupJoinsCount;
    private final HashGenerator hashGenerator;
    private final LookupSourceFactory lookupSourceFactory;
    private final PartitioningSpillerFactory partitioningSpillerFactory;

    private final JoinStatisticsCounter statisticsCounter;

    private final PageBuilder pageBuilder;

    private final boolean probeOnOuterSide;

    private final ListenableFuture<LookupSourceProvider> lookupSourceProviderFuture;
    private LookupSourceProvider lookupSourceProvider;
    private JoinProbe probe;

    private Optional<PartitioningSpiller> spiller = Optional.empty();
    private Optional<LocalPartitionGenerator> partitionGenerator = Optional.empty();
    private ListenableFuture<?> spillInProgress = NOT_BLOCKED;
    private long inputPageSpillEpoch;
    private boolean closed;
    private boolean finishing;
    private boolean unspilling;
    private boolean finished;
    private long joinPosition = -1;
    private int joinSourcePositions = 0;

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
            List<Type> allTypes,
            List<Type> probeTypes,
            JoinType joinType,
            LookupSourceFactory lookupSourceFactory,
            JoinProbeFactory joinProbeFactory,
            Runnable onClose,
            OptionalInt lookupJoinsCount,
            HashGenerator hashGenerator,
            PartitioningSpillerFactory partitioningSpillerFactory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.allTypes = ImmutableList.copyOf(requireNonNull(allTypes, "allTypes is null"));
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));

        requireNonNull(joinType, "joinType is null");
        // Cannot use switch case here, because javac will synthesize an inner class and cause IllegalAccessError
        probeOnOuterSide = joinType == PROBE_OUTER || joinType == FULL_OUTER;

        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.onClose = requireNonNull(onClose, "onClose is null");
        this.lookupJoinsCount = requireNonNull(lookupJoinsCount, "lookupJoinsCount is null");
        this.hashGenerator = requireNonNull(hashGenerator, "hashGenerator is null");
        this.lookupSourceFactory = requireNonNull(lookupSourceFactory, "lookupSourceFactory is null");
        this.partitioningSpillerFactory = requireNonNull(partitioningSpillerFactory, "partitioningSpillerFactory is null");
        this.lookupSourceProviderFuture = lookupSourceFactory.createLookupSourceProvider();

        this.statisticsCounter = new JoinStatisticsCounter(joinType);
        operatorContext.setInfoSupplier(this.statisticsCounter);

        this.pageBuilder = new PageBuilder(allTypes);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return allTypes;
    }

    @Override
    public void finish()
    {
        if (finishing) {
            return;
        }

        if (!spillInProgress.isDone()) {
            // Not ready yet.
            return;
        }

        checkSuccess(spillInProgress, "spilling failed");
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        boolean finished = this.finished && probe == null && pageBuilder.isEmpty();

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
            /*
             * Input spilling can happen only after lookupSourceProviderFuture was done.
             */
            return spillInProgress;
        }
        if (unspilledLookupSource.isPresent()) {
            /*
             * Unspilling can happen only after lookupSourceProviderFuture was done.
             */
            return unspilledLookupSource.get();
        }

        return lookupSourceProviderFuture;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing
                && lookupSourceProviderFuture.isDone()
                && spillInProgress.isDone()
                && probe == null;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(probe == null, "Current page has not been completely processed yet");

        checkState(hasLookupSourceProvider(), "Not ready to handle input yet");

        SpillInfoSnapshot spillInfoSnapshot = lookupSourceProvider.withLease(SpillInfoSnapshot::from);
        addInput(page, spillInfoSnapshot);
    }

    private void addInput(Page page, SpillInfoSnapshot spillInfoSnapshot)
    {
        requireNonNull(spillInfoSnapshot, "spillInfoSnapshot is null");

        if (spillInfoSnapshot.hasSpilled) {
            page = spillAndMaskSpilledPositions(page, spillInfoSnapshot.spillMask);
            if (page.getPositionCount() == 0) {
                return;
            }
        }

        // create probe
        inputPageSpillEpoch = spillInfoSnapshot.spillEpoch;
        probe = joinProbeFactory.createJoinProbe(page);

        // initialize to invalid join position to force output code to advance the cursors
        joinPosition = -1;
    }

    private boolean hasLookupSourceProvider()
    {
        if (lookupSourceProvider == null) {
            if (!lookupSourceProviderFuture.isDone()) {
                return false;
            }
            lookupSourceProvider = requireNonNull(getDone(lookupSourceProviderFuture));
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
                    operatorContext.getSystemMemoryContext().newAggregatedMemoryContext()));
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
        if (!spillInProgress.isDone()) {
            return null;
        }
        checkSuccess(spillInProgress, "spilling failed");

        if (probe == null && pageBuilder.isEmpty() && !finishing) {
            // Fast exit path when lookup source is still being built
            return null;
        }

        if (!hasLookupSourceProvider()) {
            return null;
        }

        if (probe == null && finishing && !unspilling) {
            /*
             * We do not have input probe and we won't have any, as we're finishing.
             * Let LookupSourceFactory know LookupSources can be disposed as far as we're concerned.
             */
            finishRegularInput();
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

        return producePage();
    }

    private void finishRegularInput()
    {
        verify(partitionedConsumption == null, "partitioned consumption already started");
        partitionedConsumption = lookupSourceFactory.finishProbeOperator(lookupJoinsCount);
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
            verify(probe != null);
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
            lookupSourceProvider = new SimpleLookupSourceProvider(lookupSource);

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
        verify(spillInfoSnapshot.hasSpilled);
        verify(spillInfoSnapshot.spillEpoch > inputPageSpillEpoch);

        Page currentPage = probe.getPage();
        int currentPosition = probe.getPosition();
        long currentJoinPosition = this.joinPosition;
        boolean currentProbePositionProducedRow = this.currentProbePositionProducedRow;
        int joinSourcePositions = this.joinSourcePositions;

        probe = null;

        if (currentPosition < 0) {
            // Processing of the page hasn't been started yet.
            addInput(currentPage, spillInfoSnapshot);
        }
        else {
            int currentRowPartition = getPartitionGenerator().getPartition(currentPage, currentPosition);
            boolean currentRowSpilled = spillInfoSnapshot.spillMask.test(currentRowPartition);

            if (currentRowSpilled) {
                savedRows.merge(
                        currentRowPartition,
                        new SavedRow(currentPage, currentPosition, joinPositionWithinPartition, currentProbePositionProducedRow, joinSourcePositions),
                        (oldValue, newValue) -> {
                            throw new IllegalStateException(format("How on earth this could happen that partition %s is spilled in the middle of processing twice?", currentRowPartition));
                        });
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

        Counter lookupPositionsConsidered = new Counter();
        while (true) {
            if (probe.getPosition() >= 0) {
                if (!joinCurrentPosition(lookupSource, lookupPositionsConsidered)) {
                    break;
                }
                if (!currentProbePositionProducedRow) {
                    currentProbePositionProducedRow = true;
                    if (!outerJoinCurrentPosition(lookupSource)) {
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

        int[] retainedPositions = new int[currentPage.getPositionCount() - startAtPosition];
        Arrays.setAll(retainedPositions, i -> startAtPosition + i);
        return mask(currentPage, retainedPositions);
    }

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
            this.row = mask(page, new int[] {position});
            this.row.compact();

            this.joinPositionWithinPartition = joinPositionWithinPartition;
            this.currentProbePositionProducedRow = currentProbePositionProducedRow;
            this.joinSourcePositions = joinSourcePositions;
        }
    }

    private Page producePage()
    {
        if (shouldProducePage()) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }
        return null;
    }

    private boolean shouldProducePage()
    {
        if (pageBuilder.isFull()) {
            return true;
        }

        if (pageBuilder.isEmpty()) {
            return false;
        }

        return probe == null && finished;
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
            closer.register(pageBuilder::reset);
            closer.register(() -> Optional.ofNullable(lookupSourceProvider).ifPresent(LookupSourceProvider::close));
            spiller.ifPresent(closer::register);
            closer.register(onClose::run);
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
     * @return true if all eligible rows have been produced; false otherwise (because pageBuilder became full)
     */
    private boolean joinCurrentPosition(LookupSource lookupSource, Counter lookupPositionsConsidered)
    {
        // while we have a position on lookup side to join against...
        while (joinPosition >= 0) {
            lookupPositionsConsidered.increment();
            if (lookupSource.isJoinPositionEligible(joinPosition, probe.getPosition(), probe.getPage())) {
                currentProbePositionProducedRow = true;

                pageBuilder.declarePosition();
                // write probe columns
                probe.appendTo(pageBuilder);
                // write build columns
                lookupSource.appendTo(joinPosition, pageBuilder, probe.getOutputChannelCount());
                joinSourcePositions++;
            }

            // get next position on lookup side for this probe row
            joinPosition = lookupSource.getNextJoinPosition(joinPosition, probe.getPosition(), probe.getPage());

            if (lookupPositionsConsidered.get() >= MAX_POSITIONS_EVALUATED_PER_CALL) {
                return false;
            }
            if (pageBuilder.isFull()) {
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
            probe = null;
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
    private boolean outerJoinCurrentPosition(LookupSource lookupSource)
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

    // This class needs to be public because LookupJoinOperator is isolated.
    public static class SpillInfoSnapshot
    {
        public final boolean hasSpilled;
        public final long spillEpoch;
        public final IntPredicate spillMask;

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
                    lookupSourceLease.getSpillMask()
            );
        }

        public static SpillInfoSnapshot noSpill()
        {
            return new SpillInfoSnapshot(false, 0, i -> false);
        }
    }
}
