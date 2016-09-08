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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.MappedBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.PartitioningSpiller;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.operator.LookupJoinOperators.JoinType.FULL_OUTER;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static java.util.Objects.requireNonNull;

public class LookupJoinOperator
        implements Operator, Closeable
{
    private final LookupJoiner lookupJoiner;
    private final OperatorContext operatorContext;

    private final List<Type> allTypes;
    private final List<Type> probeTypes;
    private final ListenableFuture<? extends LookupSource> lookupSourceFuture;
    private final LookupSourceFactory lookupSourceFactory;
    private final JoinProbeFactory joinProbeFactory;
    private final Runnable onClose;

    private final AtomicInteger lookupJoinsCount;
    private final HashGenerator hashGenerator;
    private final boolean probeOnOuterSide;

    private Optional<SpillledLookupJoiner> spilledLookupJoiner = Optional.empty();
    private LookupSource lookupSource;

    private boolean finishing;
    private boolean closed;

    private Optional<PartitioningSpiller> spiller = Optional.empty();
    private ListenableFuture<?> spillInProgress = NOT_BLOCKED;
    private Optional<Iterator<Integer>> spilledPartitions = Optional.empty();

    public LookupJoinOperator(
            OperatorContext operatorContext,
            List<Type> allTypes,
            List<Type> probeTypes,
            JoinType joinType,
            LookupSourceFactory lookupSourceFactory,
            JoinProbeFactory joinProbeFactory,
            Runnable onClose,
            AtomicInteger lookupJoinsCount,
            HashGenerator hashGenerator)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.allTypes = ImmutableList.copyOf(requireNonNull(allTypes, "allTypes is null"));
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));

        requireNonNull(joinType, "joinType is null");

        this.lookupSourceFactory = requireNonNull(lookupSourceFactory, "lookupSourceFactory is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.onClose = requireNonNull(onClose, "onClose is null");
        this.lookupJoinsCount = requireNonNull(lookupJoinsCount, "lookupJoinsCount is null");
        this.hashGenerator = requireNonNull(hashGenerator, "hashGenerator is null");

        this.lookupSourceFuture = lookupSourceFactory.createLookupSource();
        this.probeOnOuterSide = joinType == PROBE_OUTER || joinType == FULL_OUTER;
        this.lookupJoiner = new LookupJoiner(allTypes, lookupSourceFuture, joinProbeFactory, probeOnOuterSide);
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
        lookupJoiner.finish();

        if (!finishing && lookupSourceFactory.hasSpilled()) {
            // Last LookupJoinOperator will handle spilled pages
            int count = lookupJoinsCount.decrementAndGet();
            checkState(count >= 0);
            if (count > 0) {
                spiller = Optional.empty();
            }
            else {
                // last LookupJoinOperator might not spilled at all, but other parallel joins could
                // so we have to load spiller if this operator hasn't used one yet
                ensureSpillerLoaded();
                unspillNextLookupSource();
            }
        }
        finishing = true;
    }

    private void unspillNextLookupSource()
    {
        checkState(spiller.isPresent());
        checkState(spilledPartitions.isPresent());
        if (spilledPartitions.get().hasNext()) {
            int currentSpilledPartition = spilledPartitions.get().next();

            spilledLookupJoiner = Optional.of(new SpillledLookupJoiner(
                    allTypes,
                    lookupSourceFactory.readSpilledLookupSource(operatorContext.getSession(), currentSpilledPartition),
                    joinProbeFactory,
                    spiller.get().getSpilledPages(currentSpilledPartition),
                    probeOnOuterSide));
        }
        else {
            spiller.get().close();
            spiller = Optional.empty();
        }
    }

    @Override
    public boolean isFinished()
    {
        boolean finished = lookupJoiner.isFinished() && !spiller.isPresent();

        // if finished drop references so memory is freed early
        if (finished) {
            close();
        }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (spilledLookupJoiner.isPresent()) {
            return spilledLookupJoiner.get().isBlocked();
        }
        if (!spillInProgress.isDone()) {
            checkState(lookupSourceFuture.isDone());
            return spillInProgress;
        }
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
        requireNonNull(page, "page is null");

        if (lookupSourceFactory.hasSpilled()) {
            page = spillAndMaskSpilledPositions(page);
        }

        lookupJoiner.addInput(page);
    }

    private Page spillAndMaskSpilledPositions(Page page)
    {
        ensureSpillerLoaded();

        PartitioningSpiller.PartitioningSpillResult spillResult = spiller.get().partitionAndSpill(page);

        if (!spillResult.isBlocked().isDone()) {
            this.spillInProgress = toListenableFuture(spillResult.isBlocked());
        }
        IntArrayList unspilledPositions = spillResult.getUnspilledPositions();

        return mapPage(unspilledPositions, page);
    }

    private void ensureSpillerLoaded()
    {
        checkState(lookupSourceFactory.hasSpilled());
        checkState(lookupSourceFuture.isDone());
        if (lookupSource == null) {
            lookupSource = getFutureValue(lookupSourceFuture);
        }
        if (!spiller.isPresent()) {
            spiller = Optional.of(lookupSourceFactory.getProbeSpiller(probeTypes, hashGenerator));
        }
        if (!spilledPartitions.isPresent()) {
            spilledPartitions = Optional.of(ImmutableSet.copyOf(lookupSourceFactory.getSpilledPartitions()).iterator());
        }
    }

    private Page mapPage(IntArrayList unspilledPositions, Page page)
    {
        Block[] mappedBlocks = new Block[page.getChannelCount()];
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            mappedBlocks[channel] = new MappedBlock(page.getBlock(channel), unspilledPositions.elements(), 0, unspilledPositions.size());
        }
        return new Page(mappedBlocks);
    }

    @Override
    public Page getOutput()
    {
        if (finishing && spiller.isPresent()) {
            checkState(spilledLookupJoiner.isPresent());
            SpillledLookupJoiner joiner = spilledLookupJoiner.get();
            if (joiner.isFinished()) {
                unspillNextLookupSource();
                return null;
            }
            operatorContext.setMemoryReservation(joiner.getInMemorySizeInBytes());
            return joiner.getOutput();
        }
        else {
            return lookupJoiner.getOutput();
        }
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

    public static class LookupJoiner
            implements Closeable
    {
        private final ListenableFuture<? extends LookupSource> lookupSourceFuture;
        private final JoinProbeFactory joinProbeFactory;

        private final PageBuilder pageBuilder;

        private final boolean probeOnOuterSide;

        private LookupSource lookupSource;
        private JoinProbe probe;

        private long joinPosition = -1;
        private boolean finishing;

        public LookupJoiner(
                List<Type> types,
                ListenableFuture<? extends LookupSource> lookupSourceFuture,
                JoinProbeFactory joinProbeFactory,
                boolean probeOnOuterSide)
        {
            this.pageBuilder = new PageBuilder(types);
            this.lookupSourceFuture = requireNonNull(lookupSourceFuture, "lookupSourceFuture is null");
            this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
            this.probeOnOuterSide = probeOnOuterSide;
        }

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

        public ListenableFuture<?> isBlocked()
        {
            return lookupSourceFuture;
        }

        public void finish()
        {
            finishing = true;
        }

        public boolean isFinished()
        {
            return finishing && probe == null && pageBuilder.isEmpty();
        }

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
            probe = null;
            pageBuilder.reset();
            // closing lookup source is only here for index join
            if (lookupSource != null) {
                lookupSource.close();
            }
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

    public static class SpillledLookupJoiner
    {
        private final LookupJoiner lookupJoiner;
        private final Iterator<Page> probePages;
        private final CompletableFuture<? extends LookupSource> lookupSourceFuture;

        public SpillledLookupJoiner(
                List<Type> allTypes,
                CompletableFuture<? extends LookupSource> lookupSourceFuture,
                JoinProbeFactory joinProbeFactory,
                Iterator<Page> probePages,
                boolean probeOnOuterSide)
        {
            this.lookupJoiner = new LookupJoiner(allTypes, toListenableFuture(lookupSourceFuture), joinProbeFactory, probeOnOuterSide);
            this.lookupSourceFuture = requireNonNull(lookupSourceFuture, "lookupSourceFuture is null");
            this.probePages = requireNonNull(probePages, "probePages is null");
        }

        public ListenableFuture<?> isBlocked()
        {
            return lookupJoiner.isBlocked();
        }

        public Page getOutput()
        {
            if (!lookupJoiner.needsInput()) {
                return lookupJoiner.getOutput();
            }
            if (!probePages.hasNext()) {
                lookupJoiner.finish();
                return null;
            }
            Page probePage = probePages.next();
            lookupJoiner.addInput(probePage);

            return lookupJoiner.getOutput();
        }

        public boolean isFinished()
        {
            return lookupJoiner.isFinished();
        }

        public long getInMemorySizeInBytes()
        {
            if (lookupSourceFuture.isDone()) {
                return getFutureValue(lookupSourceFuture).getInMemorySizeInBytes();
            }
            return 0;
        }
    }
}
