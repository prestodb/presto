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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.LongArrayBlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.JoinProbe.JoinProbeFactory;
import com.facebook.presto.operator.LookupSourceProvider.LookupSourceLease;
import com.facebook.presto.operator.PartitionedConsumption.Partition;
import com.facebook.presto.operator.exchange.LocalPartitionGenerator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spiller.PartitioningSpiller;
import com.facebook.presto.spiller.PartitioningSpillerFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Future;
import java.util.function.IntPredicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.airlift.concurrent.MoreFutures.addSuccessCallback;
import static com.facebook.airlift.concurrent.MoreFutures.getDone;
import static com.facebook.presto.SystemSessionProperties.getStarJoinProbeType;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.operator.SpillingUtils.checkSpillSucceeded;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

public class LookupStarJoinOperator
        implements Operator
{
    public final TrackIndex outputIndexList;
    public final long[] buildPositions;
    private final OperatorContext operatorContext;
    private final List<Type> probeTypes;
    private final JoinProbeFactory joinProbeFactory;
    private final Runnable afterClose;
    private final OptionalInt lookupJoinsCount;
    private final HashGenerator hashGenerator;
    // From join bridge, contains data from build side
    private final List<LookupSourceFactory> lookupSourceFactory;
    private final PartitioningSpillerFactory partitioningSpillerFactory;
    private final JoinStatisticsCounter statisticsCounter;
    // To build output page
    private final LookupJoinPageBuilder pageBuilder;
    private final boolean probeOnOuterSide;
    // created by lookupSourceFactory
    private final List<ListenableFuture<LookupSourceProvider>> lookupSourceProviderFuture;
    private final Optional<PartitioningSpiller> spiller = Optional.empty();
    private final Optional<List<LocalPartitionGenerator>> partitionGenerator = Optional.empty();
    private final ListenableFuture<?> spillInProgress = NOT_BLOCKED;
    private final Block[] joinPositionBlockList;
    private final List<Long> modular;
    private final Optional<ListenableFuture<Supplier<LookupSource>>> unspilledLookupSource = Optional.empty();
    private final Iterator<Page> unspilledInputPages = emptyIterator();
    private final long joinPosition = -1;
    private final List<List<Long>> joinPositionLists = new LinkedList<>();
    private final List<Integer> buildOutputOffsets = new LinkedList<>();
    private final long[] buildStarPositions;
    private final long[] buildCurrentPositions;
    private List<LookupSourceProvider> lookupSourceProvider = new LinkedList<>();
    // created from input page when calling addInput
    private JoinProbe probe;
    private Page outputPage;
    private long inputPageSpillEpoch;
    private boolean closed;
    private boolean finishing;
    private boolean unspilling;
    private boolean finished;
    private boolean joinPositionBlockListHasData;
    private long numberOfOutput;
    private long currentOutputIdx;
    private int joinSourcePositions;
    private boolean currentProbePositionProducedRow;
    @Nullable
    private List<ListenableFuture<PartitionedConsumption<Supplier<LookupSource>>>> partitionedConsumption = new LinkedList<>();
    @Nullable
    private Iterator<Partition<Supplier<LookupSource>>> lookupPartitions;
    private Optional<Partition<Supplier<LookupSource>>> currentPartition = Optional.empty();
    @Nullable
    private StarJoinLookupSource starJoinLookupSource;

    public LookupStarJoinOperator(
            OperatorContext operatorContext,
            List<Type> probeTypes,
            List<Type> buildOutputTypes,
            LookupJoinOperators.JoinType joinType,
            List<LookupSourceFactory> lookupSourceFactory,
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
        probeOnOuterSide = joinType == LookupJoinOperators.JoinType.PROBE_OUTER || joinType == LookupJoinOperators.JoinType.FULL_OUTER;

        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.afterClose = requireNonNull(afterClose, "afterClose is null");
        this.lookupJoinsCount = requireNonNull(lookupJoinsCount, "lookupJoinsCount is null");
        this.hashGenerator = requireNonNull(hashGenerator, "hashGenerator is null");
        this.lookupSourceFactory = ImmutableList.copyOf(requireNonNull(lookupSourceFactory, "lookupSourceFactory is null"));
        this.partitioningSpillerFactory = requireNonNull(partitioningSpillerFactory, "partitioningSpillerFactory is null");
        this.lookupSourceProviderFuture = lookupSourceFactory.stream().map(LookupSourceFactory::createLookupSourceProvider).collect(toImmutableList());

        this.statisticsCounter = new JoinStatisticsCounter(joinType);
        operatorContext.setInfoSupplier(this.statisticsCounter);

        this.pageBuilder = new LookupJoinPageBuilder(buildOutputTypes);
        this.modular = new ArrayList<>(Collections.nCopies(lookupSourceFactory.size(), 1L));
        this.joinPositionBlockList = new Block[lookupSourceFactory.size()];
        this.joinPositionBlockListHasData = false;
        this.outputIndexList = new TrackIndex(lookupSourceFactory.size());
        this.buildPositions = new long[lookupSourceFactory.size()];
        this.buildStarPositions = new long[lookupSourceFactory.size()];
        this.buildCurrentPositions = new long[lookupSourceFactory.size()];
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

        checkSpillSucceeded(spillInProgress);
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
        if (finishing) {
            return NOT_BLOCKED;
        }

        return Futures.allAsList(lookupSourceProviderFuture);
    }

    @Override
    public boolean needsInput()
    {
        return !finishing
                && lookupSourceProviderFuture.stream().allMatch(Future::isDone)
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

        // Do not support spill yet
        SpillInfoSnapshot spillInfoSnapshot = lookupSourceProvider.get(0).withLease(LookupStarJoinOperator.SpillInfoSnapshot::from);
//        SpillInfoSnapshot spillInfoSnapshot = new SpillInfoSnapshot(false, 0, i -> false);
        addInput(page, spillInfoSnapshot);
    }

    private void addInput(Page page, SpillInfoSnapshot spillInfoSnapshot)
    {
        requireNonNull(spillInfoSnapshot, "spillInfoSnapshot is null");

        if (spillInfoSnapshot.hasSpilled()) {
            checkState(false);
            page = spillAndMaskSpilledPositions(page, spillInfoSnapshot.getSpillMask());
            if (page.getPositionCount() == 0) {
                return;
            }
        }

        // create probe
        inputPageSpillEpoch = spillInfoSnapshot.getSpillEpoch();
        probe = joinProbeFactory.createJoinProbe(page);

        // initialize to invalid join position to force output code to advance the cursors
        joinPositionLists.clear();
        joinPositionBlockListHasData = false;
    }

    private boolean tryFetchLookupSourceProvider()
    {
        if (lookupSourceProvider == null || lookupSourceProvider.isEmpty()) {
            if (!lookupSourceProviderFuture.stream().allMatch(Future::isDone)) {
                return false;
            }
            lookupSourceProvider = lookupSourceProviderFuture.stream().map(x -> requireNonNull(getDone(x))).collect(toImmutableList());
            // skip stats
            // statisticsCounter.updateLookupSourcePositions(lookupSourceProvider.withLease(lookupSourceLease -> lookupSourceLease.getLookupSource().getJoinPositionCount()));
        }
        return true;
    }

    private Page spillAndMaskSpilledPositions(Page page, IntPredicate spillMask)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "does not support spill in starjoin");
    }

    public LocalPartitionGenerator getPartitionGenerator()
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "does not support spill in starjoin");
    }

    @Override
    public Page getOutput()
    {
        checkSpillSucceeded(spillInProgress);

        if (probe == null && pageBuilder.isEmpty() && !finishing) {
            return null;
        }

        if (!tryFetchLookupSourceProvider()) {
            if (!finishing) {
                return null;
            }

            verify(finishing);
            // We are no longer interested in the build side (the lookupSourceProviderFuture's value).
            lookupSourceProviderFuture.forEach(x -> addSuccessCallback(x, LookupSourceProvider::close));
            lookupSourceProvider = IntStream.range(0, lookupSourceProviderFuture.size()).boxed()
                    .map(x -> new StaticLookupSourceProvider(new EmptyLookupSource())).collect(toImmutableList());
        }

        if (probe == null && finishing) {
            /*
             * We do not have input probe and we won't have any, as we're finishing.
             * Let LookupSourceFactory know LookupSources can be disposed as far as we're concerned.
             */
            verify(partitionedConsumption.isEmpty(), "partitioned consumption already started");
            partitionedConsumption = lookupSourceFactory.stream().map(x -> x.finishProbeOperator(lookupJoinsCount)).collect(Collectors.toList());
            finished = true;
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
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "does not support spill in starjoin");
    }

    private void processProbe()
    {
        processProbe(lookupSourceProvider.stream().map(x -> x.withLease(lookupSourceLease -> {
            checkState(lookupSourceLease.spillEpoch() == inputPageSpillEpoch);
            // Spill state didn't change, so process as usual.
            return lookupSourceLease.getLookupSource();
        })).collect(Collectors.toList()));
    }

    private void processProbe(List<LookupSource> lookupSource)
    {
        verify(probe != null);

        DriverYieldSignal yieldSignal = operatorContext.getDriverContext().getYieldSignal();
        while (!yieldSignal.isSet()) {
            if (probe.getPosition() >= 0) {
                if (!joinCurrentPosition(lookupSource, probeOnOuterSide, yieldSignal, getStarJoinProbeType(operatorContext.getSession()))) {
                    break;
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

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        probe = null;

        // In case of early termination (before operator is finished) release partition consumption to avoid a deadlock
        if (partitionedConsumption.isEmpty()) {
            partitionedConsumption = lookupSourceFactory.stream().map(x -> x.finishProbeOperator(lookupJoinsCount)).collect(Collectors.toList());
            partitionedConsumption.forEach(x -> addSuccessCallback(x, consumption -> consumption.beginConsumption().forEachRemaining(Partition::release)));
        }
        currentPartition.ifPresent(Partition::release);
        currentPartition = Optional.empty();
        if (lookupPartitions != null) {
            while (lookupPartitions.hasNext()) {
                lookupPartitions.next().release();
            }
        }

        try (Closer closer = Closer.create()) {
            // `afterClose` must be run last.
            // Closer is documented to mimic try-with-resource, which implies close will happen in reverse order.
            closer.register(afterClose::run);

            closer.register(pageBuilder::reset);
            closer.register(() -> lookupSourceProvider.forEach(LookupSourceProvider::close));
            spiller.ifPresent(closer::register);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean joinCurrentPosition(List<LookupSource> lookupSource, boolean probeOuter, DriverYieldSignal yieldSignal, String type)
    {
        if (type.equals("list")) {
            return joinCurrentPosition(lookupSource, probeOuter, yieldSignal);
        }
        else if (type.equals("block")) {
            return joinCurrentPositionBlock(lookupSource, probeOuter, yieldSignal);
        }
        else if (type.equals("blocktrack")) {
            return joinCurrentPositionBlockTrack(lookupSource, probeOuter, yieldSignal);
        }
        else if (type.equals("nolist")) {
            return joinCurrentPositionNoList(lookupSource, probeOuter, yieldSignal);
        }
        else if (type.equals("nolistcache")) {
            return joinCurrentPositionNoListCache(lookupSource, probeOuter, yieldSignal);
        }
        else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "does not support in starjoin");
        }
    }

    /**
     * Produce rows matching join condition for the current probe position. If this method was called previously
     * for the current probe position, calling this again will produce rows that wasn't been produced in previous
     * invocations.
     *
     * @return true if all eligible rows have been produced; false otherwise
     */
    private boolean joinCurrentPosition(List<LookupSource> lookupSource, boolean probeOuter, DriverYieldSignal yieldSignal)
    {
        if (buildOutputOffsets == null || buildOutputOffsets.isEmpty()) {
            for (int i = 0; i < lookupSource.size(); ++i) {
                if (i == 0) {
                    buildOutputOffsets.add(0);
                }
                else {
                    buildOutputOffsets.add(buildOutputOffsets.get(i - 1) + lookupSource.get(i - 1).getChannelCount());
                }
            }
        }
        if (joinPositionLists.isEmpty()) {
            for (LookupSource source : lookupSource) {
                List<Long> positions = new ArrayList();
                long position = probe.getCurrentJoinPosition(source);
                while (position >= 0) {
                    if (source.isJoinPositionEligible(position, probe.getPosition(), probe.getPage())) {
                        positions.add(position);
                    }
                    position = source.getNextJoinPosition(position, probe.getPosition(), probe.getPage());
                }
                if (probeOuter && positions.isEmpty()) {
                    positions.add(-1L);
                }
                joinPositionLists.add(ImmutableList.copyOf(positions));
            }
            checkState(joinPositionLists.size() == modular.size());
            for (int i = modular.size() - 2; i >= 0; --i) {
                modular.set(i, modular.get(i + 1) * joinPositionLists.get(i + 1).size());
            }
            numberOfOutput = modular.get(0) * joinPositionLists.get(0).size();
            currentOutputIdx = 0;
        }
        while (currentOutputIdx < numberOfOutput) {
            List<Long> buildsidePositions = new ArrayList<>();
            for (int i = 0; i < modular.size(); ++i) {
                buildsidePositions.add(joinPositionLists.get(i).get((int) (currentOutputIdx / modular.get(i)) % joinPositionLists.get(i).size()));
            }
            ++joinSourcePositions;
            pageBuilder.appendRow(probe, lookupSource, buildsidePositions, buildOutputOffsets);
            ++currentOutputIdx;
            if (yieldSignal.isSet() || tryBuildPage()) {
                return false;
            }
        }
        joinPositionLists.clear();
        return true;
    }

    private boolean joinCurrentPositionBlock(List<LookupSource> lookupSource, boolean probeOuter, DriverYieldSignal yieldSignal)
    {
        if (buildOutputOffsets == null || buildOutputOffsets.isEmpty()) {
            for (int i = 0; i < lookupSource.size(); ++i) {
                if (i == 0) {
                    buildOutputOffsets.add(0);
                }
                else {
                    buildOutputOffsets.add(buildOutputOffsets.get(i - 1) + lookupSource.get(i - 1).getChannelCount());
                }
            }
        }
        if (!joinPositionBlockListHasData) {
            for (int i = 0; i < lookupSource.size(); ++i) {
                LookupSource source = lookupSource.get(i);
                BlockBuilder positions = BIGINT.createBlockBuilder(null, 1);
                long position = probe.getCurrentJoinPosition(source);
                while (position >= 0) {
                    if (source.isJoinPositionEligible(position, probe.getPosition(), probe.getPage())) {
                        positions.writeLong(position);
                    }
                    position = source.getNextJoinPosition(position, probe.getPosition(), probe.getPage());
                }
                if (probeOuter && positions.getPositionCount() == 0) {
                    positions.writeLong(-1L);
                }
                joinPositionBlockList[i] = positions.build();
            }
            for (int i = modular.size() - 2; i >= 0; --i) {
                modular.set(i, modular.get(i + 1) * joinPositionBlockList[i + 1].getPositionCount());
            }
            numberOfOutput = modular.get(0) * joinPositionBlockList[0].getPositionCount();
            currentOutputIdx = 0;
            joinPositionBlockListHasData = true;
        }
        while (currentOutputIdx < numberOfOutput) {
            List<Long> buildsidePositions = new ArrayList<>();
            for (int i = 0; i < modular.size(); ++i) {
                buildsidePositions.add(joinPositionBlockList[i].getLong((int) (currentOutputIdx / modular.get(i)) % joinPositionBlockList[i].getPositionCount()));
            }
            ++joinSourcePositions;
            pageBuilder.appendRow(probe, lookupSource, buildsidePositions, buildOutputOffsets);
            ++currentOutputIdx;
            if (yieldSignal.isSet() || tryBuildPage()) {
                return false;
            }
        }
        joinPositionBlockListHasData = false;
        return true;
    }

    private boolean joinCurrentPositionBlockTrack(List<LookupSource> lookupSource, boolean probeOuter, DriverYieldSignal yieldSignal)
    {
        if (buildOutputOffsets == null || buildOutputOffsets.isEmpty()) {
            for (int i = 0; i < lookupSource.size(); ++i) {
                if (i == 0) {
                    buildOutputOffsets.add(0);
                }
                else {
                    buildOutputOffsets.add(buildOutputOffsets.get(i - 1) + lookupSource.get(i - 1).getChannelCount());
                }
            }
        }
        boolean hasOutput = true;
        if (!joinPositionBlockListHasData) {
            for (int i = 0; i < lookupSource.size(); ++i) {
                LookupSource source = lookupSource.get(i);
                LongArrayBlockBuilder positions = new LongArrayBlockBuilder(null, 1);
                long position = probe.getCurrentJoinPosition(source);
                while (position >= 0) {
                    if (source.isJoinPositionEligible(position, probe.getPosition(), probe.getPage())) {
                        positions.writeLong(position);
                    }
                    position = source.getNextJoinPosition(position, probe.getPosition(), probe.getPage());
                }
                if (probeOuter && positions.getPositionCount() == 0) {
                    positions.writeLong(-1L);
                }
                joinPositionBlockList[i] = positions.build();
                outputIndexList.setCapacity(i, positions.getPositionCount());
            }
            outputIndexList.resetCurrentIndex();
            joinPositionBlockListHasData = true;
            hasOutput = outputIndexList.hasOutput();
        }
        while (hasOutput) {
            for (int i = 0; i < buildPositions.length; ++i) {
                buildPositions[i] = joinPositionBlockList[i].getLong(outputIndexList.getCurrentIndex()[i]);
            }
            ++joinSourcePositions;
            pageBuilder.appendRow(probe, lookupSource, buildPositions, buildOutputOffsets);
            if (!outputIndexList.advance()) {
                break;
            }
            if (yieldSignal.isSet() || tryBuildPage()) {
                return false;
            }
        }
        joinPositionBlockListHasData = false;
        return true;
    }

    private boolean joinCurrentPositionNoList(List<LookupSource> lookupSource, boolean probeOuter, DriverYieldSignal yieldSignal)
    {
        if (buildOutputOffsets == null || buildOutputOffsets.isEmpty()) {
            for (int i = 0; i < lookupSource.size(); ++i) {
                if (i == 0) {
                    buildOutputOffsets.add(0);
                }
                else {
                    buildOutputOffsets.add(buildOutputOffsets.get(i - 1) + lookupSource.get(i - 1).getChannelCount());
                }
            }
        }
        if (!joinPositionBlockListHasData) {
            for (int i = 0; i < lookupSource.size(); ++i) {
                LookupSource source = lookupSource.get(i);
                long position = probe.getCurrentJoinPosition(source);
                while (position >= 0) {
                    if (source.isJoinPositionEligible(position, probe.getPosition(), probe.getPage())) {
                        break;
                    }
                    position = source.getNextJoinPosition(position, probe.getPosition(), probe.getPage());
                }
                // If probe not in outer side, and no match found, the star join will not produce output at all, return true
                if (!probeOuter && position == -1) {
                    return true;
                }
                buildStarPositions[i] = position;
                buildCurrentPositions[i] = position;
            }
        }
        while (true) {
            ++joinSourcePositions;
            pageBuilder.appendRow(probe, lookupSource, buildCurrentPositions, buildOutputOffsets);
            // Now advance the build row position
            boolean hasOutput = true;
            for (int i = buildCurrentPositions.length - 1; i >= 0; --i) {
                if (buildCurrentPositions[i] == -1) {
                    if (i == 0) {
                        hasOutput = false;
                        break;
                    }
                }
                else {
                    // find next match position
                    long nextPosition = lookupSource.get(i).getNextJoinPosition(buildCurrentPositions[i], probe.getPosition(), probe.getPage());
                    while (nextPosition >= 0) {
                        if (lookupSource.get(i).isJoinPositionEligible(nextPosition, probe.getPosition(), probe.getPage())) {
                            break;
                        }
                        nextPosition = lookupSource.get(i).getNextJoinPosition(nextPosition, probe.getPosition(), probe.getPage());
                    }
                    if (nextPosition != -1) {
                        buildCurrentPositions[i] = nextPosition;
                        break;
                    }
                    else {
                        if (i == 0) {
                            hasOutput = false;
                            break;
                        }
                        else {
                            buildCurrentPositions[i] = buildStarPositions[i];
                        }
                    }
                }
            }
            if (!hasOutput) {
                break;
            }
            if (yieldSignal.isSet() || tryBuildPage()) {
                return false;
            }
        }
        joinPositionBlockListHasData = false;
        return true;
    }

    private boolean joinCurrentPositionNoListCache(List<LookupSource> lookupSource, boolean probeOuter, DriverYieldSignal yieldSignal)
    {
        if (buildOutputOffsets == null || buildOutputOffsets.isEmpty()) {
            for (int i = 0; i < lookupSource.size(); ++i) {
                if (i == 0) {
                    buildOutputOffsets.add(0);
                }
                else {
                    buildOutputOffsets.add(buildOutputOffsets.get(i - 1) + lookupSource.get(i - 1).getChannelCount());
                }
            }
        }
        if (starJoinLookupSource == null) {
            starJoinLookupSource = new StarJoinLookupSource(lookupSource);
        }
        if (!joinPositionBlockListHasData) {
            long position = -1;
            for (int i = 0; i < lookupSource.size(); ++i) {
                if (i == 0) {
                    LookupSource source = lookupSource.get(i);
                    position = probe.getCurrentJoinPosition(source);
                }
                else {
                    position = starJoinLookupSource.getNextStarPosition(i - 1, position, probe);
                }
                // If probe not in outer side, and no match found, the star join will not produce output at all, return true
                if (!probeOuter && position == -1) {
                    return true;
                }
                buildStarPositions[i] = position;
                buildCurrentPositions[i] = position;
            }
        }
        while (true) {
            ++joinSourcePositions;
            pageBuilder.appendRow(probe, lookupSource, buildCurrentPositions, buildOutputOffsets);
            // Now advance the build row position
            boolean hasOutput = true;
            for (int i = buildCurrentPositions.length - 1; i >= 0; --i) {
                if (buildCurrentPositions[i] == -1) {
                    if (i == 0) {
                        hasOutput = false;
                        break;
                    }
                }
                else {
                    long nextPosition = lookupSource.get(i).getNextJoinPosition(buildCurrentPositions[i], probe.getPosition(), probe.getPage());
                    if (nextPosition != -1) {
                        buildCurrentPositions[i] = nextPosition;
                        break;
                    }
                    else {
                        if (i == 0) {
                            hasOutput = false;
                            break;
                        }
                        else {
                            buildCurrentPositions[i] = buildStarPositions[i];
                        }
                    }
                }
            }
            if (!hasOutput) {
                break;
            }
            if (yieldSignal.isSet() || tryBuildPage()) {
                return false;
            }
        }
        joinPositionBlockListHasData = false;
        return true;
    }

    /**
     * @return whether there are more positions on probe side
     */
    private boolean advanceProbePosition(List<LookupSource> lookupSource)
    {
        if (!probe.advanceNextPosition()) {
            clearProbe();
            return false;
        }

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
            return !tryBuildPage();
        }
        return true;
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

        // build output of join
        outputPage = pageBuilder.build(probe);
        pageBuilder.reset();
    }

    private void clearProbe()
    {
        // Before updating the probe flush the current page
        buildPage();
        probe = null;
    }

    private static class TrackIndex
    {
        private final int[] capacity;
        private final int[] currentIndex;
        private final int size;

        public TrackIndex(int size)
        {
            this.capacity = new int[size];
            this.currentIndex = new int[size];
            this.size = size;
        }

        public boolean hasOutput()
        {
            for (int i = 0; i < size; ++i) {
                if (capacity[i] == 0) {
                    return false;
                }
            }
            return true;
        }

        public boolean advance()
        {
            currentIndex[size - 1] += 1;
            for (int i = size - 1; i >= 0; --i) {
                if (currentIndex[i] == capacity[i]) {
                    if (i == 0) {
                        return false;
                    }
                    currentIndex[i] = 0;
                    currentIndex[i - 1] += 1;
                }
                else {
                    return true;
                }
            }
            return false;
        }

        public int[] getCurrentIndex()
        {
            return currentIndex;
        }

        public void setCapacity(int pos, int val)
        {
            capacity[pos] = val;
        }

        public void resetCurrentIndex()
        {
            for (int i = 0; i < size; ++i) {
                currentIndex[i] = 0;
            }
        }
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
}
