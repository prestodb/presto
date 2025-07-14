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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.execution.FragmentResultCacheContext;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.split.RemoteSplit;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static com.facebook.presto.common.RuntimeMetricName.FRAGMENT_RESULT_CACHE_HIT;
import static com.facebook.presto.common.RuntimeMetricName.FRAGMENT_RESULT_CACHE_MISS;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.facebook.presto.operator.SpillingUtils.checkSpillSucceeded;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.facebook.presto.util.MoreUninterruptibles.tryLockUninterruptibly;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.Boolean.TRUE;
import static java.util.Objects.requireNonNull;

//
// NOTE:  As a general strategy the methods should "stage" a change and only
// process the actual change before lock release (DriverLockResult.close()).
// The assures that only one thread will be working with the operators at a
// time and state changer threads are not blocked.
//
public class Driver
        implements Closeable
{
    private static final Logger log = Logger.get(Driver.class);

    private final DriverContext driverContext;
    private final AtomicReference<Optional<FragmentResultCacheContext>> fragmentResultCacheContext;
    private final List<Operator> activeOperators;
    // this is present only for debugging
    @SuppressWarnings("unused")
    private final List<Operator> allOperators;
    private final Optional<SourceOperator> sourceOperator;
    private final Optional<DeleteOperator> deleteOperator;
    private final Optional<UpdateOperator> updateOperator;
    private final Optional<MergeWriterOperator> mergeOperator;

    // This variable acts as a staging area. When new splits (encapsulated in TaskSource) are
    // provided to a Driver, the Driver will not process them right away. Instead, the splits are
    // added to this staging area. This staging area will be drained asynchronously. That's when
    // the new splits get processed.
    private final AtomicReference<TaskSource> pendingTaskSourceUpdates = new AtomicReference<>();
    private final Map<Operator, ListenableFuture<?>> revokingOperators = new HashMap<>();

    private final AtomicReference<State> state = new AtomicReference<>(State.ALIVE);

    private final DriverLock exclusiveLock = new DriverLock();

    @GuardedBy("exclusiveLock")
    private TaskSource currentTaskSource;

    private final AtomicReference<SettableFuture<?>> driverBlockedFuture = new AtomicReference<>();

    private final AtomicReference<Optional<Iterator<Page>>> cachedResult = new AtomicReference<>(Optional.empty());
    private final AtomicReference<Split> split = new AtomicReference<>();
    private final List<Page> outputPages = new ArrayList<>();

    private enum State
    {
        ALIVE, NEED_DESTRUCTION, DESTROYED
    }

    public static Driver createDriver(DriverContext driverContext, List<Operator> operators)
    {
        requireNonNull(driverContext, "driverContext is null");
        requireNonNull(operators, "operators is null");
        Driver driver = new Driver(driverContext, operators);
        driver.initialize();
        return driver;
    }

    @VisibleForTesting
    public static Driver createDriver(DriverContext driverContext, Operator firstOperator, Operator... otherOperators)
    {
        requireNonNull(driverContext, "driverContext is null");
        requireNonNull(firstOperator, "firstOperator is null");
        requireNonNull(otherOperators, "otherOperators is null");
        ImmutableList<Operator> operators = ImmutableList.<Operator>builder()
                .add(firstOperator)
                .add(otherOperators)
                .build();
        return createDriver(driverContext, operators);
    }

    private Driver(DriverContext driverContext, List<Operator> operators)
    {
        this.driverContext = requireNonNull(driverContext, "driverContext is null");
        this.fragmentResultCacheContext = new AtomicReference<>(driverContext.getFragmentResultCacheContext());
        this.allOperators = ImmutableList.copyOf(requireNonNull(operators, "operators is null"));
        checkArgument(allOperators.size() > 1, "At least two operators are required");
        this.activeOperators = new ArrayList<>(operators);
        checkArgument(!operators.isEmpty(), "There must be at least one operator");

        Optional<SourceOperator> sourceOperator = Optional.empty();
        Optional<DeleteOperator> deleteOperator = Optional.empty();
        Optional<UpdateOperator> updateOperator = Optional.empty();
        Optional<MergeWriterOperator> mergeOperator = Optional.empty();
        for (Operator operator : operators) {
            if (operator instanceof SourceOperator) {
                checkArgument(!sourceOperator.isPresent(), "There must be at most one SourceOperator");
                sourceOperator = Optional.of((SourceOperator) operator);
            }
            else if (operator instanceof DeleteOperator) {
                checkArgument(!deleteOperator.isPresent(), "There must be at most one DeleteOperator");
                deleteOperator = Optional.of((DeleteOperator) operator);
            }
            else if (operator instanceof UpdateOperator) {
                checkArgument(!updateOperator.isPresent(), "There must be at most one UpdateOperator");
                updateOperator = Optional.of((UpdateOperator) operator);
            }
            else if (operator instanceof MergeWriterOperator) {
                checkArgument(!mergeOperator.isPresent(), "There must be at most one MergeWriterOperator");
                mergeOperator = Optional.of((MergeWriterOperator) operator);
            }
        }
        this.sourceOperator = sourceOperator;
        this.deleteOperator = deleteOperator;
        this.updateOperator = updateOperator;
        this.mergeOperator = mergeOperator;

        currentTaskSource = sourceOperator.map(operator -> new TaskSource(operator.getSourceId(), ImmutableSet.of(), false)).orElse(null);
        // initially the driverBlockedFuture is not blocked (it is completed)
        SettableFuture<?> future = SettableFuture.create();
        future.set(null);
        driverBlockedFuture.set(future);
    }

    // the memory revocation request listeners are added here in a separate initialize() method
    // instead of the constructor to prevent leaking the "this" reference to
    // another thread, which will cause unsafe publication of this instance.
    private void initialize()
    {
        activeOperators.stream()
                .map(Operator::getOperatorContext)
                .forEach(operatorContext -> operatorContext.setMemoryRevocationRequestListener(() -> driverBlockedFuture.get().set(null)));
    }

    public DriverContext getDriverContext()
    {
        return driverContext;
    }

    public Optional<PlanNodeId> getSourceId()
    {
        return sourceOperator.map(SourceOperator::getSourceId);
    }

    @Override
    public void close()
    {
        // mark the service for destruction
        if (!state.compareAndSet(State.ALIVE, State.NEED_DESTRUCTION)) {
            return;
        }

        // set the yield signal and interrupt any actively running driver to stop them as soon as possible
        driverContext.getYieldSignal().yieldImmediatelyForTermination();
        exclusiveLock.interruptCurrentOwner();

        // if we can get the lock, attempt a clean shutdown; otherwise someone else will shutdown
        tryWithLock(() -> TRUE);
    }

    public boolean isFinished()
    {
        checkLockNotHeld("Can not check finished status while holding the driver lock");

        // if we can get the lock, attempt a clean shutdown; otherwise someone else will shutdown
        Optional<Boolean> result = tryWithLock(this::isFinishedInternal);
        return result.orElseGet(() -> state.get() != State.ALIVE || driverContext.isDone());
    }

    @GuardedBy("exclusiveLock")
    private boolean isFinishedInternal()
    {
        checkLockHeld("Lock must be held to call isFinishedInternal");

        boolean finished = state.get() != State.ALIVE || driverContext.isDone() || activeOperators.isEmpty() || activeOperators.get(activeOperators.size() - 1).isFinished();
        if (finished) {
            state.compareAndSet(State.ALIVE, State.NEED_DESTRUCTION);
        }
        return finished;
    }

    public void updateSource(TaskSource sourceUpdate)
    {
        checkLockNotHeld("Can not update sources while holding the driver lock");
        checkArgument(
                sourceOperator.isPresent() && sourceOperator.get().getSourceId().equals(sourceUpdate.getPlanNodeId()),
                "sourceUpdate is for a canonicalPlan node that is different from this Driver's source node");

        // stage the new updates
        pendingTaskSourceUpdates.updateAndGet(current -> current == null ? sourceUpdate : current.update(sourceUpdate));

        // attempt to get the lock and process the updates we staged above
        // updates will be processed in close if and only if we got the lock
        tryWithLock(() -> TRUE);
    }

    @GuardedBy("exclusiveLock")
    private void processNewSources()
    {
        checkLockHeld("Lock must be held to call processNewSources");

        // only update if the driver is still alive
        if (state.get() != State.ALIVE) {
            return;
        }

        TaskSource sourceUpdate = pendingTaskSourceUpdates.getAndSet(null);
        if (sourceUpdate == null) {
            return;
        }

        // merge the current source and the specified source update
        TaskSource newSource = currentTaskSource.update(sourceUpdate);

        // if the update contains no new data, just return
        if (newSource == currentTaskSource) {
            return;
        }

        // determine new splits to add
        Set<ScheduledSplit> newSplits = Sets.difference(newSource.getSplits(), currentTaskSource.getSplits());

        // add new splits
        SourceOperator sourceOperator = this.sourceOperator.orElseThrow(VerifyException::new);
        for (ScheduledSplit newSplit : newSplits) {
            Split split = newSplit.getSplit();

            if (fragmentResultCacheContext.get().isPresent() && !(split.getConnectorSplit() instanceof RemoteSplit)) {
                checkState(!this.cachedResult.get().isPresent());
                this.fragmentResultCacheContext.set(this.fragmentResultCacheContext.get().map(context -> context.updateRuntimeInformation(split.getConnectorSplit())));
                FragmentCacheResult fragmentCacheResult = fragmentResultCacheContext.get().get()
                        .getFragmentResultCacheManager()
                        .get(fragmentResultCacheContext.get().get().getHashedCanonicalPlanFragment(), split);
                Optional<Iterator<Page>> pages = fragmentCacheResult.getPages();
                sourceOperator.getOperatorContext().getRuntimeStats().addMetricValue(
                        pages.isPresent() ? FRAGMENT_RESULT_CACHE_HIT : FRAGMENT_RESULT_CACHE_MISS, NONE, 1);
                if (pages.isPresent()) {
                    sourceOperator.getOperatorContext().recordProcessedInput(fragmentCacheResult.getInputDataSize(), 0);
                    sourceOperator.getOperatorContext().recordRawInput(fragmentCacheResult.getInputDataSize(), 0);
                }
                this.cachedResult.set(pages);
                this.split.set(split);
            }

            Supplier<Optional<UpdatablePageSource>> pageSource = sourceOperator.addSplit(newSplit);
            deleteOperator.ifPresent(deleteOperator -> deleteOperator.setPageSource(pageSource));
            updateOperator.ifPresent(updateOperator -> updateOperator.setPageSource(pageSource));
            mergeOperator.ifPresent(mergeOperator -> mergeOperator.setPageSource(pageSource));
        }

        // set no more splits
        if (newSource.isNoMoreSplits()) {
            sourceOperator.noMoreSplits();
        }

        currentTaskSource = newSource;
    }

    public ListenableFuture<?> processFor(Duration duration)
    {
        checkLockNotHeld("Can not process for a duration while holding the driver lock");

        requireNonNull(duration, "duration is null");

        // if the driver is blocked we don't need to continue
        SettableFuture<?> blockedFuture = driverBlockedFuture.get();
        if (!blockedFuture.isDone()) {
            return blockedFuture;
        }

        long maxRuntime = duration.roundTo(TimeUnit.NANOSECONDS);

        Optional<ListenableFuture<?>> result = tryWithLock(100, TimeUnit.MILLISECONDS, () -> {
            OperationTimer operationTimer = createTimer();
            driverContext.startProcessTimer();
            driverContext.getYieldSignal().setWithDelay(maxRuntime, driverContext.getYieldExecutor());
            try {
                long start = System.nanoTime();
                do {
                    ListenableFuture<?> future = processInternal(operationTimer);
                    if (!future.isDone()) {
                        return updateDriverBlockedFuture(future);
                    }
                }
                while (System.nanoTime() - start < maxRuntime && !isFinishedInternal());
            }
            finally {
                driverContext.getYieldSignal().reset();
                driverContext.recordProcessed(operationTimer);
            }
            return NOT_BLOCKED;
        });
        return result.orElse(NOT_BLOCKED);
    }

    public ListenableFuture<?> process()
    {
        checkLockNotHeld("Can not process while holding the driver lock");

        // if the driver is blocked we don't need to continue
        SettableFuture<?> blockedFuture = driverBlockedFuture.get();
        if (!blockedFuture.isDone()) {
            return blockedFuture;
        }

        Optional<ListenableFuture<?>> result = tryWithLock(100, TimeUnit.MILLISECONDS, () -> {
            ListenableFuture<?> future = processInternal(createTimer());
            return updateDriverBlockedFuture(future);
        });
        return result.orElse(NOT_BLOCKED);
    }

    private OperationTimer createTimer()
    {
        return new OperationTimer(
                driverContext.isCpuTimerEnabled(),
                driverContext.isCpuTimerEnabled() && driverContext.isPerOperatorCpuTimerEnabled(),
                driverContext.isAllocationTrackingEnabled(),
                driverContext.isAllocationTrackingEnabled() && driverContext.isPerOperatorAllocationTrackingEnabled());
    }

    private ListenableFuture<?> updateDriverBlockedFuture(ListenableFuture<?> sourceBlockedFuture)
    {
        // driverBlockedFuture will be completed as soon as the sourceBlockedFuture is completed
        // or any of the operators gets a memory revocation request
        SettableFuture<?> newDriverBlockedFuture = SettableFuture.create();
        driverBlockedFuture.set(newDriverBlockedFuture);
        sourceBlockedFuture.addListener(() -> newDriverBlockedFuture.set(null), directExecutor());

        // it's possible that memory revoking is requested for some operator
        // before we update driverBlockedFuture above and we don't want to miss that
        // notification, so we check to see whether that's the case before returning.
        boolean memoryRevokingRequested = activeOperators.stream()
                .filter(operator -> !revokingOperators.containsKey(operator))
                .map(Operator::getOperatorContext)
                .anyMatch(OperatorContext::isMemoryRevokingRequested);

        if (memoryRevokingRequested) {
            newDriverBlockedFuture.set(null);
        }

        return newDriverBlockedFuture;
    }

    private boolean shouldUseFragmentResultCache()
    {
        return fragmentResultCacheContext.get().isPresent() && split.get() != null && split.get().getConnectorSplit().getNodeSelectionStrategy() != NO_PREFERENCE;
    }

    @GuardedBy("exclusiveLock")
    private ListenableFuture<?> processInternal(OperationTimer operationTimer)
    {
        checkLockHeld("Lock must be held to call processInternal");

        handleMemoryRevoke();

        try {
            processNewSources();

            // If there is only one operator, finish it
            // Some operators (LookupJoinOperator and HashBuildOperator) are broken and requires finish to be called continuously
            // TODO remove the second part of the if statement, when these operators are fixed
            // Note: finish should not be called on the natural source of the pipeline as this could cause the task to finish early
            if (!activeOperators.isEmpty() && activeOperators.size() != allOperators.size()) {
                Operator rootOperator = activeOperators.get(0);
                rootOperator.finish();
                rootOperator.getOperatorContext().recordFinish(operationTimer);
            }

            boolean movedPage = false;
            if (cachedResult.get().isPresent()) {
                Iterator<Page> remainingPages = cachedResult.get().get();
                Operator outputOperator = activeOperators.get(activeOperators.size() - 1);
                if (remainingPages.hasNext()) {
                    Page outputPage = remainingPages.next();
                    outputPages.add(outputPage);
                    outputOperator.addInput(outputPage);
                }
                else {
                    outputOperator.finish();
                    outputOperator.getOperatorContext().recordFinish(operationTimer);
                }
            }
            else {
                for (int i = 0; i < activeOperators.size() - 1 && !driverContext.isDone(); i++) {
                    Operator current = activeOperators.get(i);
                    Operator next = activeOperators.get(i + 1);

                    // skip blocked operator
                    if (getBlockedFuture(current).isPresent()) {
                        continue;
                    }

                    // if the current operator is not finished and next operator isn't blocked and needs input...
                    if (!current.isFinished() && !getBlockedFuture(next).isPresent() && next.needsInput()) {
                        // get an output page from current operator
                        Page page = current.getOutput();
                        current.getOperatorContext().recordGetOutput(operationTimer, page);

                        // For the last non-output operator, we keep the pages for caching purpose.
                        if (shouldUseFragmentResultCache() && i == activeOperators.size() - 2 && page != null) {
                            outputPages.add(page);
                        }

                        // if we got an output page, add it to the next operator
                        if (page != null && page.getPositionCount() != 0) {
                            next.addInput(page);
                            next.getOperatorContext().recordAddInput(operationTimer, page);
                            movedPage = true;
                        }

                        if (current instanceof SourceOperator) {
                            movedPage = true;
                        }
                    }

                    // if current operator is finished...
                    if (current.isFinished()) {
                        // let next operator know there will be no more data
                        next.finish();
                        next.getOperatorContext().recordFinish(operationTimer);
                    }
                }
            }

            for (int index = activeOperators.size() - 1; index >= 0; index--) {
                if (activeOperators.get(index).isFinished()) {
                    boolean outputOperatorFinished = index == activeOperators.size() - 1;
                    // close and remove this operator and all source operators
                    List<Operator> finishedOperators = this.activeOperators.subList(0, index + 1);
                    Throwable throwable = closeAndDestroyOperators(finishedOperators);
                    finishedOperators.clear();
                    if (throwable != null) {
                        throwIfUnchecked(throwable);
                        throw new RuntimeException(throwable);
                    }

                    if (shouldUseFragmentResultCache() && outputOperatorFinished && !cachedResult.get().isPresent()) {
                        checkState(split.get() != null);
                        checkState(fragmentResultCacheContext.get().isPresent());
                        fragmentResultCacheContext.get().get().getFragmentResultCacheManager().put(
                                fragmentResultCacheContext.get().get().getHashedCanonicalPlanFragment(),
                                split.get(),
                                outputPages,
                                // also cache the bytes read count from the source operator for this fragment
                                sourceOperator.get().getOperatorContext().getInputDataSize().getTotalCount());
                    }

                    // Finish the next operator, which is now the first operator.
                    if (!activeOperators.isEmpty()) {
                        Operator newRootOperator = activeOperators.get(0);
                        newRootOperator.finish();
                        newRootOperator.getOperatorContext().recordFinish(operationTimer);
                    }
                    break;
                }
            }

            // if we did not move any pages, check if we are blocked
            if (!movedPage) {
                List<Operator> blockedOperators = new ArrayList<>();
                List<ListenableFuture<?>> blockedFutures = new ArrayList<>();
                for (Operator operator : activeOperators) {
                    Optional<ListenableFuture<?>> blocked = getBlockedFuture(operator);
                    if (blocked.isPresent()) {
                        blockedOperators.add(operator);
                        blockedFutures.add(blocked.get());
                    }
                }

                if (!blockedFutures.isEmpty()) {
                    // unblock when the first future is complete
                    ListenableFuture<?> blocked = firstFinishedFuture(blockedFutures);
                    // driver records serial blocked time
                    driverContext.recordBlocked(blocked);
                    // each blocked operator is responsible for blocking the execution
                    // until one of the operators can continue
                    for (Operator operator : blockedOperators) {
                        operator.getOperatorContext().recordBlocked(blocked);
                    }
                    return blocked;
                }
            }

            return NOT_BLOCKED;
        }
        catch (Throwable t) {
            List<StackTraceElement> interrupterStack = exclusiveLock.getInterrupterStack();
            if (interrupterStack == null) {
                driverContext.failed(t);
                throw t;
            }

            // Driver thread was interrupted which should only happen if the task is already finished.
            // If this becomes the actual cause of a failed query there is a bug in the task state machine.
            Exception exception = new Exception("Interrupted By");
            exception.setStackTrace(interrupterStack.stream().toArray(StackTraceElement[]::new));
            PrestoException newException = new PrestoException(GENERIC_INTERNAL_ERROR, "Driver was interrupted", exception);
            newException.addSuppressed(t);
            driverContext.failed(newException);
            throw newException;
        }
    }

    @GuardedBy("exclusiveLock")
    private void handleMemoryRevoke()
    {
        for (int i = 0; i < activeOperators.size() && !driverContext.isDone(); i++) {
            Operator operator = activeOperators.get(i);

            if (revokingOperators.containsKey(operator)) {
                checkOperatorFinishedRevoking(operator);
            }
            else if (operator.getOperatorContext().isMemoryRevokingRequested()) {
                ListenableFuture<?> future = operator.startMemoryRevoke();
                revokingOperators.put(operator, future);
                checkOperatorFinishedRevoking(operator);
            }
        }
    }

    @GuardedBy("exclusiveLock")
    private void checkOperatorFinishedRevoking(Operator operator)
    {
        ListenableFuture<?> future = revokingOperators.get(operator);
        if (future.isDone()) {
            checkSpillSucceeded(future); // propagate exception if there was some
            revokingOperators.remove(operator);
            operator.finishMemoryRevoke();
            operator.getOperatorContext().resetMemoryRevokingRequested();
        }
    }

    @GuardedBy("exclusiveLock")
    private void destroyIfNecessary()
    {
        checkLockHeld("Lock must be held to call destroyIfNecessary");

        if (!state.compareAndSet(State.NEED_DESTRUCTION, State.DESTROYED)) {
            return;
        }

        // if we get an error while closing a driver, record it and we will throw it at the end
        Throwable inFlightException = null;
        try {
            inFlightException = closeAndDestroyOperators(activeOperators);
            if (driverContext.getMemoryUsage() > 0) {
                log.error("Driver still has memory reserved after freeing all operator memory.");
            }
            if (driverContext.getSystemMemoryUsage() > 0) {
                log.error("Driver still has system memory reserved after freeing all operator memory.");
            }
            if (driverContext.getRevocableMemoryUsage() > 0) {
                log.error("Driver still has revocable memory reserved after freeing all operator memory. Freeing it.");
            }
            driverContext.finished();
        }
        catch (Throwable t) {
            // this shouldn't happen but be safe
            inFlightException = addSuppressedException(
                    inFlightException,
                    t,
                    "Error destroying driver for task %s",
                    driverContext.getTaskId());
        }

        if (inFlightException != null) {
            // this will always be an Error or Runtime
            throwIfUnchecked(inFlightException);
            throw new RuntimeException(inFlightException);
        }
    }

    private Throwable closeAndDestroyOperators(List<Operator> operators)
    {
        // record the current interrupted status (and clear the flag); we'll reset it later
        boolean wasInterrupted = Thread.interrupted();

        Throwable inFlightException = null;
        try {
            for (Operator operator : operators) {
                try {
                    operator.close();
                }
                catch (InterruptedException t) {
                    // don't record the stack
                    wasInterrupted = true;
                }
                catch (Throwable t) {
                    inFlightException = addSuppressedException(
                            inFlightException,
                            t,
                            "Error closing operator %s for task %s",
                            operator.getOperatorContext().getOperatorId(),
                            driverContext.getTaskId());
                }
                try {
                    operator.getOperatorContext().destroy();
                }
                catch (Throwable t) {
                    inFlightException = addSuppressedException(
                            inFlightException,
                            t,
                            "Error freeing all allocated memory for operator %s for task %s",
                            operator.getOperatorContext().getOperatorId(),
                            driverContext.getTaskId());
                }
            }
        }
        finally {
            // reset the interrupted flag
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
        return inFlightException;
    }

    private Optional<ListenableFuture<?>> getBlockedFuture(Operator operator)
    {
        ListenableFuture<?> blocked = revokingOperators.get(operator);
        if (blocked != null) {
            // We mark operator as blocked regardless of blocked.isDone(), because finishMemoryRevoke has not been called yet.
            return Optional.of(blocked);
        }
        blocked = operator.isBlocked();
        if (!blocked.isDone()) {
            return Optional.of(blocked);
        }
        blocked = operator.getOperatorContext().isWaitingForMemory();
        if (!blocked.isDone()) {
            return Optional.of(blocked);
        }
        blocked = operator.getOperatorContext().isWaitingForRevocableMemory();
        if (!blocked.isDone()) {
            return Optional.of(blocked);
        }
        return Optional.empty();
    }

    private static Throwable addSuppressedException(Throwable inFlightException, Throwable newException, String message, Object... args)
    {
        if (newException instanceof Error) {
            if (inFlightException == null) {
                inFlightException = newException;
            }
            else {
                // Self-suppression not permitted
                if (inFlightException != newException) {
                    inFlightException.addSuppressed(newException);
                }
            }
        }
        else {
            // log normal exceptions instead of rethrowing them
            log.error(newException, message, args);
        }
        return inFlightException;
    }

    private synchronized void checkLockNotHeld(String message)
    {
        checkState(!exclusiveLock.isHeldByCurrentThread(), message);
    }

    @GuardedBy("exclusiveLock")
    private synchronized void checkLockHeld(String message)
    {
        checkState(exclusiveLock.isHeldByCurrentThread(), message);
    }

    private static ListenableFuture<?> firstFinishedFuture(List<ListenableFuture<?>> futures)
    {
        if (futures.size() == 1) {
            return futures.get(0);
        }

        SettableFuture<?> result = SettableFuture.create();

        for (ListenableFuture<?> future : futures) {
            future.addListener(() -> result.set(null), directExecutor());
        }

        return result;
    }

    // Note: task can not return null
    private <T> Optional<T> tryWithLock(Supplier<T> task)
    {
        return tryWithLock(0, TimeUnit.MILLISECONDS, task);
    }

    // Note: task can not return null
    private <T> Optional<T> tryWithLock(long timeout, TimeUnit unit, Supplier<T> task)
    {
        checkLockNotHeld("Lock can not be reacquired");

        boolean acquired = exclusiveLock.tryLock(timeout, unit);

        if (!acquired) {
            return Optional.empty();
        }

        Optional<T> result;
        try {
            result = Optional.of(task.get());
        }
        finally {
            try {
                try {
                    processNewSources();
                }
                finally {
                    destroyIfNecessary();
                }
            }
            finally {
                exclusiveLock.unlock();
            }
        }

        // If there are more source updates available, attempt to reacquire the lock and process them.
        // This can happen if new sources are added while we're holding the lock here doing work.
        // NOTE: this is separate duplicate code to make debugging lock reacquisition easier
        // The first condition is for processing the pending updates if this driver is still ALIVE
        // The second condition is to destroy the driver if the state is NEED_DESTRUCTION
        while (((pendingTaskSourceUpdates.get() != null && state.get() == State.ALIVE) || state.get() == State.NEED_DESTRUCTION)
                && exclusiveLock.tryLock()) {
            try {
                try {
                    processNewSources();
                }
                finally {
                    destroyIfNecessary();
                }
            }
            finally {
                exclusiveLock.unlock();
            }
        }

        return result;
    }

    private static class DriverLock
    {
        private final ReentrantLock lock = new ReentrantLock();

        @GuardedBy("this")
        private Thread currentOwner;

        @GuardedBy("this")
        private List<StackTraceElement> interrupterStack;

        public boolean isHeldByCurrentThread()
        {
            return lock.isHeldByCurrentThread();
        }

        public boolean tryLock()
        {
            checkState(!lock.isHeldByCurrentThread(), "Lock is not reentrant");
            boolean acquired = lock.tryLock();
            if (acquired) {
                setOwner();
            }
            return acquired;
        }

        public boolean tryLock(long timeout, TimeUnit unit)
        {
            checkState(!lock.isHeldByCurrentThread(), "Lock is not reentrant");
            boolean acquired = tryLockUninterruptibly(lock, timeout, unit);
            if (acquired) {
                setOwner();
            }
            return acquired;
        }

        private synchronized void setOwner()
        {
            checkState(lock.isHeldByCurrentThread(), "Current thread does not hold lock");
            currentOwner = Thread.currentThread();
            // NOTE: We do not use interrupted stack information to know that another
            // thread has attempted to interrupt the driver, and interrupt this new lock
            // owner.  The interrupted stack information is for debugging purposes only.
            // In the case of interruption, the caller should (and does) have a separate
            // state to prevent further processing in the Driver.
        }

        public synchronized void unlock()
        {
            checkState(lock.isHeldByCurrentThread(), "Current thread does not hold lock");
            currentOwner = null;
            lock.unlock();
        }

        public synchronized List<StackTraceElement> getInterrupterStack()
        {
            return interrupterStack;
        }

        public synchronized void interruptCurrentOwner()
        {
            // there is a benign race condition here were the lock holder
            // can be change between attempting to get lock and grabbing
            // the synchronized lock here, but in either case we want to
            // interrupt the lock holder thread
            if (interrupterStack == null) {
                interrupterStack = ImmutableList.copyOf(Thread.currentThread().getStackTrace());
            }

            if (currentOwner != null) {
                currentOwner.interrupt();
            }
        }
    }
}
