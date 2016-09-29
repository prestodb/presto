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

import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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
    private final List<Operator> operators;
    private final Optional<SourceOperator> sourceOperator;
    private final Optional<DeleteOperator> deleteOperator;
    private final ConcurrentMap<PlanNodeId, TaskSource> newSources = new ConcurrentHashMap<>();

    private final AtomicReference<State> state = new AtomicReference<>(State.ALIVE);

    private final ReentrantLock exclusiveLock = new ReentrantLock();

    @GuardedBy("this")
    private Thread lockHolder;

    @GuardedBy("exclusiveLock")
    private final Map<PlanNodeId, TaskSource> currentSources = new ConcurrentHashMap<>();

    private enum State
    {
        ALIVE, NEED_DESTRUCTION, DESTROYED
    }

    public Driver(DriverContext driverContext, Operator firstOperator, Operator... otherOperators)
    {
        this(requireNonNull(driverContext, "driverContext is null"),
                ImmutableList.<Operator>builder()
                        .add(requireNonNull(firstOperator, "firstOperator is null"))
                        .add(requireNonNull(otherOperators, "otherOperators is null"))
                        .build());
    }

    public Driver(DriverContext driverContext, List<Operator> operators)
    {
        this.driverContext = requireNonNull(driverContext, "driverContext is null");
        this.operators = ImmutableList.copyOf(requireNonNull(operators, "operators is null"));
        checkArgument(!operators.isEmpty(), "There must be at least one operator");

        Optional<SourceOperator> sourceOperator = Optional.empty();
        Optional<DeleteOperator> deleteOperator = Optional.empty();
        for (Operator operator : operators) {
            if (operator instanceof SourceOperator) {
                checkArgument(!sourceOperator.isPresent(), "There must be at most one SourceOperator");
                sourceOperator = Optional.of((SourceOperator) operator);
            }
            else if (operator instanceof DeleteOperator) {
                checkArgument(!deleteOperator.isPresent(), "There must be at most one DeleteOperator");
                deleteOperator = Optional.of((DeleteOperator) operator);
            }
        }
        this.sourceOperator = sourceOperator;
        this.deleteOperator = deleteOperator;
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

        // if we can get the lock, attempt a clean shutdown; otherwise someone else will shutdown
        try (DriverLockResult lockResult = tryLockAndProcessPendingStateChanges(0, TimeUnit.MILLISECONDS)) {
            // if we did not get the lock, interrupt the lock holder
            if (!lockResult.wasAcquired()) {
                // there is a benign race condition here were the lock holder
                // can be change between attempting to get lock and grabbing
                // the synchronized lock here, but in either case we want to
                // interrupt the lock holder thread
                synchronized (this) {
                    if (lockHolder != null) {
                        lockHolder.interrupt();
                    }
                }
            }

            // clean shutdown is automatically triggered during lock release
        }
    }

    public boolean isFinished()
    {
        checkLockNotHeld("Can not check finished status while holding the driver lock");

        // if we can get the lock, attempt a clean shutdown; otherwise someone else will shutdown
        try (DriverLockResult lockResult = tryLockAndProcessPendingStateChanges(0, TimeUnit.MILLISECONDS)) {
            if (lockResult.wasAcquired()) {
                return isFinishedInternal();
            }
            else {
                // did not get the lock, so we can't check operators, or destroy
                return state.get() != State.ALIVE || driverContext.isDone();
            }
        }
    }

    private boolean isFinishedInternal()
    {
        checkLockHeld("Lock must be held to call isFinishedInternal");

        boolean finished = state.get() != State.ALIVE || driverContext.isDone() || operators.get(operators.size() - 1).isFinished();
        if (finished) {
            state.compareAndSet(State.ALIVE, State.NEED_DESTRUCTION);
        }
        return finished;
    }

    public void updateSource(TaskSource source)
    {
        checkLockNotHeld("Can not update sources while holding the driver lock");

        // does this driver have an operator for the specified source?
        if (!sourceOperator.isPresent() || !sourceOperator.get().getSourceId().equals(source.getPlanNodeId())) {
            return;
        }

        // stage the new updates
        while (true) {
            // attempt to update directly to the new source
            TaskSource currentNewSource = newSources.putIfAbsent(source.getPlanNodeId(), source);

            // if update succeeded, just break
            if (currentNewSource == null) {
                break;
            }

            // merge source into the current new source
            TaskSource newSource = currentNewSource.update(source);

            // if this is not a new source, just return
            if (newSource == currentNewSource) {
                break;
            }

            // attempt to replace the currentNewSource with the new source
            if (newSources.replace(source.getPlanNodeId(), currentNewSource, newSource)) {
                break;
            }

            // someone else updated while we were processing
        }

        // attempt to get the lock and process the updates we staged above
        // updates will be processed in close if and only if we got the lock
        tryLockAndProcessPendingStateChanges(0, TimeUnit.MILLISECONDS).close();
    }

    private void processNewSources()
    {
        checkLockHeld("Lock must be held to call processNewSources");

        // only update if the driver is still alive
        if (state.get() != State.ALIVE) {
            return;
        }

        // copy the pending sources
        // it is ok to "miss" a source added during the copy as it will be
        // handled on the next call to this method
        Map<PlanNodeId, TaskSource> sources = new HashMap<>(newSources);
        for (Entry<PlanNodeId, TaskSource> entry : sources.entrySet()) {
            // Remove the entries we are going to process from the newSources map.
            // It is ok if someone already updated the entry; we will catch it on
            // the next iteration.
            newSources.remove(entry.getKey(), entry.getValue());

            processNewSource(entry.getValue());
        }
    }

    private void processNewSource(TaskSource source)
    {
        checkLockHeld("Lock must be held to call processNewSources");

        // create new source
        Set<ScheduledSplit> newSplits;
        TaskSource currentSource = currentSources.get(source.getPlanNodeId());
        if (currentSource == null) {
            newSplits = source.getSplits();
            currentSources.put(source.getPlanNodeId(), source);
        }
        else {
            // merge the current source and the specified source
            TaskSource newSource = currentSource.update(source);

            // if this is not a new source, just return
            if (newSource == currentSource) {
                return;
            }

            // find the new splits to add
            newSplits = Sets.difference(newSource.getSplits(), currentSource.getSplits());
            currentSources.put(source.getPlanNodeId(), newSource);
        }

        // add new splits
        if (sourceOperator.isPresent() && sourceOperator.get().getSourceId().equals(source.getPlanNodeId())) {
            for (ScheduledSplit newSplit : newSplits) {
                Split split = newSplit.getSplit();

                Supplier<Optional<UpdatablePageSource>> pageSource = sourceOperator.get().addSplit(split);
                if (deleteOperator.isPresent()) {
                    deleteOperator.get().setPageSource(pageSource);
                }
            }

            // set no more splits
            if (source.isNoMoreSplits()) {
                sourceOperator.get().noMoreSplits();
            }
        }
    }

    public ListenableFuture<?> processFor(Duration duration)
    {
        checkLockNotHeld("Can not process for a duration while holding the driver lock");

        requireNonNull(duration, "duration is null");

        long maxRuntime = duration.roundTo(TimeUnit.NANOSECONDS);

        try (DriverLockResult lockResult = tryLockAndProcessPendingStateChanges(100, TimeUnit.MILLISECONDS)) {
            if (lockResult.wasAcquired()) {
                driverContext.startProcessTimer();
                try {
                    long start = System.nanoTime();
                    do {
                        ListenableFuture<?> future = processInternal();
                        if (!future.isDone()) {
                            return future;
                        }
                    }
                    while (System.nanoTime() - start < maxRuntime && !isFinishedInternal());
                }
                finally {
                    driverContext.recordProcessed();
                }
            }
        }
        return NOT_BLOCKED;
    }

    public ListenableFuture<?> process()
    {
        checkLockNotHeld("Can not process while holding the driver lock");

        try (DriverLockResult lockResult = tryLockAndProcessPendingStateChanges(100, TimeUnit.MILLISECONDS)) {
            if (!lockResult.wasAcquired()) {
                // this is unlikely to happen unless the driver is being
                // destroyed and in that case the caller should notice notice
                // this state change by calling isFinished
                return NOT_BLOCKED;
            }
            return processInternal();
        }
    }

    private ListenableFuture<?> processInternal()
    {
        checkLockHeld("Lock must be held to call processInternal");

        try {
            if (!newSources.isEmpty()) {
                processNewSources();
            }

            // special handling for drivers with a single operator
            if (operators.size() == 1) {
                if (driverContext.isDone()) {
                    return NOT_BLOCKED;
                }

                // check if operator is blocked
                Operator current = operators.get(0);
                ListenableFuture<?> blocked = isBlocked(current);
                if (!blocked.isDone()) {
                    current.getOperatorContext().recordBlocked(blocked);
                    return blocked;
                }

                // there is only one operator so just finish it
                current.getOperatorContext().startIntervalTimer();
                current.finish();
                current.getOperatorContext().recordFinish();
                return NOT_BLOCKED;
            }

            boolean movedPage = false;
            for (int i = 0; i < operators.size() - 1 && !driverContext.isDone(); i++) {
                Operator current = operators.get(i);
                Operator next = operators.get(i + 1);

                // skip blocked operators
                if (!isBlocked(current).isDone()) {
                    continue;
                }
                if (!isBlocked(next).isDone()) {
                    continue;
                }

                // if the current operator is not finished and next operator needs input...
                if (!current.isFinished() && next.needsInput()) {
                    // get an output page from current operator
                    current.getOperatorContext().startIntervalTimer();
                    Page page = current.getOutput();
                    current.getOperatorContext().recordGetOutput(page);

                    // if we got an output page, add it to the next operator
                    if (page != null && page.getPositionCount() != 0) {
                        next.getOperatorContext().startIntervalTimer();
                        next.addInput(page);
                        next.getOperatorContext().recordAddInput(page);
                        movedPage = true;
                    }
                }

                // if current operator is finished...
                if (current.isFinished()) {
                    // let next operator know there will be no more data
                    next.getOperatorContext().startIntervalTimer();
                    next.finish();
                    next.getOperatorContext().recordFinish();
                }
            }

            // if we did not move any pages, check if we are blocked
            if (!movedPage) {
                List<Operator> blockedOperators = new ArrayList<>();
                List<ListenableFuture<?>> blockedFutures = new ArrayList<>();
                for (Operator operator : operators) {
                    ListenableFuture<?> blocked = isBlocked(operator);
                    if (!blocked.isDone()) {
                        blockedOperators.add(operator);
                        blockedFutures.add(blocked);
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
            driverContext.failed(t);
            throw t;
        }
    }

    private void destroyIfNecessary()
    {
        checkLockHeld("Lock must be held to call destroyIfNecessary");

        if (!state.compareAndSet(State.NEED_DESTRUCTION, State.DESTROYED)) {
            return;
        }

        // record the current interrupted status (and clear the flag); we'll reset it later
        boolean wasInterrupted = Thread.interrupted();

        // if we get an error while closing a driver, record it and we will throw it at the end
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
                    operator.getOperatorContext().setMemoryReservation(0);
                }
                catch (Throwable t) {
                    inFlightException = addSuppressedException(
                            inFlightException,
                            t,
                            "Error freeing memory for operator %s for task %s",
                            operator.getOperatorContext().getOperatorId(),
                            driverContext.getTaskId());
                }
                try {
                    operator.getOperatorContext().closeSystemMemoryContext();
                }
                catch (Throwable t) {
                    inFlightException = addSuppressedException(
                            inFlightException,
                            t,
                            "Error freeing system memory for operator %s for task %s",
                            operator.getOperatorContext().getOperatorId(),
                            driverContext.getTaskId());
                }
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
        finally {
            // reset the interrupted flag
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }

        if (inFlightException != null) {
            // this will always be an Error or Runtime
            throw Throwables.propagate(inFlightException);
        }
    }

    private static ListenableFuture<?> isBlocked(Operator operator)
    {
        ListenableFuture<?> blocked = operator.isBlocked();
        if (blocked.isDone()) {
            blocked = operator.getOperatorContext().isWaitingForMemory();
        }
        return blocked;
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

    private DriverLockResult tryLockAndProcessPendingStateChanges(int timeout, TimeUnit unit)
    {
        checkLockNotHeld("Can not acquire the driver lock while already holding the driver lock");

        return new DriverLockResult(timeout, unit);
    }

    private synchronized void checkLockNotHeld(String message)
    {
        checkState(Thread.currentThread() != lockHolder, message);
    }

    private synchronized void checkLockHeld(String message)
    {
        checkState(Thread.currentThread() == lockHolder, message);
    }

    private static ListenableFuture<?> firstFinishedFuture(List<ListenableFuture<?>> futures)
    {
        SettableFuture<?> result = SettableFuture.create();
        ExecutorService executor = MoreExecutors.newDirectExecutorService();

        for (ListenableFuture<?> future : futures) {
            future.addListener(() -> result.set(null), executor);
        }

        return result;
    }

    private class DriverLockResult
            implements AutoCloseable
    {
        private final boolean acquired;

        private DriverLockResult(int timeout, TimeUnit unit)
        {
            acquired = tryAcquire(timeout, unit);
        }

        private boolean tryAcquire(int timeout, TimeUnit unit)
        {
            boolean acquired = false;
            try {
                acquired = exclusiveLock.tryLock(timeout, unit);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            if (acquired) {
                synchronized (Driver.this) {
                    lockHolder = Thread.currentThread();
                }
            }

            return acquired;
        }

        public boolean wasAcquired()
        {
            return acquired;
        }

        @Override
        public void close()
        {
            if (!acquired) {
                return;
            }

            boolean done = false;
            while (!done) {
                done = true;
                // before releasing the lock, process any new sources and/or destroy the driver
                try {
                    try {
                        processNewSources();
                    }
                    finally {
                        destroyIfNecessary();
                    }
                }
                finally {
                    synchronized (Driver.this) {
                        lockHolder = null;
                    }
                    exclusiveLock.unlock();

                    // if new sources were added after we processed them, go around and try again
                    // in case someone else failed to acquire the lock and as a result won't update them
                    if (!newSources.isEmpty() && state.get() == State.ALIVE && tryAcquire(0, TimeUnit.MILLISECONDS)) {
                        done = false;
                    }
                }
            }
        }
    }
}
