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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Throwables;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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
    private final List<Operator> operators;
    private final Optional<SourceOperator> sourceOperator;
    private final Optional<DeleteOperator> deleteOperator;
    private final AtomicReference<TaskSource> newTaskSource = new AtomicReference<>();

    private final AtomicReference<State> state = new AtomicReference<>(State.ALIVE);

    private final DriverLock exclusiveLock = new DriverLock();

    @GuardedBy("exclusiveLock")
    private TaskSource currentTaskSource;

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

        currentTaskSource = sourceOperator.map(operator -> new TaskSource(operator.getSourceId(), ImmutableSet.of(), false)).orElse(null);
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
        newTaskSource.updateAndGet(current -> current == null ? source : current.update(source));

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

        TaskSource source = newTaskSource.getAndSet(null);
        if (source == null) {
            return;
        }

        // merge the current source and the specified source
        TaskSource newSource = currentTaskSource.update(source);

        // if source contains no new data, just return
        if (newSource == currentTaskSource) {
            return;
        }

        // determine new splits to add
        Set<ScheduledSplit> newSplits = Sets.difference(newSource.getSplits(), currentTaskSource.getSplits());

        // add new splits
        SourceOperator sourceOperator = this.sourceOperator.orElseThrow(VerifyException::new);
        for (ScheduledSplit newSplit : newSplits) {
            Split split = newSplit.getSplit();

            Supplier<Optional<UpdatablePageSource>> pageSource = sourceOperator.addSplit(split);
            deleteOperator.ifPresent(deleteOperator -> deleteOperator.setPageSource(pageSource));
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

        long maxRuntime = duration.roundTo(TimeUnit.NANOSECONDS);

        Optional<ListenableFuture<?>> result = tryWithLock(100, TimeUnit.MILLISECONDS, () -> {
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
            return NOT_BLOCKED;
        });
        return result.orElse(NOT_BLOCKED);
    }

    public ListenableFuture<?> process()
    {
        checkLockNotHeld("Can not process while holding the driver lock");

        Optional<ListenableFuture<?>> result = tryWithLock(100, TimeUnit.MILLISECONDS, this::processInternal);
        return result.orElse(NOT_BLOCKED);
    }

    @GuardedBy("exclusiveLock")
    private ListenableFuture<?> processInternal()
    {
        checkLockHeld("Lock must be held to call processInternal");

        try {
            processNewSources();

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

                    if (current instanceof SourceOperator) {
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
            if (driverContext.getMemoryUsage() > 0) {
                log.error("Driver still has memory reserved after freeing all operator memory. Freeing it.");
            }
            if (driverContext.getSystemMemoryUsage() > 0) {
                log.error("Driver still has system memory reserved after freeing all operator memory. Freeing it.");
            }
            driverContext.freeMemory(driverContext.getMemoryUsage());
            driverContext.freeSystemMemory(driverContext.getSystemMemoryUsage());
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
            Throwables.throwIfUnchecked(inFlightException);
            throw new RuntimeException(inFlightException);
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
        SettableFuture<?> result = SettableFuture.create();
        ExecutorService executor = MoreExecutors.newDirectExecutorService();

        for (ListenableFuture<?> future : futures) {
            future.addListener(() -> result.set(null), executor);
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

        boolean acquired = false;
        try {
            acquired = exclusiveLock.tryLock(timeout, unit);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

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

        // if necessary, attempt to reacquire the lock and process new sources
        // NOTE: this is separate duplicate code to make debugging lock reacquisition easier
        while (newTaskSource.get() != null && state.get() == State.ALIVE && exclusiveLock.tryLock()) {
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
                throws InterruptedException
        {
            checkState(!lock.isHeldByCurrentThread(), "Lock is not reentrant");
            boolean acquired = lock.tryLock(timeout, unit);
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
