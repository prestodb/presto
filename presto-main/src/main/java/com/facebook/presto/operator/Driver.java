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
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.CollocatedSplit;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class Driver
{
    private static final Logger log = Logger.get(Driver.class);

    private final DriverContext driverContext;
    private final List<Operator> operators;
    private final Map<PlanNodeId, SourceOperator> sourceOperators;

    public Driver(DriverContext driverContext, Operator firstOperator, Operator... otherOperators)
    {
        this(checkNotNull(driverContext, "driverContext is null"),
                ImmutableList.<Operator>builder()
                        .add(checkNotNull(firstOperator, "firstOperator is null"))
                        .add(checkNotNull(otherOperators, "otherOperators is null"))
                        .build());
    }

    public Driver(DriverContext driverContext, List<Operator> operators)
    {
        this.driverContext = checkNotNull(driverContext, "driverContext is null");
        this.operators = ImmutableList.copyOf(checkNotNull(operators, "operators is null"));
        checkArgument(!operators.isEmpty(), "There must be at least one operator");

        ImmutableMap.Builder<PlanNodeId, SourceOperator> sourceOperators = ImmutableMap.builder();
        for (Operator operator : operators) {
            if (operator instanceof SourceOperator) {
                SourceOperator sourceOperator = (SourceOperator) operator;
                sourceOperators.put(sourceOperator.getSourceId(), sourceOperator);
            }
        }
        this.sourceOperators = sourceOperators.build();
    }

    public synchronized void close()
    {
        try {
            for (Operator operator : operators) {
                operator.finish();
            }
        }
        finally {
            for (Operator operator : operators) {
                if (operator instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) operator).close();
                    }
                    catch (Exception e) {
                        if (e instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        log.error(e, "Error closing operator %s for task %s", operator.getOperatorContext().getOperatorId(), driverContext.getTaskId());
                    }
                }
            }
            driverContext.finished();
        }
    }

    public DriverContext getDriverContext()
    {
        return driverContext;
    }

    public Set<PlanNodeId> getSourceIds()
    {
        return sourceOperators.keySet();
    }


    private final ConcurrentMap<PlanNodeId, TaskSource> sources = new ConcurrentHashMap<>();

    public synchronized void updateSource(TaskSource source)
    {
        // does this driver have an operator for the specified source?
        PlanNodeId sourceId = source.getPlanNodeId();
        if (!sourceOperators.containsKey(sourceId)) {
            return;
        }

        // create new source
        Set<ScheduledSplit> newSplits;
        TaskSource currentSource = sources.get(sourceId);
        if (currentSource == null) {
            newSplits = source.getSplits();
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
            sources.put(sourceId, newSource);
        }

        // add new splits
        for (ScheduledSplit newSplit : newSplits) {
            addSplit(sourceId, newSplit.getSplit());
        }

        // set no more splits
        if (source.isNoMoreSplits()) {
            sourceOperators.get(sourceId).noMoreSplits();
        }
    }

    private synchronized void addSplit(PlanNodeId sourceId, Split split)
    {
        checkNotNull(sourceId, "sourceId is null");
        checkNotNull(split, "split is null");

        if (split instanceof CollocatedSplit) {
            CollocatedSplit collocatedSplit = (CollocatedSplit) split;
            // unwind collocated splits
            for (Entry<PlanNodeId, Split> entry : collocatedSplit.getSplits().entrySet()) {
                addSplit(entry.getKey(), entry.getValue());
            }
        }
        else {
            SourceOperator sourceOperator = sourceOperators.get(sourceId);
            if (sourceOperator != null) {
                sourceOperator.addSplit(split);
            }
        }
    }

    public synchronized boolean isFinished()
    {
        boolean finished = driverContext.isDone() || operators.get(operators.size() - 1).isFinished();
        if (finished) {
            close();
        }
        return finished;
    }

    public synchronized ListenableFuture<?> process()
    {
        driverContext.start();

        try {
            for (int i = 0; i < operators.size() - 1 && !driverContext.isDone(); i++) {
                // check if current operator is blocked
                Operator current = operators.get(i);
                ListenableFuture<?> blocked = current.isBlocked();
                if (!blocked.isDone()) {
                    current.getOperatorContext().recordBlocked(blocked);
                    return blocked;
                }

                // check if next operator is blocked
                Operator next = operators.get(i + 1);
                blocked = next.isBlocked();
                if (!blocked.isDone()) {
                    next.getOperatorContext().recordBlocked(blocked);
                    return blocked;
                }

                // if current operator is finished...
                if (current.isFinished()) {
                    // let next operator know there will be no more data
                    next.getOperatorContext().startIntervalTimer();
                    next.finish();
                    next.getOperatorContext().recordFinish();
                }
                else {
                    // if next operator needs input...
                    if (next.needsInput()) {
                        // get an output page from current operator
                        current.getOperatorContext().startIntervalTimer();
                        Page page = current.getOutput();
                        current.getOperatorContext().recordGetOutput(page);

                        // if we got an output page, add it to the next operator
                        if (page != null) {
                            next.getOperatorContext().startIntervalTimer();
                            next.addInput(page);
                            next.getOperatorContext().recordAddInput(page);
                        }
                    }
                }
            }
            return NOT_BLOCKED;
        }
        catch (Throwable t) {
            driverContext.failed(t);
            throw t;
        }
    }

    public ListenableFuture<?> processFor(Duration duration)
    {
        checkNotNull(duration, "duration is null");
        checkState(!Thread.holdsLock(this), "Can not process for a duration while holding a lock on the %s", getClass().getSimpleName());

        long maxRuntime = duration.roundTo(TimeUnit.NANOSECONDS);

        long start = System.nanoTime();
        do {
            ListenableFuture<?> future = process();
            if (!future.isDone()) {
                return future;
            }
        }
        while (System.nanoTime() - start < maxRuntime && !isFinished());

        return NOT_BLOCKED;
    }
}
