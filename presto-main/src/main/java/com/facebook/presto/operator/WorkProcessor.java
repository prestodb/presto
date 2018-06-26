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

import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.Immutable;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.operator.WorkProcessor.ProcessorState.Type.BLOCKED;
import static com.facebook.presto.operator.WorkProcessor.ProcessorState.Type.FINISHED;
import static com.facebook.presto.operator.WorkProcessor.ProcessorState.Type.NEEDS_MORE_DATA;
import static com.facebook.presto.operator.WorkProcessor.ProcessorState.Type.RESULT;
import static com.facebook.presto.operator.WorkProcessor.ProcessorState.Type.YIELD;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public interface WorkProcessor<T>
{
    /**
     * Call the method to progress the work.
     * When this method returns true then the processor is either finished
     * or has a result available via {@link WorkProcessor#getResult()}.
     * When this method returns false then the processor is either
     * blocked or has yielded.
     */
    boolean process();

    boolean isBlocked();

    /**
     * @return a blocked future when {@link WorkProcessor#isBlocked()} returned true.
     */
    ListenableFuture<?> getBlockedFuture();

    /**
     * @return true if the processor is finished. No more results are expected.
     */
    boolean isFinished();

    /**
     * Get the result once the unit of work is done and the processor hasn't finished.
     */
    T getResult();

    default <R> WorkProcessor<R> flatMap(Function<T, WorkProcessor<R>> mapper)
    {
        return WorkProcessorUtils.flatMap(this, mapper);
    }

    default <R> WorkProcessor<R> map(Function<T, R> mapper)
    {
        return WorkProcessorUtils.map(this, mapper);
    }

    /**
     * Flattens {@link WorkProcessor}s returned by transformation. Each {@link WorkProcessor} produced
     * by transformation will be fully consumed before transformation is called again to produce more processors.
     */
    default <R> WorkProcessor<R> flatTransform(Transformation<T, WorkProcessor<R>> transformation)
    {
        return WorkProcessorUtils.flatTransform(this, transformation);
    }

    default <R> WorkProcessor<R> transform(Transformation<T, R> transformation)
    {
        return WorkProcessorUtils.transform(this, transformation);
    }

    /**
     * Converts {@link WorkProcessor} into an {@link Iterator}. The iterator will throw {@link IllegalStateException} when underlying {@link WorkProcessor}
     * yields or becomes blocked.
     */
    default Iterator<T> iterator()
    {
        return WorkProcessorUtils.iteratorFrom(this);
    }

    /**
     * Converts {@link WorkProcessor} into an yielding {@link Iterator}. The iterator will throw {@link IllegalStateException} when underlying {@link WorkProcessor}
     * becomes blocked.
     */
    default Iterator<Optional<T>> yieldingIterator()
    {
        return WorkProcessorUtils.yieldingIteratorFrom(this);
    }

    static <T> WorkProcessor<T> fromIterable(Iterable<T> iterable)
    {
        return WorkProcessorUtils.fromIterator(iterable.iterator());
    }

    static <T> WorkProcessor<T> fromIterator(Iterator<T> iterator)
    {
        return WorkProcessorUtils.fromIterator(iterator);
    }

    static <T> WorkProcessor<T> create(Process<T> process)
    {
        return WorkProcessorUtils.create(process);
    }

    static <T> WorkProcessor<T> mergeSorted(Iterable<WorkProcessor<T>> processorIterable, Comparator<T> comparator)
    {
        return WorkProcessorUtils.mergeSorted(processorIterable, comparator);
    }

    interface Transformation<T, R>
    {
        ProcessorState<R> process(Optional<T> elementOptional);
    }

    interface Process<T>
    {
        ProcessorState<T> process();
    }

    @Immutable
    final class ProcessorState<T>
    {
        private static final ProcessorState NEEDS_MORE_DATE_STATE = new ProcessorState<>(NEEDS_MORE_DATA, true, Optional.empty(), Optional.empty());
        private static final ProcessorState YIELD_STATE = new ProcessorState<>(YIELD, false, Optional.empty(), Optional.empty());
        private static final ProcessorState FINISHED_STATE = new ProcessorState<>(FINISHED, false, Optional.empty(), Optional.empty());

        enum Type
        {
            NEEDS_MORE_DATA,
            BLOCKED,
            YIELD,
            RESULT,
            FINISHED
        }

        private final ProcessorState.Type type;
        private final boolean needsMoreData;
        private final Optional<T> result;
        private final Optional<ListenableFuture<?>> blocked;

        ProcessorState(Type type, boolean needsMoreData, Optional<T> result, Optional<ListenableFuture<?>> blocked)
        {
            this.type = requireNonNull(type, "type is null");
            this.needsMoreData = needsMoreData;
            this.result = requireNonNull(result, "result is null");
            this.blocked = requireNonNull(blocked, "blocked is null");

            checkArgument(!needsMoreData || type == NEEDS_MORE_DATA || type == RESULT);
            checkArgument(!blocked.isPresent() || type == BLOCKED);
            checkArgument(!result.isPresent() || type == RESULT);
        }

        public static <T> ProcessorState<T> needsMoreData()
        {
            return NEEDS_MORE_DATE_STATE;
        }

        public static <T> ProcessorState<T> blocked(ListenableFuture<?> blocked)
        {
            return new ProcessorState<>(Type.BLOCKED, false, Optional.empty(), Optional.of(blocked));
        }

        public static <T> ProcessorState<T> yield()
        {
            return YIELD_STATE;
        }

        public static <T> ProcessorState<T> ofResult(T result)
        {
            return ofResult(result, true);
        }

        public static <T> ProcessorState<T> ofResult(T result, boolean needsMoreData)
        {
            return new ProcessorState<>(Type.RESULT, needsMoreData, Optional.of(result), Optional.empty());
        }

        public static <T> ProcessorState<T> finished()
        {
            return FINISHED_STATE;
        }

        Type getType()
        {
            return type;
        }

        boolean isNeedsMoreData()
        {
            return needsMoreData;
        }

        Optional<T> getResult()
        {
            return result;
        }

        Optional<ListenableFuture<?>> getBlocked()
        {
            return blocked;
        }
    }
}
