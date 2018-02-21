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
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public final class ContinuousWorkUtils
{
    private ContinuousWorkUtils() {}

    static <T> ContinuousWork<T> fromIterator(Iterator<T> iterator)
    {
        return create(() -> {
            if (!iterator.hasNext()) {
                return WorkState.finished();
            }

            return WorkState.ofResult(iterator.next());
        });
    }

    static <T> ContinuousWork<T> mergeSorted(Iterable<ContinuousWork<T>> workIterable, Comparator<T> comparator)
    {
        requireNonNull(comparator, "comparator is null");
        Iterator<ContinuousWork<T>> workIterator = requireNonNull(workIterable, "workIterable is null").iterator();
        checkArgument(workIterator.hasNext(), "There must be at least one base work");
        PriorityQueue<ElementAndWork<T>> queue = new PriorityQueue<>(2, comparing(ElementAndWork::getElement, comparator));

        return create(new Supplier<WorkState<T>>()
        {
            ContinuousWork<T> work = workIterator.next();

            @Override
            public WorkState<T> get()
            {
                while (true) {
                    if (work.process()) {
                        if (!work.isFinished()) {
                            queue.add(new ElementAndWork<>(work.getResult(), work));
                        }
                    }
                    else if (work.isBlocked()) {
                        return WorkState.blocked(work.getBlockedFuture());
                    }
                    else {
                        return WorkState.yield();
                    }

                    if (workIterator.hasNext()) {
                        work = workIterator.next();
                        continue;
                    }

                    if (queue.isEmpty()) {
                        return WorkState.finished();
                    }

                    ElementAndWork<T> elementAndWork = queue.poll();
                    work = elementAndWork.getWork();
                    return WorkState.ofResult(elementAndWork.getElement());
                }
            }
        });
    }

    static <T, R> ContinuousWork<R> flatMap(ContinuousWork<T> work, Function<T, Iterator<R>> mapper)
    {
        return flatten(map(work, mapper));
    }

    private static <T> ContinuousWork<T> flatten(ContinuousWork<Iterator<T>> work)
    {
        return work.transform(iteratorOptional ->
                iteratorOptional
                        .map(iterator -> {
                            if (iterator.hasNext()) {
                                return WorkState.ofResult(iterator.next(), false);
                            }
                            else {
                                return WorkState.<T>needsMoreData();
                            }
                        })
                        .orElse(WorkState.finished()));
    }

    static <T, R> ContinuousWork<R> map(ContinuousWork<T> work, Function<T, R> mapper)
    {
        return work.transform(elementOptional ->
                elementOptional
                        .map(element -> WorkState.ofResult(mapper.apply(element)))
                        .orElse(WorkState.finished()));
    }

    static <T, R> ContinuousWork<R> transform(ContinuousWork<T> work, Function<Optional<T>, WorkState<R>> transformation)
    {
        return create(new Supplier<WorkState<R>>()
        {
            Optional<T> element = Optional.empty();

            @Override
            public WorkState<R> get()
            {
                while (true) {
                    if (!element.isPresent() && !work.isFinished()) {
                        if (work.process()) {
                            if (!work.isFinished()) {
                                element = Optional.of(work.getResult());
                            }
                        }
                        else if (work.isBlocked()) {
                            return WorkState.blocked(work.getBlockedFuture());
                        }
                        else {
                            return WorkState.yield();
                        }
                    }

                    WorkState<R> state = requireNonNull(transformation.apply(element), "state is null");

                    if (state.needsMoreData) {
                        checkState(!work.isFinished(), "Cannot request more data when base work is finished");
                        // set element to empty() in order to fetch a new one
                        element = Optional.empty();
                    }

                    if (state.type != WorkState.Type.NEEDS_MORE_DATA) {
                        // passthrough transformation state if it doesn't require new data
                        return state;
                    }
                }
            }
        });
    }

    static <T> ContinuousWork<T> create(Supplier<WorkState<T>> supplier)
    {
        return new ContinuousWork<T>()
        {
            WorkState<T> state;

            @Override
            public boolean process()
            {
                if (isBlocked()) {
                    return false;
                }
                if (isFinished()) {
                    return true;
                }
                state = requireNonNull(supplier.get());
                return state.type == WorkState.Type.RESULT || state.type == WorkState.Type.FINISHED;
            }

            @Override
            public boolean isBlocked()
            {
                return state != null && state.type == WorkState.Type.BLOCKED && !state.blocked.get().isDone();
            }

            @Override
            public ListenableFuture<?> getBlockedFuture()
            {
                checkState(state != null && state.type == WorkState.Type.BLOCKED, "Must be blocked to get blocked future");
                return state.blocked.get();
            }

            @Override
            public boolean isFinished()
            {
                return state != null && state.type == WorkState.Type.FINISHED;
            }

            @Override
            public T getResult()
            {
                checkState(state != null && state.type == WorkState.Type.RESULT, "process() must return true and must not be finished");
                return state.result.get();
            }
        };
    }

    @Immutable
    public static final class WorkState<T>
    {
        private static final WorkState NEEDS_MORE_DATE_STATE = new WorkState<>(Type.NEEDS_MORE_DATA, true, Optional.empty(), Optional.empty());
        private static final WorkState YIELD_STATE = new WorkState<>(Type.YIELD, false, Optional.empty(), Optional.empty());
        private static final WorkState FINISHED_STATE = new WorkState<>(Type.FINISHED, false, Optional.empty(), Optional.empty());

        private enum Type
        {
            NEEDS_MORE_DATA,
            BLOCKED,
            YIELD,
            RESULT,
            FINISHED
        }

        private final Type type;
        private final boolean needsMoreData;
        private final Optional<T> result;
        private final Optional<ListenableFuture<?>> blocked;

        private WorkState(Type type, boolean needsMoreData, Optional<T> result, Optional<ListenableFuture<?>> blocked)
        {
            this.type = type;
            this.needsMoreData = needsMoreData;
            this.result = result;
            this.blocked = blocked;
        }

        public static <T> WorkState<T> needsMoreData()
        {
            return NEEDS_MORE_DATE_STATE;
        }

        public static <T> WorkState<T> blocked(ListenableFuture<?> blocked)
        {
            return new WorkState<>(Type.BLOCKED, false, Optional.empty(), Optional.of(blocked));
        }

        public static <T> WorkState<T> yield()
        {
            return YIELD_STATE;
        }

        public static <T> WorkState<T> ofResult(T result)
        {
            return ofResult(result, true);
        }

        public static <T> WorkState<T> ofResult(T result, boolean needsMoreData)
        {
            return new WorkState<>(Type.RESULT, needsMoreData, Optional.of(result), Optional.empty());
        }

        public static <T> WorkState<T> finished()
        {
            return FINISHED_STATE;
        }
    }

    private static class ElementAndWork<T>
    {
        final T element;
        final ContinuousWork<T> work;

        ElementAndWork(T element, ContinuousWork<T> work)
        {
            this.element = element;
            this.work = work;
        }

        T getElement()
        {
            return element;
        }

        ContinuousWork<T> getWork()
        {
            return work;
        }
    }
}
