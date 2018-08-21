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

import com.facebook.presto.operator.WorkProcessor.ProcessorState;
import com.facebook.presto.operator.WorkProcessor.Transformation;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.function.Function;

import static com.facebook.presto.operator.WorkProcessor.ProcessorState.Type.BLOCKED;
import static com.facebook.presto.operator.WorkProcessor.ProcessorState.Type.FINISHED;
import static com.facebook.presto.operator.WorkProcessor.ProcessorState.Type.NEEDS_MORE_DATA;
import static com.facebook.presto.operator.WorkProcessor.ProcessorState.Type.RESULT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public final class WorkProcessorUtils
{
    private WorkProcessorUtils() {}

    static <T> Iterator<T> iteratorFrom(WorkProcessor<T> processor)
    {
        requireNonNull(processor, "processor is null");
        return new AbstractIterator<T>()
        {
            final Iterator<Optional<T>> yieldingIterator = yieldingIteratorFrom(processor);

            @Override
            protected T computeNext()
            {
                if (!yieldingIterator.hasNext()) {
                    return endOfData();
                }

                return yieldingIterator.next()
                        .orElseThrow(() -> new IllegalStateException("Cannot iterate over yielding WorkProcessor"));
            }
        };
    }

    static <T> Iterator<Optional<T>> yieldingIteratorFrom(WorkProcessor<T> processor)
    {
        return new YieldingIterator<>(processor);
    }

    private static class YieldingIterator<T>
            extends AbstractIterator<Optional<T>>
    {
        @Nullable
        private WorkProcessor<T> processor;

        private YieldingIterator(WorkProcessor<T> processor)
        {
            this.processor = requireNonNull(processor, "processorParameter is null");
        }

        @Override
        protected Optional<T> computeNext()
        {
            if (processor.process()) {
                if (processor.isFinished()) {
                    processor = null;
                    return endOfData();
                }

                return Optional.of(processor.getResult());
            }

            if (processor.isBlocked()) {
                throw new IllegalStateException("Cannot iterate over blocking WorkProcessor");
            }

            // yielded
            return Optional.empty();
        }
    }

    static <T> WorkProcessor<T> fromIterator(Iterator<T> iterator)
    {
        requireNonNull(iterator, "iterator is null");
        return create(() -> {
            if (!iterator.hasNext()) {
                return ProcessorState.finished();
            }

            return ProcessorState.ofResult(iterator.next());
        });
    }

    static <T> WorkProcessor<T> mergeSorted(Iterable<WorkProcessor<T>> processorIterable, Comparator<T> comparator)
    {
        requireNonNull(comparator, "comparator is null");
        Iterator<WorkProcessor<T>> processorIterator = requireNonNull(processorIterable, "processorIterable is null").iterator();
        checkArgument(processorIterator.hasNext(), "There must be at least one base processor");
        PriorityQueue<ElementAndProcessor<T>> queue = new PriorityQueue<>(2, comparing(ElementAndProcessor::getElement, comparator));

        return create(new WorkProcessor.Process<T>()
        {
            WorkProcessor<T> processor = requireNonNull(processorIterator.next());

            @Override
            public ProcessorState<T> process()
            {
                while (true) {
                    if (processor.process()) {
                        if (!processor.isFinished()) {
                            queue.add(new ElementAndProcessor<>(processor.getResult(), processor));
                        }
                    }
                    else if (processor.isBlocked()) {
                        return ProcessorState.blocked(processor.getBlockedFuture());
                    }
                    else {
                        return ProcessorState.yield();
                    }

                    if (processorIterator.hasNext()) {
                        processor = processorIterator.next();
                        continue;
                    }

                    if (queue.isEmpty()) {
                        return ProcessorState.finished();
                    }

                    ElementAndProcessor<T> elementAndProcessor = queue.poll();
                    processor = elementAndProcessor.getProcessor();
                    return ProcessorState.ofResult(elementAndProcessor.getElement());
                }
            }
        });
    }

    static <T, R> WorkProcessor<R> flatMap(WorkProcessor<T> processor, Function<T, WorkProcessor<R>> mapper)
    {
        requireNonNull(processor, "processor is null");
        requireNonNull(mapper, "mapper is null");
        return processor.flatTransform(elementOptional ->
                elementOptional
                        .map(element -> ProcessorState.ofResult(mapper.apply(element)))
                        .orElse(ProcessorState.finished()));
    }

    static <T, R> WorkProcessor<R> map(WorkProcessor<T> processor, Function<T, R> mapper)
    {
        requireNonNull(processor, "processor is null");
        requireNonNull(mapper, "mapper is null");
        return processor.transform(elementOptional ->
                elementOptional
                        .map(element -> ProcessorState.ofResult(mapper.apply(element)))
                        .orElse(ProcessorState.finished()));
    }

    static <T, R> WorkProcessor<R> flatTransform(WorkProcessor<T> processor, Transformation<T, WorkProcessor<R>> transformation)
    {
        requireNonNull(processor, "processor is null");
        requireNonNull(transformation, "transformation is null");
        return processor.transform(transformation).transformProcessor(WorkProcessorUtils::flatten);
    }

    static <T> WorkProcessor<T> flatten(WorkProcessor<WorkProcessor<T>> processor)
    {
        requireNonNull(processor, "processor is null");
        return processor.transform(nestedProcessorOptional -> {
            if (!nestedProcessorOptional.isPresent()) {
                return ProcessorState.finished();
            }

            WorkProcessor<T> nestedProcessor = nestedProcessorOptional.get();
            if (nestedProcessor.process()) {
                if (nestedProcessor.isFinished()) {
                    return ProcessorState.needsMoreData();
                }

                return ProcessorState.ofResult(nestedProcessor.getResult(), false);
            }

            if (nestedProcessor.isBlocked()) {
                return ProcessorState.blocked(nestedProcessor.getBlockedFuture());
            }

            return ProcessorState.yield();
        });
    }

    static <T, R> WorkProcessor<R> transform(WorkProcessor<T> processor, Transformation<T, R> transformation)
    {
        requireNonNull(processor, "processor is null");
        requireNonNull(transformation, "transformation is null");
        return create(new WorkProcessor.Process<R>()
        {
            Optional<T> element = Optional.empty();

            @Override
            public ProcessorState<R> process()
            {
                while (true) {
                    if (!element.isPresent() && !processor.isFinished()) {
                        if (processor.process()) {
                            if (!processor.isFinished()) {
                                element = Optional.of(processor.getResult());
                            }
                        }
                        else if (processor.isBlocked()) {
                            return ProcessorState.blocked(processor.getBlockedFuture());
                        }
                        else {
                            return ProcessorState.yield();
                        }
                    }

                    ProcessorState<R> state = requireNonNull(transformation.process(element), "state is null");

                    if (state.isNeedsMoreData()) {
                        checkState(!processor.isFinished(), "Cannot request more data when base processor is finished");
                        // set element to empty() in order to fetch a new one
                        element = Optional.empty();
                    }

                    if (state.getType() != NEEDS_MORE_DATA) {
                        // passthrough transformation state if it doesn't require new data
                        return state;
                    }
                }
            }
        });
    }

    static <T> WorkProcessor<T> create(WorkProcessor.Process<T> process)
    {
        return new ProcessWorkProcessor<>(process);
    }

    private static class ProcessWorkProcessor<T>
            implements WorkProcessor<T>
    {
        @Nullable
        private WorkProcessor.Process<T> process;
        @Nullable
        private ProcessorState<T> state;

        private ProcessWorkProcessor(WorkProcessor.Process<T> process)
        {
            this.process = requireNonNull(process, "process is null");
        }

        @Override
        public boolean process()
        {
            if (isBlocked()) {
                return false;
            }
            if (isFinished()) {
                return true;
            }
            state = requireNonNull(process.process());
            checkState(state.getType() != NEEDS_MORE_DATA, "Unexpected state: NEEDS_MORE_DATA");

            if (state.getType() == FINISHED) {
                process = null;
            }

            return state.getType() == RESULT || state.getType() == FINISHED;
        }

        @Override
        public boolean isBlocked()
        {
            return state != null && state.getType() == BLOCKED && !state.getBlocked().get().isDone();
        }

        @Override
        public ListenableFuture<?> getBlockedFuture()
        {
            checkState(state != null && state.getType() == BLOCKED, "Must be blocked to get blocked future");
            return state.getBlocked().get();
        }

        @Override
        public boolean isFinished()
        {
            return state != null && state.getType() == FINISHED;
        }

        @Override
        public T getResult()
        {
            checkState(state != null && state.getType() == RESULT, "process() must return true and must not be finished");
            return state.getResult().get();
        }
    }

    private static class ElementAndProcessor<T>
    {
        final T element;
        final WorkProcessor<T> processor;

        ElementAndProcessor(T element, WorkProcessor<T> processor)
        {
            this.element = element;
            this.processor = processor;
        }

        T getElement()
        {
            return element;
        }

        WorkProcessor<T> getProcessor()
        {
            return processor;
        }
    }
}
