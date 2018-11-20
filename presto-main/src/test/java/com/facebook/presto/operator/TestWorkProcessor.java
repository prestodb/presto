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

import com.facebook.presto.operator.WorkProcessor.ProcessState;
import com.facebook.presto.operator.WorkProcessor.TransformationState;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import org.testng.annotations.Test;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.operator.WorkProcessorAssertion.assertBlocks;
import static com.facebook.presto.operator.WorkProcessorAssertion.assertFinishes;
import static com.facebook.presto.operator.WorkProcessorAssertion.assertResult;
import static com.facebook.presto.operator.WorkProcessorAssertion.assertUnblocks;
import static com.facebook.presto.operator.WorkProcessorAssertion.assertYields;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestWorkProcessor
{
    @Test
    public void testIterator()
    {
        WorkProcessor<Integer> processor = processorFrom(ImmutableList.of(
                ProcessState.ofResult(1),
                ProcessState.ofResult(2),
                ProcessState.finished()));

        Iterator<Integer> iterator = processor.iterator();
        assertTrue(iterator.hasNext());
        assertEquals(iterator.next(), (Integer) 1);
        assertTrue(iterator.hasNext());
        assertEquals(iterator.next(), (Integer) 2);
        assertFalse(iterator.hasNext());
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Cannot iterate over yielding WorkProcessor")
    public void testIteratorFailsWhenWorkProcessorHasYielded()
    {
        // iterator should fail if underlying work has yielded
        WorkProcessor<Integer> processor = processorFrom(ImmutableList.of(ProcessState.yield()));
        Iterator<Integer> iterator = processor.iterator();
        iterator.hasNext();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Cannot iterate over blocking WorkProcessor")
    public void testIteratorFailsWhenWorkProcessorIsBlocked()
    {
        // iterator should fail if underlying work is blocked
        WorkProcessor<Integer> processor = processorFrom(ImmutableList.of(ProcessState.blocked(SettableFuture.create())));
        Iterator<Integer> iterator = processor.iterator();
        iterator.hasNext();
    }

    @Test(timeOut = 5000)
    public void testMergeSorted()
    {
        List<ProcessState<Integer>> firstStream = ImmutableList.of(
                ProcessState.ofResult(1),
                ProcessState.ofResult(3),
                ProcessState.yield(),
                ProcessState.ofResult(5),
                ProcessState.finished());

        SettableFuture<?> secondFuture = SettableFuture.create();
        List<ProcessState<Integer>> secondStream = ImmutableList.of(
                ProcessState.ofResult(2),
                ProcessState.ofResult(4),
                ProcessState.blocked(secondFuture),
                ProcessState.finished());

        WorkProcessor<Integer> mergedStream = WorkProcessorUtils.mergeSorted(
                ImmutableList.of(processorFrom(firstStream), processorFrom(secondStream)),
                Comparator.comparingInt(firstInteger -> firstInteger));

        // first stream result 1
        assertResult(mergedStream, 1);

        // second stream result 2
        assertResult(mergedStream, 2);

        // first stream result 3
        assertResult(mergedStream, 3);

        // first stream yield
        assertYields(mergedStream);

        // second stream result 4
        assertResult(mergedStream, 4);

        // second stream blocked
        assertBlocks(mergedStream);

        // second stream unblock
        assertUnblocks(mergedStream, secondFuture);

        // first stream result 5
        assertResult(mergedStream, 5);

        // both streams finished
        assertFinishes(mergedStream);
    }

    @Test(timeOut = 5000)
    public void testMergeSortedEmptyStreams()
    {
        SettableFuture<?> firstFuture = SettableFuture.create();
        List<ProcessState<Integer>> firstStream = ImmutableList.of(
                ProcessState.blocked(firstFuture),
                ProcessState.yield(),
                ProcessState.finished());

        SettableFuture<?> secondFuture = SettableFuture.create();
        List<ProcessState<Integer>> secondStream = ImmutableList.of(
                ProcessState.blocked(secondFuture),
                ProcessState.finished());

        WorkProcessor<Integer> mergedStream = WorkProcessorUtils.mergeSorted(
                ImmutableList.of(processorFrom(firstStream), processorFrom(secondStream)),
                Comparator.comparingInt(firstInteger -> firstInteger));

        assertFalse(mergedStream.isBlocked());
        assertFalse(mergedStream.isFinished());

        // first stream blocked
        assertBlocks(mergedStream);

        // first stream unblock
        assertUnblocks(mergedStream, firstFuture);

        // first stream yield
        assertYields(mergedStream);

        // second stream blocked
        assertBlocks(mergedStream);

        // first stream unblock
        assertUnblocks(mergedStream, secondFuture);

        // both streams finished
        assertFinishes(mergedStream);
    }

    @Test(timeOut = 5000)
    public void testMergeSortedEmptyStreamsWithFinishedOnly()
    {
        List<ProcessState<Integer>> firstStream = ImmutableList.of(
                ProcessState.finished());

        List<ProcessState<Integer>> secondStream = ImmutableList.of(
                ProcessState.finished());

        WorkProcessor<Integer> mergedStream = WorkProcessorUtils.mergeSorted(
                ImmutableList.of(processorFrom(firstStream), processorFrom(secondStream)),
                Comparator.comparingInt(firstInteger -> firstInteger));

        // before
        assertFalse(mergedStream.isBlocked());
        assertFalse(mergedStream.isFinished());

        assertFinishes(mergedStream);
    }

    @Test(timeOut = 5000)
    public void testYield()
    {
        SettableFuture<?> future = SettableFuture.create();

        List<ProcessState<Integer>> baseScenario = ImmutableList.of(
                ProcessState.ofResult(1),
                ProcessState.ofResult(2),
                ProcessState.blocked(future),
                ProcessState.ofResult(3),
                ProcessState.ofResult(4),
                ProcessState.finished());

        AtomicBoolean yieldSignal = new AtomicBoolean();
        WorkProcessor<Integer> processor = processorFrom(baseScenario)
                .yielding(yieldSignal::get);

        // no yield, process normally
        assertResult(processor, 1);

        yieldSignal.set(true);
        assertYields(processor);

        // processor should progress since it yielded last time
        assertResult(processor, 2);

        // base scenario future blocks
        assertBlocks(processor);
        assertUnblocks(processor, future);

        // yield signal is still set
        assertYields(processor);

        // continue to process normally
        yieldSignal.set(false);
        assertResult(processor, 3);
        assertResult(processor, 4);
        assertFinishes(processor);
    }

    @Test(timeOut = 5000)
    public void testFlatMap()
    {
        List<ProcessState<Integer>> baseScenario = ImmutableList.of(
                ProcessState.ofResult(1),
                ProcessState.ofResult(2),
                ProcessState.finished());

        WorkProcessor<Double> processor = processorFrom(baseScenario)
                .flatMap(element -> WorkProcessor.fromIterable(ImmutableList.of((Double) 2. * element, (Double) 3. * element)));

        assertResult(processor, 2.);
        assertResult(processor, 3.);
        assertResult(processor, 4.);
        assertResult(processor, 6.);
        assertFinishes(processor);
    }

    @Test(timeOut = 5000)
    public void testMap()
    {
        List<ProcessState<Integer>> baseScenario = ImmutableList.of(
                ProcessState.ofResult(1),
                ProcessState.ofResult(2),
                ProcessState.finished());

        WorkProcessor<Double> processor = processorFrom(baseScenario)
                .map(element -> 2. * element);

        assertResult(processor, 2.);
        assertResult(processor, 4.);
        assertFinishes(processor);
    }

    @Test(timeOut = 5000)
    public void testFlatTransform()
    {
        SettableFuture<?> baseFuture = SettableFuture.create();
        List<ProcessState<Double>> baseScenario = ImmutableList.of(
                ProcessState.ofResult(1.0),
                ProcessState.blocked(baseFuture),
                ProcessState.ofResult(2.0),
                ProcessState.yield(),
                ProcessState.ofResult(3.0),
                ProcessState.ofResult(4.0),
                ProcessState.finished());

        SettableFuture<?> mappedFuture1 = SettableFuture.create();
        List<ProcessState<Integer>> mappedScenario1 = ImmutableList.of(
                ProcessState.ofResult(1),
                ProcessState.yield(),
                ProcessState.blocked(mappedFuture1),
                ProcessState.ofResult(2),
                ProcessState.finished());

        List<ProcessState<Integer>> mappedScenario2 = ImmutableList.of(ProcessState.finished());

        SettableFuture<?> mappedFuture3 = SettableFuture.create();
        List<ProcessState<Integer>> mappedScenario3 = ImmutableList.of(
                ProcessState.blocked(mappedFuture3),
                ProcessState.finished());

        List<ProcessState<Integer>> mappedScenario4 = ImmutableList.of(
                ProcessState.ofResult(3),
                ProcessState.finished());

        SettableFuture<?> transformationFuture = SettableFuture.create();
        List<Transform<Double, WorkProcessor<Integer>>> transformationScenario = ImmutableList.of(
                Transform.of(Optional.of(1.0), TransformationState.ofResult(processorFrom(mappedScenario1), false)),
                Transform.of(Optional.of(1.0), TransformationState.ofResult(processorFrom(mappedScenario2), false)),
                Transform.of(Optional.of(1.0), TransformationState.ofResult(processorFrom(mappedScenario3))),
                Transform.of(Optional.of(2.0), TransformationState.blocked(transformationFuture)),
                Transform.of(Optional.of(2.0), TransformationState.ofResult(processorFrom(mappedScenario4))),
                Transform.of(Optional.of(3.0), TransformationState.finished()));

        WorkProcessor<Integer> processor = processorFrom(baseScenario)
                .flatTransform(transformationFrom(transformationScenario));

        // mappedScenario1.result 1
        assertResult(processor, 1);

        // mappedScenario1.yield
        assertYields(processor);

        // mappedScenario1.blocked
        assertBlocks(processor);

        // mappedScenario1 unblocks
        assertUnblocks(processor, mappedFuture1);

        // mappedScenario1 result 2
        assertResult(processor, 2);

        // mappedScenario3.blocked
        assertBlocks(processor);

        // mappedScenario3 unblocks
        assertUnblocks(processor, mappedFuture3);

        // base.blocked
        assertBlocks(processor);

        // base unblocks
        assertUnblocks(processor, baseFuture);

        // transformation.blocked
        assertBlocks(processor);

        // transformation unblocks
        assertUnblocks(processor, transformationFuture);

        // mappedScenario4 result 3
        assertResult(processor, 3);

        // base.yield
        assertYields(processor);

        // transformation finishes
        assertFinishes(processor);
    }

    @Test(timeOut = 5000)
    public void testTransform()
    {
        SettableFuture<?> baseFuture = SettableFuture.create();
        List<ProcessState<Integer>> baseScenario = ImmutableList.of(
                ProcessState.ofResult(1),
                ProcessState.yield(),
                ProcessState.blocked(baseFuture),
                ProcessState.ofResult(2),
                ProcessState.ofResult(3),
                ProcessState.finished());

        SettableFuture<?> transformationFuture = SettableFuture.create();
        List<Transform<Integer, String>> transformationScenario = ImmutableList.of(
                Transform.of(Optional.of(1), TransformationState.needsMoreData()),
                Transform.of(Optional.of(2), TransformationState.ofResult("foo")),
                Transform.of(Optional.of(3), TransformationState.blocked(transformationFuture)),
                Transform.of(Optional.of(3), TransformationState.yield()),
                Transform.of(Optional.of(3), TransformationState.ofResult("bar", false)),
                Transform.of(Optional.of(3), TransformationState.ofResult("zoo", true)),
                Transform.of(Optional.empty(), TransformationState.ofResult("car", false)),
                Transform.of(Optional.empty(), TransformationState.finished()));

        WorkProcessor<String> processor = processorFrom(baseScenario)
                .transform(transformationFrom(transformationScenario));

        // before
        assertFalse(processor.isBlocked());
        assertFalse(processor.isFinished());

        // base.yield
        assertYields(processor);

        // base.blocked
        assertBlocks(processor);

        // base unblock
        assertUnblocks(processor, baseFuture);

        // transformation.result foo
        assertResult(processor, "foo");

        // transformation.blocked
        assertBlocks(processor);

        // transformation.unblock
        assertUnblocks(processor, transformationFuture);

        // transformation.yield
        assertYields(processor);

        // transformation.result bar
        assertResult(processor, "bar");

        // transformation.result zoo
        assertResult(processor, "zoo");

        // transformation.result car
        assertResult(processor, "car");

        // transformation.finished
        assertFinishes(processor);
    }

    @Test(timeOut = 5000)
    public void testCreateFrom()
    {
        SettableFuture<?> future = SettableFuture.create();
        List<ProcessState<Integer>> scenario = ImmutableList.of(
                ProcessState.yield(),
                ProcessState.ofResult(1),
                ProcessState.blocked(future),
                ProcessState.yield(),
                ProcessState.ofResult(2),
                ProcessState.finished());
        WorkProcessor<Integer> processor = processorFrom(scenario);

        // before
        assertFalse(processor.isBlocked());
        assertFalse(processor.isFinished());

        assertYields(processor);
        assertResult(processor, 1);
        assertBlocks(processor);
        assertUnblocks(processor, future);
        assertYields(processor);
        assertResult(processor, 2);
        assertFinishes(processor);
    }

    private static <T, R> WorkProcessor.Transformation<T, R> transformationFrom(List<Transform<T, R>> transformations)
    {
        Iterator<Transform<T, R>> iterator = transformations.iterator();
        return elementOptional -> {
            assertTrue(iterator.hasNext());
            return iterator.next().transform(elementOptional);
        };
    }

    private static <T> WorkProcessor<T> processorFrom(List<ProcessState<T>> states)
    {
        return WorkProcessorUtils.create(processFrom(states));
    }

    private static <T> WorkProcessor.Process<T> processFrom(List<ProcessState<T>> states)
    {
        Iterator<ProcessState<T>> iterator = states.iterator();
        return () -> {
            assertTrue(iterator.hasNext());
            return iterator.next();
        };
    }

    private static class Transform<T, R>
    {
        final Optional<T> from;
        final TransformationState<R> to;

        Transform(Optional<T> from, TransformationState<R> to)
        {
            this.from = requireNonNull(from);
            this.to = requireNonNull(to);
        }

        static <T, R> Transform<T, R> of(Optional<T> from, TransformationState<R> to)
        {
            return new Transform<>(from, to);
        }

        TransformationState<R> transform(Optional<T> from)
        {
            assertEquals(from, this.from);
            return to;
        }
    }
}
