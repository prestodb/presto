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

import com.facebook.presto.operator.ContinuousWorkUtils.WorkState;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.SettableFuture;
import org.testng.annotations.Test;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestContinuousWorkUtils
{
    @Test(timeOut = 5000)
    public void testMergeSorted()
    {
        List<WorkState<Integer>> firstStream = ImmutableList.of(
                WorkState.ofResult(1),
                WorkState.ofResult(3),
                WorkState.yield(),
                WorkState.ofResult(5),
                WorkState.finished());

        SettableFuture<?> secondFuture = SettableFuture.create();
        List<WorkState<Integer>> secondStream = ImmutableList.of(
                WorkState.ofResult(2),
                WorkState.ofResult(4),
                WorkState.blocked(secondFuture),
                WorkState.finished());

        ContinuousWork<Integer> mergedStream = ContinuousWorkUtils.mergeSorted(
                ImmutableList.of(workFrom(firstStream), workFrom(secondStream)),
                Comparator.comparingInt(firstInteger -> firstInteger));

        // first stream result 1
        assertTrue(mergedStream.process());
        assertFalse(mergedStream.isBlocked());
        assertFalse(mergedStream.isFinished());
        assertEquals(mergedStream.getResult(), (Integer) 1);

        // second stream result 2
        assertTrue(mergedStream.process());
        assertFalse(mergedStream.isBlocked());
        assertFalse(mergedStream.isFinished());
        assertEquals(mergedStream.getResult(), (Integer) 2);

        // first stream result 3
        assertTrue(mergedStream.process());
        assertFalse(mergedStream.isBlocked());
        assertFalse(mergedStream.isFinished());
        assertEquals(mergedStream.getResult(), (Integer) 3);

        // first stream yield
        assertFalse(mergedStream.process());
        assertFalse(mergedStream.isBlocked());
        assertFalse(mergedStream.isFinished());

        // second stream result 4
        assertTrue(mergedStream.process());
        assertFalse(mergedStream.isBlocked());
        assertFalse(mergedStream.isFinished());
        assertEquals(mergedStream.getResult(), (Integer) 4);

        // second stream blocked
        assertFalse(mergedStream.process());
        assertTrue(mergedStream.isBlocked());
        assertFalse(mergedStream.isFinished());

        // second stream unblock
        secondFuture.set(null);
        assertFalse(mergedStream.isBlocked());
        assertFalse(mergedStream.isFinished());

        // first stream result 5
        assertTrue(mergedStream.process());
        assertFalse(mergedStream.isBlocked());
        assertFalse(mergedStream.isFinished());
        assertEquals(mergedStream.getResult(), (Integer) 5);

        // both streams finished
        assertTrue(mergedStream.process());
        assertFalse(mergedStream.isBlocked());
        assertTrue(mergedStream.isFinished());
    }

    @Test(timeOut = 5000)
    public void testMergeSortedEmptyStreams()
    {
        SettableFuture<?> firstFuture = SettableFuture.create();
        List<WorkState<Integer>> firstStream = ImmutableList.of(
                WorkState.blocked(firstFuture),
                WorkState.yield(),
                WorkState.finished());

        SettableFuture<?> secondFuture = SettableFuture.create();
        List<WorkState<Integer>> secondStream = ImmutableList.of(
                WorkState.blocked(secondFuture),
                WorkState.finished());

        ContinuousWork<Integer> mergedStream = ContinuousWorkUtils.mergeSorted(
                ImmutableList.of(workFrom(firstStream), workFrom(secondStream)),
                Comparator.comparingInt(firstInteger -> firstInteger));

        assertFalse(mergedStream.isBlocked());
        assertFalse(mergedStream.isFinished());

        // first stream blocked
        assertFalse(mergedStream.process());
        assertTrue(mergedStream.isBlocked());
        assertFalse(mergedStream.isFinished());

        // first stream unblock
        firstFuture.set(null);
        assertFalse(mergedStream.isBlocked());
        assertFalse(mergedStream.isFinished());

        // first stream yield
        assertFalse(mergedStream.process());
        assertFalse(mergedStream.isBlocked());
        assertFalse(mergedStream.isFinished());

        // second stream blocked
        assertFalse(mergedStream.process());
        assertTrue(mergedStream.isBlocked());
        assertFalse(mergedStream.isFinished());

        // first stream unblock
        secondFuture.set(null);
        assertFalse(mergedStream.isBlocked());
        assertFalse(mergedStream.isFinished());

        // both streams finished
        assertTrue(mergedStream.process());
        assertFalse(mergedStream.isBlocked());
        assertTrue(mergedStream.isFinished());
    }

    @Test(timeOut = 5000)
    public void testMergeSortedEmptyStreamsWithFinishedOnly()
    {
        List<WorkState<Integer>> firstStream = ImmutableList.of(
                WorkState.finished());

        List<WorkState<Integer>> secondStream = ImmutableList.of(
                WorkState.finished());

        ContinuousWork<Integer> mergedStream = ContinuousWorkUtils.mergeSorted(
                ImmutableList.of(workFrom(firstStream), workFrom(secondStream)),
                Comparator.comparingInt(firstInteger -> firstInteger));

        assertFalse(mergedStream.isBlocked());
        assertFalse(mergedStream.isFinished());

        assertTrue(mergedStream.process());
        assertFalse(mergedStream.isBlocked());
        assertTrue(mergedStream.isFinished());
    }

    @Test(timeOut = 5000)
    public void testFlatMap()
    {
        List<WorkState<Integer>> baseScenario = ImmutableList.of(
                WorkState.ofResult(1),
                WorkState.ofResult(2),
                WorkState.finished());

        ContinuousWork<Double> work = ContinuousWorkUtils.flatMap(
                workFrom(baseScenario),
                element -> Iterators.forArray((Double) 2. * element, (Double) 3. * element));

        assertTrue(work.process());
        assertEquals(work.getResult(), 2.);

        assertTrue(work.process());
        assertEquals(work.getResult(), 3.);

        assertTrue(work.process());
        assertEquals(work.getResult(), 4.);

        assertTrue(work.process());
        assertEquals(work.getResult(), 6.);

        assertTrue(work.process());
        assertTrue(work.isFinished());
    }

    @Test(timeOut = 5000)
    public void testMap()
    {
        List<WorkState<Integer>> baseScenario = ImmutableList.of(
                WorkState.ofResult(1),
                WorkState.ofResult(2),
                WorkState.finished());

        ContinuousWork<Double> work = ContinuousWorkUtils.map(
                workFrom(baseScenario),
                element -> 2. * element);

        assertTrue(work.process());
        assertEquals(work.getResult(), 2.);

        assertTrue(work.process());
        assertEquals(work.getResult(), 4.);

        assertTrue(work.process());
        assertTrue(work.isFinished());
    }

    @Test(timeOut = 5000)
    public void testTransform()
    {
        SettableFuture<?> baseFuture = SettableFuture.create();
        List<WorkState<Integer>> baseScenario = ImmutableList.of(
                WorkState.ofResult(1),
                WorkState.yield(),
                WorkState.blocked(baseFuture),
                WorkState.ofResult(2),
                WorkState.ofResult(3),
                WorkState.finished());

        SettableFuture<?> composeFuture = SettableFuture.create();
        List<Transform<Integer, String>> composeScenario = ImmutableList.of(
                Transform.of(Optional.of(1), WorkState.needsMoreData()),
                Transform.of(Optional.of(2), WorkState.ofResult("foo")),
                Transform.of(Optional.of(3), WorkState.blocked(composeFuture)),
                Transform.of(Optional.of(3), WorkState.yield()),
                Transform.of(Optional.of(3), WorkState.ofResult("bar", false)),
                Transform.of(Optional.of(3), WorkState.ofResult("zoo", true)),
                Transform.of(Optional.empty(), WorkState.ofResult("car", false)),
                Transform.of(Optional.empty(), WorkState.finished()));

        ContinuousWork<String> work = ContinuousWorkUtils.transform(
                workFrom(baseScenario),
                transformationFrom(composeScenario));

        // before
        assertFalse(work.isBlocked());
        assertFalse(work.isFinished());

        // base.yield
        assertFalse(work.process());
        assertFalse(work.isBlocked());
        assertFalse(work.isFinished());

        // base.blocked
        assertFalse(work.process());
        assertTrue(work.isBlocked());
        assertFalse(work.isFinished());

        // base unblock
        baseFuture.set(null);
        assertFalse(work.isBlocked());
        assertFalse(work.isFinished());

        // compose.result foo
        assertTrue(work.process());
        assertFalse(work.isBlocked());
        assertFalse(work.isFinished());
        assertEquals(work.getResult(), "foo");

        // compose.blocked
        assertFalse(work.process());
        assertTrue(work.isBlocked());
        assertFalse(work.isFinished());

        // compose.unblock
        composeFuture.set(null);
        assertFalse(work.isBlocked());
        assertFalse(work.isFinished());

        // compose.yield
        assertFalse(work.process());
        assertFalse(work.isBlocked());
        assertFalse(work.isFinished());

        // compose.result bar
        assertTrue(work.process());
        assertFalse(work.isBlocked());
        assertFalse(work.isFinished());
        assertEquals(work.getResult(), "bar");

        // compose.result zoo
        assertTrue(work.process());
        assertFalse(work.isBlocked());
        assertFalse(work.isFinished());
        assertEquals(work.getResult(), "zoo");

        // compose.result car
        assertTrue(work.process());
        assertFalse(work.isBlocked());
        assertFalse(work.isFinished());
        assertEquals(work.getResult(), "car");

        // compose.finished
        assertTrue(work.process());
        assertFalse(work.isBlocked());
        assertTrue(work.isFinished());
    }

    @Test(timeOut = 5000)
    public void testCreateFrom()
    {
        SettableFuture<?> future = SettableFuture.create();
        List<WorkState<Integer>> scenario = ImmutableList.of(
                WorkState.yield(),
                WorkState.ofResult(1),
                WorkState.blocked(future),
                WorkState.yield(),
                WorkState.ofResult(2),
                WorkState.finished());
        ContinuousWork<Integer> work = workFrom(scenario);

        // before
        assertFalse(work.isBlocked());
        assertFalse(work.isFinished());

        // yield
        assertFalse(work.process());
        assertFalse(work.isBlocked());
        assertFalse(work.isFinished());

        // result 1
        assertTrue(work.process());
        assertFalse(work.isBlocked());
        assertFalse(work.isFinished());
        assertEquals(work.getResult(), (Integer) 1);

        // blocked
        assertFalse(work.process());
        assertTrue(work.isBlocked());
        assertFalse(work.isFinished());
        assertFalse(work.process());

        // unblock
        future.set(null);
        assertFalse(work.isBlocked());

        // yield
        assertFalse(work.process());
        assertFalse(work.isBlocked());
        assertFalse(work.isFinished());

        // result 2
        assertTrue(work.process());
        assertFalse(work.isBlocked());
        assertFalse(work.isFinished());
        assertEquals(work.getResult(), (Integer) 2);

        // finished
        assertTrue(work.process());
        assertFalse(work.isBlocked());
        assertTrue(work.isFinished());
        assertTrue(work.process());
    }

    private static <T, R> Function<Optional<T>, WorkState<R>> transformationFrom(List<Transform<T, R>> transformations)
    {
        Iterator<Transform<T, R>> iterator = transformations.iterator();
        return elementOptional -> {
            assertTrue(iterator.hasNext());
            return iterator.next().transform(elementOptional);
        };
    }

    private static <T> ContinuousWork<T> workFrom(List<WorkState<T>> states)
    {
        return ContinuousWorkUtils.create(supplierFrom(states));
    }

    private static <T> Supplier<WorkState<T>> supplierFrom(List<WorkState<T>> states)
    {
        Iterator<WorkState<T>> iterator = states.iterator();
        return () -> {
            assertTrue(iterator.hasNext());
            return iterator.next();
        };
    }

    private static class Transform<T, R>
    {
        final Optional<T> from;
        final WorkState<R> to;

        Transform(Optional<T> from, WorkState<R> to)
        {
            this.from = requireNonNull(from);
            this.to = requireNonNull(to);
        }

        static <T, R> Transform<T, R> of(Optional<T> from, WorkState<R> to)
        {
            return new Transform<>(from, to);
        }

        WorkState<R> transform(Optional<T> from)
        {
            assertEquals(from, this.from);
            return to;
        }
    }
}
