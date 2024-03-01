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
package com.facebook.presto.execution.resourceGroups;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestUpdateablePriorityQueue
{
    @Test
    public void testFifoQueue()
    {
        assertEquals(populateAndExtract(new FifoQueue<>()), ImmutableList.of(1, 2, 3));
    }

    @Test
    public void testIndexedPriorityQueue()
    {
        assertEquals(populateAndExtract(new IndexedPriorityQueue<>()), ImmutableList.of(3, 2, 1));
    }

    @Test
    public void testStochasticPriorityQueue()
    {
        assertTrue(populateAndExtract(new StochasticPriorityQueue<>()).size() == 3);
    }

    @Test
    public void testTieredQueue()
    {
        TieredQueue<Integer> queue = new TieredQueue<>(FifoQueue::new);
        assertEquals(populateAndExtract(queue), ImmutableList.of(1, 2, 3));

        queue.prioritize(4, 0);
        queue.prioritize(5, 0);
        assertEquals(populateAndExtract(queue), ImmutableList.of(4, 5, 1, 2, 3));
    }

    private static List<Integer> populateAndExtract(UpdateablePriorityQueue<Integer> queue)
    {
        queue.addOrUpdate(1, 1);
        queue.addOrUpdate(2, 2);
        queue.addOrUpdate(3, 3);
        return ImmutableList.copyOf(queue);
    }

    @Test
    public void testIndexedPriorityQueueIterator()
    {
        IndexedPriorityQueue<Integer> queue = new IndexedPriorityQueue<>();
        queue.addOrUpdate(1, 1);
        queue.addOrUpdate(2, 2);
        queue.addOrUpdate(3, 3);

        Iterator<Integer> iterator = queue.iterator();
        assertTrue(iterator.hasNext());
        assertThrows(IllegalStateException.class, iterator::remove);

        assertEquals(ImmutableList.of(3, 2, 1), ImmutableList.copyOf(queue));
        assertTrue(iterator.hasNext());
        assertEquals(queue.peek(), Integer.valueOf(3));
        assertEquals(iterator.next(), Integer.valueOf(3));
        assertTrue(queue.contains(3));
        iterator.remove();
        assertFalse(queue.remove(3));
        assertFalse(queue.contains(3));
        assertThrows(IllegalStateException.class, iterator::remove);

        assertEquals(ImmutableList.of(2, 1), ImmutableList.copyOf(queue));
        assertTrue(iterator.hasNext());
        assertEquals(queue.peek(), Integer.valueOf(2));
        assertEquals(iterator.next(), Integer.valueOf(2));
        assertTrue(queue.contains(2));
        iterator.remove();
        assertFalse(queue.remove(2));
        assertFalse(queue.contains(2));
        assertThrows(IllegalStateException.class, iterator::remove);

        assertEquals(ImmutableList.of(1), ImmutableList.copyOf(queue));
        assertTrue(iterator.hasNext());
        assertEquals(queue.peek(), Integer.valueOf(1));
        assertEquals(iterator.next(), Integer.valueOf(1));
        assertTrue(queue.contains(1));
        iterator.remove();
        assertFalse(queue.remove(1));
        assertFalse(queue.contains(1));
        assertThrows(IllegalStateException.class, iterator::remove);

        assertEquals(ImmutableList.of(), ImmutableList.copyOf(queue));
        assertFalse(iterator.hasNext());
        assertThrows(IllegalStateException.class, iterator::remove);
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    public void testConcurrentModificationOnIterator()
    {
        IndexedPriorityQueue<Integer> queue = new IndexedPriorityQueue<>();
        queue.addOrUpdate(1, 1);
        queue.addOrUpdate(2, 2);
        queue.addOrUpdate(3, 3);

        Iterator<Integer> iterator = queue.iterator();
        assertTrue(iterator.hasNext());
        assertThrows(IllegalStateException.class, iterator::remove);

        assertTrue(iterator.hasNext());
        assertEquals(iterator.next(), Integer.valueOf(3));

        assertTrue(queue.remove(3));
        assertThrows(ConcurrentModificationException.class, iterator::remove);
        assertFalse(queue.remove(3));
        assertThrows(ConcurrentModificationException.class, iterator::hasNext);

        assertEquals(ImmutableList.of(2, 1), ImmutableList.copyOf(queue));
        iterator = queue.iterator();
        assertEquals(queue.poll(), Integer.valueOf(2));
        assertThrows(ConcurrentModificationException.class, iterator::next);

        iterator = queue.iterator();
        assertEquals(iterator.next(), Integer.valueOf(1));
        iterator.remove();
        assertNull(queue.poll());
    }
}
