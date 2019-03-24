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
package com.facebook.presto.operator.aggregation.heavyhitters;

import com.google.common.collect.ImmutableList;
import static com.google.common.collect.Iterators.transform;
import com.facebook.presto.operator.aggregation.heavyhitters.IndexedPriorityQueue.Entry;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestUpdateablePriorityQueue
{

    @Test
    public void testIndexedPriorityQueue()
    {
        IndexedPriorityQueue<Integer> testQ = new IndexedPriorityQueue<>();
        populate(testQ);
        assertEquals(extract(testQ), ImmutableList.of(5,9,4,3,2,1));
    }

    @Test
    public void testIndexedPriorityQueueReverse()
    {
        IndexedPriorityQueue<Integer> testQ = new IndexedPriorityQueue<>(false);
        populate(testQ);
        testQ.poll();
        Iterator x = testQ.iterator();
        while (x.hasNext())
            System.out.print(x.next() + " ");
        System.out.println();
        assertEquals(extract(testQ), ImmutableList.of(2,3,4,5,9));
    }

    @Test
    public void testRemoveBelowPriority()
    {
        IndexedPriorityQueue<Integer> testQ = new IndexedPriorityQueue<>(false);
        populate(testQ);
        testQ.removeBelowPriority(4);
        Iterator x = testQ.iterator();
        while (x.hasNext())
            System.out.print(x.next() + " ");
        System.out.println();
        assertEquals(extract(testQ), ImmutableList.of(4,5,9));
    }

    private static void populate(IndexedPriorityQueue<Integer> queue)
    {
        queue.addOrUpdate(1, 1);
        queue.addOrUpdate(2, 2);
        queue.addOrUpdate(3, 3);
        queue.addOrUpdate(4, 2);
        queue.addOrUpdate(5, 5);
        queue.addOrUpdate(4, 4);  //Update priority of existing entry
        queue.addOrUpdate(9, 5); //duplicate priority
    }

    private static List<Integer> extract(IndexedPriorityQueue<Integer> queue)
    {
        return ImmutableList.copyOf(transform(queue.iterator(), Entry::getValue));
    }

}
