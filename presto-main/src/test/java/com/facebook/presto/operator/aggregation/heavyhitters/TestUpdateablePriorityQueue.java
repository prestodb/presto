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
        assertEquals(populateAndExtract(new IndexedPriorityQueue<>()), ImmutableList.of(2, 3, 5, 1));
    }

    @Test
    public void testIndexedPriorityQueueReverse()
    {
        assertEquals(populateAndExtract(new IndexedPriorityQueue<>(false)), ImmutableList.of(5, 3, 2, 4));
    }


    private static List<Integer> populateAndExtract(IndexedPriorityQueue<Integer> queue)
    {
        queue.addOrUpdate(1, 1);
        queue.addOrUpdate(2, 2);
        queue.addOrUpdate(3, 3);
        queue.addOrUpdate(4, 7);
        queue.addOrUpdate(2, 5);  //Update priority of existing entry
        queue.addOrUpdate(5, 2);  //duplicate priority

        queue.poll();
        Iterator x = queue.iterator();
        //System.out.println(queue.getClass());
        while (x.hasNext())
            System.out.print(x.next() + " ");
        System.out.println();

        return ImmutableList.copyOf(transform(queue.iterator(), Entry::getValue));
    }
}
