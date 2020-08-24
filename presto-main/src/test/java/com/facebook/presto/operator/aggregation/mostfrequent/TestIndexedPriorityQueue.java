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

package com.facebook.presto.operator.aggregation.mostfrequent;

import com.facebook.presto.operator.aggregation.mostfrequent.IndexedPriorityQueue.Entry;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestIndexedPriorityQueue
{
    private static void populate(IndexedPriorityQueue queue)
    {
        queue.addOrUpdate(Slices.utf8Slice("a"), 1);
        queue.addOrUpdate(Slices.utf8Slice("b"), 2);
        queue.addOrUpdate(Slices.utf8Slice("c"), 3);
        queue.addOrUpdate(Slices.utf8Slice("d"), 2);
        queue.addOrUpdate(Slices.utf8Slice("e"), 5);
        queue.addOrUpdate(Slices.utf8Slice("d"), 4);  //Update priority of existing entry
        queue.addOrUpdate(Slices.utf8Slice("i"), 5); //duplicate priority
    }

    private static Map<String, Long> extract(IndexedPriorityQueue queue)
    {
        Iterator<Entry> entries = queue.iterator();
        Map<String, Long> result = new HashMap<>();
        while (entries.hasNext()) {
            Entry entry = entries.next();
            result.put(entry.getValue().toStringUtf8(), entry.getPriority());
        }
        return result;
    }

    @Test
    public void testIndexedPriorityQueue()
    {
        IndexedPriorityQueue testQ = new IndexedPriorityQueue();
        populate(testQ);
        testQ.poll();
        Iterator x = testQ.iterator();
        assertEquals(extract(testQ), ImmutableMap.of("b", 2L, "c", 3L, "d", 4L, "i", 5L, "e", 5L));
    }

    @Test
    public void testRemoveBelowPriority()
    {
        IndexedPriorityQueue testQ = new IndexedPriorityQueue();
        populate(testQ);
        testQ.removeBelowPriority(4);
        Iterator x = testQ.iterator();
        assertEquals(extract(testQ), ImmutableMap.of("d", 4L, "i", 5L, "e", 5L));
    }

    @Test
    public void testSerializeQueue()
    {
        IndexedPriorityQueue testQ = new IndexedPriorityQueue();
        populate(testQ);
        IndexedPriorityQueue revivedQ = new IndexedPriorityQueue(testQ.serialize());
        assertEquals(testQ.poll().getValue().toStringUtf8(), revivedQ.poll().getValue().toStringUtf8());
        assertEquals(testQ.poll().getValue().toStringUtf8(), revivedQ.poll().getValue().toStringUtf8());
        assertEquals(testQ.poll().getValue().toStringUtf8(), revivedQ.poll().getValue().toStringUtf8());
    }
}
