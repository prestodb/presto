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

import java.util.List;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
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

    private static List<Integer> populateAndExtract(UpdateablePriorityQueue<Integer> queue)
    {
        queue.addOrUpdate(1, 1);
        queue.addOrUpdate(2, 2);
        queue.addOrUpdate(3, 3);
        return ImmutableList.copyOf(queue);
    }
}
