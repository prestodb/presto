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

import com.facebook.presto.execution.resourceGroups.WeightedFairQueue.Usage;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestWeightedFairQueue
{
    @Test
    public void testBasic()
    {
        WeightedFairQueue<String> queue = new WeightedFairQueue<>();
        String item1 = "1";
        String item2 = "2";
        queue.addOrUpdate(item1, new Usage(1, 1));
        queue.addOrUpdate(item2, new Usage(2, 1));

        assertEquals(queue.size(), 2);
        assertEquals(queue.poll(), item2);
        assertTrue(queue.contains(item1));
        assertEquals(queue.poll(), item1);
        assertEquals(queue.size(), 0);
        assertEquals(queue.poll(), null);
        assertEquals(queue.poll(), null);
        assertEquals(queue.size(), 0);
    }

    @Test
    public void testUpdate()
    {
        WeightedFairQueue<String> queue = new WeightedFairQueue<>();
        String item1 = "1";
        String item2 = "2";
        String item3 = "3";
        queue.addOrUpdate(item1, new Usage(1, 1));
        queue.addOrUpdate(item2, new Usage(2, 1));
        queue.addOrUpdate(item3, new Usage(3, 1));

        assertEquals(queue.poll(), item3);
        queue.addOrUpdate(item1, new Usage(4, 1));
        assertEquals(queue.poll(), item1);
        assertEquals(queue.poll(), item2);
        assertEquals(queue.size(), 0);
    }

    @Test
    public void testMultipleWinners()
    {
        WeightedFairQueue<String> queue = new WeightedFairQueue<>();
        String item1 = "1";
        String item2 = "2";
        queue.addOrUpdate(item1, new Usage(2, 0));
        queue.addOrUpdate(item2, new Usage(1, 0));

        int count1 = 0;
        int count2 = 0;
        for (int i = 0; i < 1000; i++) {
            if (queue.poll().equals(item1)) {
                queue.addOrUpdate(item1, new Usage(2, 0));
                count1++;
            }
            else {
                queue.addOrUpdate(item2, new Usage(1, 0));
                count2++;
            }
        }

        assertEquals(count1, 500);
        assertEquals(count2, 500);
    }
}
