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
package com.facebook.presto.common.array;

import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.testng.Assert.assertEquals;

public class TestLong2IntOpenHashMap
{
    private final Random random = new Random();

    @Test
    public void testAddAndGet()
    {
        // Test addTo() and get() without rehashing
        testAddAndGet(10);

        // Test addTo() and get() with rehashing
        testAddAndGet(1000);
    }

    @Test
    public void testRemove()
    {
        // Test remove() and get() without rehashing
        testRemove(10);

        // Test remove() and get() with rehashing
        testRemove(1000);
    }

    @Test
    public void testNull()
    {
        // Test adding nulls and removing nulls, without rehashing
        testNull(10);

        // Test adding nulls and removing nulls, with rehashing
        testNull(1000);
    }

    private void testAddAndGet(int entries)
    {
        Map<Long, Integer> map = new HashMap<Long, Integer>();
        Long2IntOpenHashMap long2IntOpenHashMap = new Long2IntOpenHashMap();

        assertEquals(long2IntOpenHashMap.size(), 0);

        int defaultReturnValue = long2IntOpenHashMap.getDefaultReturnValue();
        for (int i = 0; i < entries; i++) {
            long key = random.nextInt(1 << 10);
            int value = random.nextInt(1 << 10);

            Integer oldValue = map.get(key);
            map.put(key, (oldValue == null ? defaultReturnValue : oldValue) + value);
            long2IntOpenHashMap.addTo(key, value);

            assertEquals(long2IntOpenHashMap.get(key), map.get(key).intValue());

            if (key > 0) {
                // Negative keys should be non-existing in long2IntOpenHashMap
                assertEquals(long2IntOpenHashMap.get(-key), defaultReturnValue);
            }
        }

        assertEquals(map.size(), long2IntOpenHashMap.size());

        map.forEach((key, value) -> {
            assertEquals(long2IntOpenHashMap.get(key), (int) value);
        });
    }

    private void testRemove(int entries)
    {
        Map<Long, Integer> map = new HashMap<>();
        Long2IntOpenHashMap long2IntOpenHashMap = new Long2IntOpenHashMap();
        long2IntOpenHashMap.setDefaultReturnValue(-1);

        assertEquals(long2IntOpenHashMap.size(), 0);

        int defaultReturnValue = long2IntOpenHashMap.getDefaultReturnValue();
        for (int i = 0; i < entries; i++) {
            long key = random.nextInt(1 << 10);
            int value = random.nextInt(1 << 10);

            Integer oldValue = map.get(key);
            map.put(key, (oldValue == null ? defaultReturnValue : oldValue) + value);
            long2IntOpenHashMap.addTo(key, value);
        }

        assertEquals(long2IntOpenHashMap.size(), map.size());

        map.forEach((key, value) -> {
            if (key > 0) {
                // Removing non-existing key should return default value
                assertEquals(long2IntOpenHashMap.remove(-key), defaultReturnValue);
            }
            assertEquals(long2IntOpenHashMap.remove(key), (int) value);
        });
        assertEquals(long2IntOpenHashMap.size(), 0);
    }

    private void testNull(int entries)
    {
        Long2IntOpenHashMap long2IntOpenHashMap = new Long2IntOpenHashMap();

        assertEquals(long2IntOpenHashMap.containsNullKey, false);

        for (int i = 0; i < entries; i++) {
            long2IntOpenHashMap.addTo(0, 1);

            assertEquals(long2IntOpenHashMap.containsNullKey, true);
            assertEquals(long2IntOpenHashMap.size(), 1);
            assertEquals(long2IntOpenHashMap.get(0), i + 1);
        }

        int nullCount = long2IntOpenHashMap.remove(0);

        assertEquals(nullCount, entries);
        assertEquals(long2IntOpenHashMap.containsNullKey, false);
        assertEquals(long2IntOpenHashMap.size(), 0);
        assertEquals(long2IntOpenHashMap.get(0), long2IntOpenHashMap.getDefaultReturnValue());
    }
}
