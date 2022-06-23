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
package com.facebook.presto.orc.writer;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestLongDictionaryBuilder
{
    private static class LongDictionaryCollisionBuilder
            extends LongDictionaryBuilder
    {
        LongDictionaryCollisionBuilder()
        {
            super(10_000);
        }

        @Override
        public long getHash(long value)
        {
            return 0;
        }
    }

    @Test
    public void testHashCollision()
    {
        LongDictionaryCollisionBuilder builder = new LongDictionaryCollisionBuilder();

        assertEquals(builder.putIfAbsent(100), 0);
        assertEquals(builder.putIfAbsent(200), 1);

        //re-inserting the same value preserves the index
        assertEquals(builder.putIfAbsent(100), 0);
        assertEquals(builder.putIfAbsent(200), 1);

        assertEquals(builder.size(), 2);
    }

    @Test
    public void testBuilder()
    {
        LongDictionaryBuilder builder = new LongDictionaryBuilder(1_000);
        int startElement = -50_000;
        int totalElements = 100_000;
        for (int attempt = 0; attempt < 3; attempt++) {
            for (int i = 0; i < totalElements; i++) {
                long value = startElement + i;
                for (int j = 0; j < 3; j++) {
                    assertEquals(builder.putIfAbsent(value), i);
                }
                assertEquals(builder.size(), i + 1);
                assertEquals(builder.getValue(i), value);
            }

            long[] elements = builder.elements();
            for (int i = 0; i < totalElements; i++) {
                assertEquals(elements[i], startElement + i);
            }

            long minExpectedSize = totalElements * (Long.BYTES + Integer.BYTES);
            assertTrue(builder.getRetainedBytes() > minExpectedSize, "retainedBytes " + builder.getRetainedBytes());
            builder.clear();
            assertEquals(builder.size(), 0);
        }
    }

    @Test
    public void testRandomLongs()
    {
        LongDictionaryBuilder builder = new LongDictionaryBuilder(1_000);
        // Dividing by 10 million milliseconds, so the tests will have the same seed
        // for about 10,000 seconds (or 2.5 hours) to make failure investigations easier.
        long seed = System.currentTimeMillis() / 10_000_000;
        Random random = new Random(seed);
        Set<Long> uniqueValues = new HashSet<>();

        // generate unique random longs
        for (int i = 0; i < 100_000; i++) {
            uniqueValues.add(random.nextLong());
        }
        List<Long> values = new ArrayList<>(uniqueValues);

        // Insert into dictionary to seed them.
        for (int i = 0; i < values.size(); i++) {
            assertEquals(builder.putIfAbsent(values.get(i)), i);
        }

        // Now re-insert them randomly, verify the index are the same.
        for (int i = 0; i < 500_000; i++) {
            int nextIndex = random.nextInt(values.size());
            assertEquals(builder.putIfAbsent(values.get(nextIndex)), nextIndex);
        }

        // Finally verify the size.
        assertEquals(builder.size(), values.size());
    }
}
