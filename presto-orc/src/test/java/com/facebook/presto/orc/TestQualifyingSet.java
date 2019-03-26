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
package com.facebook.presto.orc;

import org.testng.annotations.Test;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestQualifyingSet
{
    @Test
    public void testInsert()
    {
        QualifyingSet qualifyingSet = new QualifyingSet();
        qualifyingSet.ensureCapacity(10);
        for (int i = 1; i < 10; i += 2) {
            qualifyingSet.append(i, i);
        }

        // insert no nulls
        qualifyingSet.insert(new int[0], new int[0], 0);
        assertEquals(qualifyingSet.getPositionCount(), 5);
        assertArrayPrefix(qualifyingSet.getPositions(), new int[] {1, 3, 5, 7, 9});

        // insert nulls in the middle
        qualifyingSet.insert(new int[] {2, 6}, new int[] {2, 6}, 2);
        assertEquals(qualifyingSet.getPositionCount(), 7);
        assertArrayPrefix(qualifyingSet.getPositions(), new int[] {1, 2, 3, 5, 6, 7, 9});

        // insert nulls in the middle and at the end
        qualifyingSet.insert(new int[] {8, 11, 12, 15, 16}, new int[] {8, 11, 12, 15, 16}, 4);
        assertEquals(qualifyingSet.getPositionCount(), 11);
        assertArrayPrefix(qualifyingSet.getPositions(), new int[] {1, 2, 3, 5, 6, 7, 8, 9, 11, 12, 15});

        // insert null in the beginning, middle and end
        qualifyingSet.insert(new int[] {0, 10, 22, 34, 35}, new int[] {0, 10, 22, 34}, 4);
        assertEquals(qualifyingSet.getPositionCount(), 15);
        assertArrayPrefix(qualifyingSet.getPositions(), new int[] {0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 15, 22, 34});
    }

    private void assertArrayPrefix(int[] actual, int[] expectedPrefix)
    {
        assertTrue(actual.length >= expectedPrefix.length);
        for (int i = 0; i < expectedPrefix.length; i++) {
            assertEquals(actual[i], expectedPrefix[i]);
        }
    }
}
