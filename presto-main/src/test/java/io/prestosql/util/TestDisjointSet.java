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
package io.prestosql.util;

import com.google.common.collect.Iterables;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestDisjointSet
{
    @Test
    public void testInitial()
    {
        DisjointSet<Integer> disjoint = new DisjointSet<>();

        // assert that every node is considered its own group
        for (int i = 0; i < 100; i++) {
            assertEquals(disjoint.find(i).intValue(), i);
        }
        assertEquals(disjoint.getEquivalentClasses().size(), 100);
    }

    @Test
    public void testMergeAllSequentially()
    {
        DisjointSet<Integer> disjoint = new DisjointSet<>();

        // insert pair (i, i+1); assert all inserts are considered new
        for (int i = 0; i < 100; i++) {
            assertTrue(disjoint.findAndUnion(i, i + 1));
            if (i != 0) {
                assertEquals(disjoint.find(i - 1), disjoint.find(i));
            }
            if (i != 99) {
                assertNotEquals(disjoint.find(i + 1), disjoint.find(i + 2));
            }
        }
        // assert every pair (i, j) is in the same set
        for (int i = 0; i <= 100; i++) {
            for (int j = 0; j <= 100; j++) {
                assertEquals(disjoint.find(i), disjoint.find(j));
                assertFalse(disjoint.findAndUnion(i, j));
            }
        }
        Collection<Set<Integer>> equivalentClasses = disjoint.getEquivalentClasses();
        assertEquals(equivalentClasses.size(), 1);
        assertEquals(Iterables.getOnlyElement(equivalentClasses).size(), 101);
    }

    @Test
    public void testMergeAllBackwardsSequentially()
    {
        DisjointSet<Integer> disjoint = new DisjointSet<>();

        // insert pair (i, i+1); assert all inserts are considered new
        for (int i = 100; i > 0; i--) {
            assertTrue(disjoint.findAndUnion(i, i - 1));
            if (i != 100) {
                assertEquals(disjoint.find(i + 1), disjoint.find(i));
            }
            if (i != 1) {
                assertNotEquals(disjoint.find(i - 1), disjoint.find(i - 2));
            }
        }
        // assert every pair (i, j) is in the same set
        for (int i = 0; i <= 100; i++) {
            for (int j = 0; j <= 100; j++) {
                assertEquals(disjoint.find(i), disjoint.find(j));
                assertFalse(disjoint.findAndUnion(i, j));
            }
        }
        Collection<Set<Integer>> equivalentClasses = disjoint.getEquivalentClasses();
        assertEquals(equivalentClasses.size(), 1);
        assertEquals(Iterables.getOnlyElement(equivalentClasses).size(), 101);
    }

    @Test
    public void testMergeFourGroups()
    {
        DisjointSet<Integer> disjoint = new DisjointSet<>();

        // insert pair (i, i+1); assert all inserts are considered new
        List<Integer> inputs = IntStream.range(0, 96).boxed().collect(Collectors.toList());
        Collections.shuffle(inputs);
        for (int i : inputs) {
            assertTrue(disjoint.findAndUnion(i, i + 4));
        }
        // assert every pair (i, j) is in the same set
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 100; j++) {
                if ((i - j) % 4 == 0) {
                    assertEquals(disjoint.find(i), disjoint.find(j));
                    assertFalse(disjoint.findAndUnion(i, j));
                }
                else {
                    assertNotEquals(disjoint.find(i), disjoint.find(j));
                }
            }
        }
        Collection<Set<Integer>> equivalentClasses = disjoint.getEquivalentClasses();
        assertEquals(equivalentClasses.size(), 4);
        equivalentClasses.stream()
                .forEach(equivalentClass -> assertEquals(equivalentClass.size(), 25));
    }

    @Test
    public void testMergeRandomly()
    {
        DisjointSet<Double> disjoint = new DisjointSet<>();

        Random rand = new Random();
        // Don't use List here. That will box the primitive early.
        // Boxing need to happen right before calls to DisjointSet to test that correct equality check is used for comparisons.
        double[] numbers = new double[100];
        for (int i = 0; i < 100; i++) {
            numbers[i] = rand.nextDouble();
            disjoint.find(numbers[i]);
        }
        int groupCount = 100;
        while (groupCount > 1) {
            // this loop roughly runs 180-400 times
            boolean newEquivalence = disjoint.findAndUnion(numbers[rand.nextInt(100)], numbers[rand.nextInt(100)]);
            if (newEquivalence) {
                groupCount--;
            }
            assertEquals(disjoint.getEquivalentClasses().size(), groupCount);
        }
    }
}
