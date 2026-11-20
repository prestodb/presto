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
package com.facebook.presto.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

import static com.facebook.presto.execution.scheduler.DeterministicBoundedAssignment.assign;
import static com.facebook.presto.execution.scheduler.DeterministicBoundedAssignment.capacity;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestDeterministicBoundedAssignment
{
    @Test
    public void testSmallScanBounds()
    {
        for (int count : new int[] {1, 7, 8, 15, 60, 64, 1000}) {
            Map<String, Long> weights = keys(count);
            Map<String, String> owners = assign(weights, workers(8));
            assertBound(weights, owners, 8);
            assertTrue(loads(weights, owners).values().stream().allMatch(load -> load <= (count + 7) / 8));
        }
    }

    @Test
    public void testEnumerationAndWorkerOrderDoNotAffectOwners()
    {
        Map<String, Long> weights = keys(60);
        Map<String, String> expected = assign(weights, workers(8));
        Random random = new Random(13);
        for (int iteration = 0; iteration < 50; iteration++) {
            List<String> keys = new ArrayList<>(weights.keySet());
            List<String> nodes = workers(8);
            Collections.shuffle(keys, random);
            Collections.shuffle(nodes, random);
            Map<String, Long> shuffled = new LinkedHashMap<>();
            keys.forEach(key -> shuffled.put(key, weights.get(key)));
            assertEquals(assign(shuffled, nodes), expected);
            assertEquals(DeterministicBoundedAssignment.fingerprint(assign(shuffled, nodes)), DeterministicBoundedAssignment.fingerprint(expected));
        }
    }

    @Test
    public void testWeightedKeysIncludingLargeIndivisibleFiles()
    {
        Random random = new Random(28);
        for (int iteration = 0; iteration < 100; iteration++) {
            Map<String, Long> weights = keys(60);
            weights.replaceAll((key, value) -> 1L + random.nextInt(100_000));
            weights.put("large", 10_000_000L);
            assertBound(weights, assign(weights, workers(8)), 8);
        }
    }

    @Test
    public void testDifferentWorkerMembershipCanBeReconstructed()
    {
        Map<String, Long> weights = keys(60);
        List<String> originalWorkers = workers(8);
        Map<String, String> original = assign(weights, originalWorkers);
        List<String> remaining = workers(8);
        remaining.remove("worker-3");
        Map<String, String> afterLoss = assign(weights, remaining);
        assertTrue(afterLoss.values().stream().noneMatch("worker-3"::equals));
        assertBound(weights, afterLoss, 7);
        Collections.reverse(remaining);
        assertEquals(assign(weights, remaining), afterLoss);
        assertEquals(assign(weights, originalWorkers), original);
    }

    @Test
    public void testSameCountButDifferentWorkersIsNotTheSameMembership()
    {
        Map<String, Long> weights = keys(60);
        List<String> replaced = workers(8);
        replaced.set(0, "replacement");
        assertTrue(assign(weights, replaced).values().stream().noneMatch("worker-0"::equals));
    }

    @Test
    public void testInputSubsetHasNoCrossQueryOwnershipGuarantee()
    {
        Map<String, Long> weights = keys(100);
        Map<String, String> owners = assign(weights, workers(8));
        // Overflow depends on competing keys. Explicitly document this rather
        // than claiming a per-source layout is a table-wide cache directory.
        assertTrue(weights.keySet().stream().anyMatch(key ->
                !assign(ImmutableMap.of(key, 1L), workers(8)).get(key).equals(owners.get(key))));
    }

    @Test
    public void testInvalidInputsAndEmptyScan()
    {
        assertEquals(assign(ImmutableMap.of(), workers(8)), ImmutableMap.of());
        expectThrows(IllegalArgumentException.class, () -> assign(keys(1), ImmutableList.of()));
        expectThrows(IllegalArgumentException.class, () -> assign(keys(1), ImmutableList.of("same", "same")));
        expectThrows(IllegalArgumentException.class, () -> assign(ImmutableMap.of("file", 0L), workers(8)));
        expectThrows(ArithmeticException.class, () -> assign(ImmutableMap.of("a", Long.MAX_VALUE, "b", 1L), workers(8)));
        assertEquals(assign(ImmutableMap.of("a", Long.MAX_VALUE), workers(1)).size(), 1);
    }

    private static Map<String, Long> keys(int count)
    {
        Map<String, Long> weights = new HashMap<>();
        IntStream.range(0, count).forEach(i -> weights.put("s3://bucket/table/part-" + i + "#0", 1L));
        return weights;
    }

    private static List<String> workers(int count)
    {
        return IntStream.range(0, count).mapToObj(i -> "worker-" + i).collect(toList());
    }

    private static Map<String, Long> loads(Map<String, Long> weights, Map<String, String> owners)
    {
        Map<String, Long> loads = new HashMap<>();
        owners.forEach((key, worker) -> loads.merge(worker, weights.get(key), Long::sum));
        return loads;
    }

    private static void assertBound(Map<String, Long> weights, Map<String, String> owners, int workerCount)
    {
        assertEquals(owners.keySet(), weights.keySet());
        long limit = capacity(weights.values(), workerCount);
        assertTrue(loads(weights, owners).values().stream().allMatch(load -> load <= limit));
    }
}
