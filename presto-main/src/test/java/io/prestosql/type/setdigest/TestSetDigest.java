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

package io.prestosql.type.setdigest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static io.prestosql.type.setdigest.SetDigest.DEFAULT_MAX_HASHES;
import static io.prestosql.type.setdigest.SetDigest.NUMBER_OF_BUCKETS;
import static io.prestosql.type.setdigest.SetDigestFunctions.hashCounts;
import static io.prestosql.type.setdigest.SetDigestFunctions.intersectionCardinality;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestSetDigest
{
    @Test
    public void testIntersectionCardinality()
            throws Exception
    {
        testIntersectionCardinality(DEFAULT_MAX_HASHES, NUMBER_OF_BUCKETS, DEFAULT_MAX_HASHES, NUMBER_OF_BUCKETS);
    }

    @Test
    public void testUnevenIntersectionCardinality()
            throws Exception
    {
        testIntersectionCardinality(DEFAULT_MAX_HASHES / 4, NUMBER_OF_BUCKETS, DEFAULT_MAX_HASHES, NUMBER_OF_BUCKETS);
    }

    private static void testIntersectionCardinality(int maxHashes1, int numBuckets1, int maxHashes2, int numBuckets2)
            throws Exception
    {
        List<Integer> sizes = new ArrayList<>();

        Random rand = new Random(0);
        // Generate random size from each power of ten in [10, 100,000,000]
        for (int i = 10; i < 100_000_000; i *= 10) {
            sizes.add(rand.nextInt(i) + 10);
        }

        for (int size : sizes) {
            int expectedCardinality = 0;
            SetDigest digest1 = new SetDigest(maxHashes1, numBuckets1);
            SetDigest digest2 = new SetDigest(maxHashes2, numBuckets2);

            for (int j = 0; j < size; j++) {
                int added = 0;
                long value = rand.nextLong();
                if (rand.nextDouble() < 0.5) {
                    digest1.add(value);
                    added++;
                }
                if (rand.nextDouble() < 0.5) {
                    digest2.add(value);
                    added++;
                }
                if (added == 2) {
                    expectedCardinality++;
                }
            }

            long estimatedCardinality = intersectionCardinality(digest1.serialize(), digest2.serialize());
            assertTrue(Math.abs(expectedCardinality - estimatedCardinality) / (double) expectedCardinality < 0.10,
                    format("Expected intersection cardinality %d +/- 10%%, got %d, for set of size %d", expectedCardinality, estimatedCardinality, size));
        }
    }

    @Test
    public void testHashCounts()
            throws Exception
    {
        SetDigest digest1 = new SetDigest();
        digest1.add(0);
        digest1.add(0);
        digest1.add(1);

        SetDigest digest2 = new SetDigest();
        digest2.add(0);
        digest2.add(0);
        digest2.add(2);
        digest2.add(2);

        ObjectMapper mapper = new ObjectMapperProvider().get();

        Slice slice = hashCounts(digest1.serialize());
        Map<Long, Short> counts = mapper.readValue(slice.toStringUtf8(), new TypeReference<Map<Long, Short>>() {});
        Set<Short> expected = ImmutableSet.of((short) 1, (short) 2);
        assertEquals(counts.values(), expected);

        digest1.mergeWith(digest2);
        slice = hashCounts(digest1.serialize());
        counts = mapper.readValue(slice.toStringUtf8(), new TypeReference<Map<Long, Short>>() {});
        expected = ImmutableSet.of((short) 1, (short) 2, (short) 4);
        assertEquals(ImmutableSet.copyOf(counts.values()), expected);
    }

    @Test
    public void testSmallLargeIntersections()
    {
        List<Integer> sizes = new ArrayList<>();

        Random rand = new Random(0);
        for (int i = 1000; i < 1_000_000; i *= 10) {
            sizes.add(rand.nextInt(i) + 10);
        }

        for (int size1 : sizes) {
            SetDigest digest1 = new SetDigest(DEFAULT_MAX_HASHES, NUMBER_OF_BUCKETS);
            Map<SetDigest, Integer> smallerSets = new HashMap<>();
            for (int size2 : sizes) {
                if (size2 >= size1) {
                    break;
                }
                for (int overlap = 2; overlap <= 10; overlap += 2) {
                    int expectedCardinality = 0;
                    SetDigest digest2 = new SetDigest(DEFAULT_MAX_HASHES, NUMBER_OF_BUCKETS);
                    for (int j = 0; j < size1; j++) {
                        long value = rand.nextLong();
                        digest1.add(value);
                        if (rand.nextDouble() < size2 / (double) size1) {
                            if (rand.nextDouble() * 10 < overlap) {
                                digest2.add(value);
                                expectedCardinality++;
                            }
                            else {
                                digest2.add(rand.nextLong());
                            }
                        }
                    }
                    smallerSets.put(digest2, expectedCardinality);
                }
            }
            for (Map.Entry<SetDigest, Integer> pair : smallerSets.entrySet()) {
                SetDigest digest2 = pair.getKey();
                long estIntersectionCardinality =
                        intersectionCardinality(digest1.serialize(), digest2.serialize());
                double size2 = digest2.cardinality();
                assertTrue(estIntersectionCardinality <= size2);
                int expectedCardinality = pair.getValue();
                assertTrue(Math.abs(expectedCardinality - estIntersectionCardinality) /
                        (double) size1 < 0.05);
            }
        }
    }
}
