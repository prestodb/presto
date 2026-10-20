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
package com.facebook.presto.orc.metadata.statistics.hll;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.statistics.hll.HyperLogLogSerde.DEFAULT_NUMBER_OF_BUCKETS;
import static com.facebook.presto.orc.metadata.statistics.hll.HyperLogLogSerde.deserialize;
import static com.facebook.presto.orc.metadata.statistics.hll.HyperLogLogSerde.serialize;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHyperLogLogSerde
{
    private static final int MAX_SPARSE_VALUES = 100;

    @Test
    public void testRoundTrip()
    {
        assertRoundTrip(ImmutableList.of());
        assertRoundTrip(ImmutableList.of(Optional.empty()));
        assertRoundTrip(ImmutableList.of(Optional.empty(), Optional.empty()));
        assertRoundTrip(ImmutableList.of(createSparse(10)));
        assertRoundTrip(ImmutableList.of(createDense(100)));
        assertRoundTrip(ImmutableList.of(createSparse(10), createDense(100), createDense(100)));
        assertRoundTrip(ImmutableList.of(createDense(100), Optional.empty(), createSparse(10)));
        assertRoundTrip(ImmutableList.of(createDense(100), createSparse(10)));
        assertRoundTrip(ImmutableList.of(Optional.empty(), createSparse(10), Optional.empty(), createDense(100), Optional.empty()));
    }

    @Test
    public void testBackwardCompatibility()
            throws Exception
    {
        // Deserialize the frozen golden blob produced by the bbio DWRF writer
        // and confirm our serializer reproduces the exact same bytes for the
        // equivalent snapshot list. This proves byte-identity with the Spectrum
        // reader contract that already consumes DWRF files in production.
        ByteBuffer serialized = ByteBuffer.wrap(toByteArray(getResource("hll.data")));
        List<Optional<HyperLogLog>> deserialized = deserialize(serialized);
        ByteBuffer reserialized = serialize(deserialized);
        List<Optional<HyperLogLog>> expected = ImmutableList.of(
                Optional.empty(),
                createSparse(10),
                Optional.empty(),
                createDense(100),
                createSparse(10),
                Optional.empty(),
                createDense(100),
                Optional.empty());
        ByteBuffer expectedSerialized = serialize(expected);
        assertEquals(reserialized, expectedSerialized);
    }

    @Test
    public void testCardinalityPreservedAcrossSerialization()
    {
        // Feed values through XxHash64 exactly like OrcWriter does, so this
        // exercises the write-path hashing and the sparse->dense promotion.
        HyperLogLog hll = HyperLogLog.newInstance(DEFAULT_NUMBER_OF_BUCKETS);
        int distinct = 5000;
        for (long i = 0; i < distinct; i++) {
            hll.add(XxHash64.hash(i));
        }
        long before = hll.cardinality();
        assertWithinTenPercent(distinct, before);

        List<Optional<HyperLogLog>> deserialized = deserialize(serialize(ImmutableList.of(Optional.empty(), Optional.of(hll))));
        assertEquals(deserialized.size(), 2);
        assertFalse(deserialized.get(0).isPresent());
        assertTrue(deserialized.get(1).isPresent());
        assertEquals(deserialized.get(1).get().cardinality(), before);
    }

    @Test
    public void testStringHashingCardinality()
    {
        HyperLogLog hll = HyperLogLog.newInstance(DEFAULT_NUMBER_OF_BUCKETS);
        int distinct = 2000;
        for (int i = 0; i < distinct; i++) {
            byte[] bytes = ("value_" + i).getBytes();
            hll.add(XxHash64.hash(bytes, 0, bytes.length));
        }
        assertWithinTenPercent(distinct, hll.cardinality());
    }

    private static void assertWithinTenPercent(long expected, long actual)
    {
        long tolerance = Math.max(1, expected / 10);
        assertTrue(
                actual >= expected - tolerance && actual <= expected + tolerance,
                String.format("cardinality %s not within 10%% of %s", actual, expected));
    }

    private static void assertRoundTrip(List<Optional<HyperLogLog>> list)
    {
        ByteBuffer serialized = serialize(list);
        List<Optional<HyperLogLog>> deserialized = deserialize(serialized.asReadOnlyBuffer());
        ByteBuffer reserialized = serialize(deserialized);
        assertEquals(serialized, reserialized);
    }

    private static Optional<HyperLogLog> createSparse(long distinctValues)
    {
        checkArgument(
                distinctValues <= MAX_SPARSE_VALUES,
                "cannot create sparse hll with more than %s values",
                MAX_SPARSE_VALUES);
        HyperLogLog hll = HyperLogLog.newInstance(DEFAULT_NUMBER_OF_BUCKETS);
        for (long i = 0; i < distinctValues; i++) {
            hll.add(i);
        }
        return Optional.of(hll);
    }

    private static Optional<HyperLogLog> createDense(long distinctValues)
    {
        HyperLogLog hll = HyperLogLog.newInstance(DEFAULT_NUMBER_OF_BUCKETS);
        for (long i = 0; i < distinctValues; i++) {
            hll.add(i);
        }
        hll.makeDense();
        return Optional.of(hll);
    }
}
