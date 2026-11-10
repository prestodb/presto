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
package com.facebook.presto.spark.execution;

import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PartitioningScheme;
import org.apache.spark.Partitioner;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.stream.IntStream;

import static com.facebook.presto.spark.execution.AbstractPrestoSparkQueryExecution.createPartitioner;
import static com.facebook.presto.spark.execution.AbstractPrestoSparkQueryExecution.getSystemBucketToPartition;
import static com.facebook.presto.spark.util.PrestoSparkUtils.getPartitionCountFromBucketToPartition;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitionFunction.ROUND_ROBIN;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitioning.FIXED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.createSystemPartitioning;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestPrestoSparkPartitioning
{
    private static final int HASH_PARTITION_COUNT = 4;
    private static final PartitioningHandle COUNTED_ROUND_ROBIN = createSystemPartitioning(FIXED, ROUND_ROBIN, 8);

    @Test
    public void testBucketToPartitionHonorsExplicitCount()
    {
        assertEquals(getSystemBucketToPartition(COUNTED_ROUND_ROBIN, HASH_PARTITION_COUNT).orElse(null), IntStream.range(0, 8).toArray());
    }

    @Test
    public void testBucketToPartitionIsNotClampedToHashPartitionCount()
    {
        assertEquals(getSystemBucketToPartition(createSystemPartitioning(FIXED, ROUND_ROBIN, 64), HASH_PARTITION_COUNT).orElse(null), IntStream.range(0, 64).toArray());
    }

    @Test
    public void testBucketToPartitionUnchangedForCountLessHandles()
    {
        assertEquals(getSystemBucketToPartition(FIXED_ARBITRARY_DISTRIBUTION, HASH_PARTITION_COUNT).orElse(null), IntStream.range(0, HASH_PARTITION_COUNT).toArray());
        assertEquals(getSystemBucketToPartition(FIXED_HASH_DISTRIBUTION, HASH_PARTITION_COUNT).orElse(null), IntStream.range(0, HASH_PARTITION_COUNT).toArray());
        assertFalse(getSystemBucketToPartition(FIXED_BROADCAST_DISTRIBUTION, HASH_PARTITION_COUNT).isPresent());
    }

    @Test
    public void testCreatePartitionerForCountedHandle()
    {
        assertEquals(createPartitioner(partitioningScheme(COUNTED_ROUND_ROBIN, HASH_PARTITION_COUNT)).numPartitions(), 8);
    }

    @Test
    public void testCreatePartitionerUnchangedForCountLessHandles()
    {
        assertEquals(createPartitioner(partitioningScheme(FIXED_ARBITRARY_DISTRIBUTION, HASH_PARTITION_COUNT)).numPartitions(), HASH_PARTITION_COUNT);
        assertEquals(createPartitioner(partitioningScheme(FIXED_HASH_DISTRIBUTION, HASH_PARTITION_COUNT)).numPartitions(), HASH_PARTITION_COUNT);

        Partitioner single = createPartitioner(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, Collections.emptyList()), Collections.emptyList()));
        assertEquals(single.numPartitions(), 1);
    }

    @Test
    public void testCreatePartitionerStillRejectsUnsupportedPartitioning()
    {
        IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> createPartitioner(new PartitioningScheme(Partitioning.create(FIXED_BROADCAST_DISTRIBUTION, Collections.emptyList()), Collections.emptyList())));
        assertTrue(exception.getMessage().startsWith("Unexpected partitioning"), exception.getMessage());
    }

    @Test
    public void testWriteSideAndPartitionerAgreeOnPartitionCount()
    {
        for (int requestedCount : new int[] {1, 2, 8, 64}) {
            PartitioningHandle handle = createSystemPartitioning(FIXED, ROUND_ROBIN, requestedCount);
            int[] bucketToPartition = getSystemBucketToPartition(handle, HASH_PARTITION_COUNT).orElseThrow(AssertionError::new);
            int writeSideCount = getPartitionCountFromBucketToPartition(bucketToPartition);
            assertEquals(writeSideCount, createPartitioner(partitioningScheme(handle, HASH_PARTITION_COUNT)).numPartitions());
            assertEquals(writeSideCount, requestedCount);
        }
    }

    @Test
    public void testPartitionCountIsMaxPlusOneNotArrayLength()
    {
        assertEquals(getPartitionCountFromBucketToPartition(new int[] {0, 1, 2, 3}), 4);
        assertEquals(getPartitionCountFromBucketToPartition(new int[] {0, 1, 0, 1, 0, 1}), 2);
        assertEquals(getPartitionCountFromBucketToPartition(new int[] {0}), 1);
    }

    @Test
    public void testPartitionCountRejectsEmptyMapping()
    {
        expectThrows(IllegalArgumentException.class, () -> getPartitionCountFromBucketToPartition(new int[] {}));
    }

    private static PartitioningScheme partitioningScheme(PartitioningHandle handle, int hashPartitionCount)
    {
        return new PartitioningScheme(Partitioning.create(handle, Collections.emptyList()), Collections.emptyList())
                .withBucketToPartition(getSystemBucketToPartition(handle, hashPartitionCount));
    }
}
