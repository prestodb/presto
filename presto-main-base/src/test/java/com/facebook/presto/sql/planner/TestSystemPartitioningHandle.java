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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.plan.PartitioningHandle;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitionFunction.ROUND_ROBIN;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitioning.FIXED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.createSystemPartitioning;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.getSystemPartitionCount;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.isFixedRoundRobin;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestSystemPartitioningHandle
{
    private static final PartitioningHandle COUNTED_ROUND_ROBIN = createSystemPartitioning(FIXED, ROUND_ROBIN, 8);

    @Test
    public void testCountedHandleIsNotEqualToSingleton()
    {
        assertNotEquals(COUNTED_ROUND_ROBIN, FIXED_ARBITRARY_DISTRIBUTION);
        assertNotEquals(COUNTED_ROUND_ROBIN, createSystemPartitioning(FIXED, ROUND_ROBIN, 16));
        assertEquals(COUNTED_ROUND_ROBIN, createSystemPartitioning(FIXED, ROUND_ROBIN, 8));
        assertEquals(COUNTED_ROUND_ROBIN.hashCode(), createSystemPartitioning(FIXED, ROUND_ROBIN, 8).hashCode());
    }

    @Test
    public void testIsFixedRoundRobin()
    {
        assertTrue(isFixedRoundRobin(FIXED_ARBITRARY_DISTRIBUTION));
        assertTrue(isFixedRoundRobin(COUNTED_ROUND_ROBIN));
        assertTrue(isFixedRoundRobin(createSystemPartitioning(FIXED, ROUND_ROBIN, 1)));
    }

    @Test
    public void testIsFixedRoundRobinRejectsOtherPartitionings()
    {
        assertFalse(isFixedRoundRobin(SCALED_WRITER_DISTRIBUTION));
        assertFalse(isFixedRoundRobin(FIXED_HASH_DISTRIBUTION));
        assertFalse(isFixedRoundRobin(FIXED_BROADCAST_DISTRIBUTION));
        assertFalse(isFixedRoundRobin(SINGLE_DISTRIBUTION));
        assertFalse(isFixedRoundRobin(SOURCE_DISTRIBUTION));
    }

    @Test
    public void testGetSystemPartitionCount()
    {
        assertEquals(getSystemPartitionCount(COUNTED_ROUND_ROBIN), Optional.of(8));
        assertEquals(getSystemPartitionCount(FIXED_ARBITRARY_DISTRIBUTION), Optional.empty());
        assertEquals(getSystemPartitionCount(FIXED_HASH_DISTRIBUTION), Optional.empty());
    }

    @Test
    public void testToStringOfCountedHandle()
    {
        assertEquals(COUNTED_ROUND_ROBIN.toString(), "ROUND_ROBIN");
    }
}
