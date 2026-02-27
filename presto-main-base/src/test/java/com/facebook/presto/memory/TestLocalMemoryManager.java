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
package com.facebook.presto.memory;

import com.facebook.airlift.units.DataSize;
import org.testng.annotations.Test;

import static com.facebook.airlift.units.DataSize.Unit.GIGABYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestLocalMemoryManager
{
    @Test
    public void testReservedMemoryDisabled()
    {
        NodeMemoryConfig config = new NodeMemoryConfig()
                .setReservedPoolEnabled(false)
                .setHeapHeadroom(new DataSize(10, GIGABYTE))
                .setMaxQueryMemoryPerNode(new DataSize(20, GIGABYTE))
                .setMaxQueryTotalMemoryPerNode(new DataSize(20, GIGABYTE));

        LocalMemoryManager localMemoryManager = new LocalMemoryManager(config,
                new DataSize(60, GIGABYTE).toBytes());
        assertFalse(localMemoryManager.getReservedPool().isPresent());
        assertEquals(localMemoryManager.getPools().size(), 1);
    }

    @Test
    public void testReservedMemoryEnabled()
    {
        NodeMemoryConfig config = new NodeMemoryConfig()
                .setHeapHeadroom(new DataSize(10, GIGABYTE))
                .setMaxQueryMemoryPerNode(new DataSize(20, GIGABYTE))
                .setMaxQueryTotalMemoryPerNode(new DataSize(20, GIGABYTE));

        LocalMemoryManager localMemoryManager = new LocalMemoryManager(config,
                new DataSize(60, GIGABYTE).toBytes());
        assertTrue(localMemoryManager.getReservedPool().isPresent());
        assertEquals(localMemoryManager.getPools().size(), 2);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMaxQueryMemoryPerNodeBiggerQueryTotalMemoryPerNode()
    {
        NodeMemoryConfig config = new NodeMemoryConfig()
                .setHeapHeadroom(new DataSize(10, GIGABYTE))
                .setMaxQueryMemoryPerNode(new DataSize(200, GIGABYTE))
                .setMaxQueryTotalMemoryPerNode(new DataSize(20, GIGABYTE));

        new LocalMemoryManager(config, new DataSize(60, GIGABYTE).toBytes());
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Invalid memory configuration.* " +
                    "The sum of max total query memory per node .* and heap headroom .*" +
                    "cannot be larger than the available heap memory .*")
    public void testNotEnoughAvailableMemory()
    {
        NodeMemoryConfig config = new NodeMemoryConfig()
                .setHeapHeadroom(new DataSize(10, GIGABYTE))
                .setMaxQueryMemoryPerNode(new DataSize(20, GIGABYTE))
                .setMaxQueryTotalMemoryPerNode(new DataSize(20, GIGABYTE));

        new LocalMemoryManager(config, new DataSize(10, GIGABYTE).toBytes());
    }

    /**
     * When coordinator-only validation is used, the coordinator only validates that heap headroom
     * fits in the JVM heap. Large query.max-memory-per-node / query.max-total-memory-per-node
     * (intended for workers) do not need to fit in the coordinator's heap.
     */
    @Test
    public void testCoordinatorOnlyValidationAllowsLargePerNodeConfigWithSmallHeap()
    {
        NodeMemoryConfig config = new NodeMemoryConfig()
                .setHeapHeadroom(new DataSize(1, GIGABYTE))
                .setMaxQueryMemoryPerNode(new DataSize(32, GIGABYTE))
                .setMaxQueryTotalMemoryPerNode(new DataSize(64, GIGABYTE));

        long smallCoordinatorHeap = new DataSize(4, GIGABYTE).toBytes();
        LocalMemoryManager localMemoryManager = new LocalMemoryManager(config, smallCoordinatorHeap, true);

        assertFalse(localMemoryManager.getReservedPool().isPresent());
        assertEquals(localMemoryManager.getPools().size(), 1);
        assertEquals(localMemoryManager.getGeneralPool().getMaxBytes(), smallCoordinatorHeap - new DataSize(1, GIGABYTE).toBytes());
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Invalid memory configuration for coordinator.*")
    public void testCoordinatorOnlyValidationFailsWhenHeadroomExceedsHeap()
    {
        NodeMemoryConfig config = new NodeMemoryConfig()
                .setHeapHeadroom(new DataSize(5, GIGABYTE));

        new LocalMemoryManager(config, new DataSize(4, GIGABYTE).toBytes(), true);
    }
}
