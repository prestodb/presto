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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Behavioral tests for worker-advertised capacity capping of query limits in ClusterMemoryManager.
 * Verifies that effective user/total limits are computed correctly from config and worker capacity.
 */
public class TestClusterMemoryManager
{
    private static final long CONFIG_MAX_USER_BYTES = 20L << 30;   // 20 GB
    private static final long CONFIG_MAX_TOTAL_BYTES = 40L << 30;  // 40 GB

    @Test
    public void testEffectiveLimitsCappedByWorkerCapacityWhenEnabledAndNotSchedulingOnCoordinator()
    {
        // Worker capacity smaller than configured limits -> effective limits are capped by worker capacity
        long workerCapacityBytes = 10L << 30;  // 10 GB
        long[] effective = ClusterMemoryManager.computeEffectiveQueryMemoryLimits(
                CONFIG_MAX_USER_BYTES,
                CONFIG_MAX_TOTAL_BYTES,
                true,   // useWorkerAdvertisedMemoryForLimit
                false,  // isWorkScheduledOnCoordinator (coordinator does not run tasks)
                workerCapacityBytes);

        assertEquals(effective[0], workerCapacityBytes, "effective max user memory should be capped by worker capacity");
        assertEquals(effective[1], workerCapacityBytes, "effective max total memory should be capped by worker capacity");
    }

    @Test
    public void testEffectiveLimitsUseConfigWhenWorkerCapacityLargerThanConfig()
    {
        // Worker capacity larger than configured limits -> configured limits still apply
        long workerCapacityBytes = 100L << 30;  // 100 GB
        long[] effective = ClusterMemoryManager.computeEffectiveQueryMemoryLimits(
                CONFIG_MAX_USER_BYTES,
                CONFIG_MAX_TOTAL_BYTES,
                true,
                false,
                workerCapacityBytes);

        assertEquals(effective[0], CONFIG_MAX_USER_BYTES, "effective max user memory should remain config limit");
        assertEquals(effective[1], CONFIG_MAX_TOTAL_BYTES, "effective max total memory should remain config limit");
    }

    @Test
    public void testEffectiveLimitsUnchangedWhenUseWorkerAdvertisedDisabled()
    {
        // useWorkerAdvertisedMemoryForLimit = false -> config limits used regardless of worker capacity
        long workerCapacityBytes = 10L << 30;  // 10 GB (smaller than config)
        long[] effective = ClusterMemoryManager.computeEffectiveQueryMemoryLimits(
                CONFIG_MAX_USER_BYTES,
                CONFIG_MAX_TOTAL_BYTES,
                false,  // useWorkerAdvertisedMemoryForLimit
                false,
                workerCapacityBytes);

        assertEquals(effective[0], CONFIG_MAX_USER_BYTES, "effective max user memory should be config when flag disabled");
        assertEquals(effective[1], CONFIG_MAX_TOTAL_BYTES, "effective max total memory should be config when flag disabled");
    }

    @Test
    public void testEffectiveLimitsUnchangedWhenCoordinatorSchedulesWork()
    {
        // isWorkScheduledOnCoordinator = true -> config limits used (coordinator is a worker)
        long workerCapacityBytes = 10L << 30;  // 10 GB
        long[] effective = ClusterMemoryManager.computeEffectiveQueryMemoryLimits(
                CONFIG_MAX_USER_BYTES,
                CONFIG_MAX_TOTAL_BYTES,
                true,
                true,   // isWorkScheduledOnCoordinator
                workerCapacityBytes);

        assertEquals(effective[0], CONFIG_MAX_USER_BYTES, "effective max user memory should be config when coordinator schedules work");
        assertEquals(effective[1], CONFIG_MAX_TOTAL_BYTES, "effective max total memory should be config when coordinator schedules work");
    }

    @Test
    public void testEffectiveLimitsUseConfigWhenWorkerCapacityZero()
    {
        // No workers reported yet (workerTotalCapacity = 0) -> config limits used
        long[] effective = ClusterMemoryManager.computeEffectiveQueryMemoryLimits(
                CONFIG_MAX_USER_BYTES,
                CONFIG_MAX_TOTAL_BYTES,
                true,
                false,
                0);

        assertEquals(effective[0], CONFIG_MAX_USER_BYTES, "effective max user memory should be config when no worker capacity");
        assertEquals(effective[1], CONFIG_MAX_TOTAL_BYTES, "effective max total memory should be config when no worker capacity");
    }
}
