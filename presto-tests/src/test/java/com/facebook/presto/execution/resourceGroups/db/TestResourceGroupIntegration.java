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
package com.facebook.presto.execution.resourceGroups.db;

import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupInfo;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.execution.resourceGroups.db.TestQueues.getSimpleQueryRunner;

public class TestResourceGroupIntegration
{
    @Test(timeOut = 60_000)
    public void testMemoryFraction()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = getSimpleQueryRunner()) {
            queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk");
            while (true) {
                TimeUnit.SECONDS.sleep(1);
                ResourceGroupInfo global = queryRunner.getCoordinator().getResourceGroupManager().get().getResourceGroupInfo(new ResourceGroupId("global"));
                if (global.getSoftMemoryLimit().toBytes() > 0) {
                    break;
                }
            }
        }
    }
}
