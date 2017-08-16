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

import com.facebook.presto.resourceGroups.db.H2ResourceGroupsDao;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.adminSession;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.createQueryRunner;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.getDao;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.getDbConfigUrl;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestResourceGroupSpecSystemTable
{
    private static final String RESOURCE_GROUP_SPECS_QUERY = "SELECT resource_group_template_id,\n" +
            "  soft_memory_limit,\n" +
            "  max_queued,\n" +
            "  max_running,\n" +
            "  scheduling_policy,\n" +
            "  scheduling_weight,\n" +
            "  jmx_export,\n" +
            "  soft_cpu_limit,\n" +
            "  hard_cpu_limit,\n" +
            "  queued_time_limit,\n" +
            "  running_time_limit,\n" +
            "  cpu_quota_period,\n" +
            "  parent_template_id\n" +
            "FROM resource_group_managers.system.resource_group_specs";
    @Test
    public void testResourceGroupSpecSystemTable()
            throws Exception
    {
        String dbConfigUrl = getDbConfigUrl();
        H2ResourceGroupsDao dao = getDao(dbConfigUrl);
        try (DistributedQueryRunner queryRunner = createQueryRunner(dbConfigUrl, dao)) {
            MaterializedResult result = getResourceGroupSpecs(queryRunner);
            assertEquals(result.getRowCount(), 6);
            for (MaterializedRow row : result.getMaterializedRows()) {
                if (row.getField(0).toString().equals("global.user-${USER}.dashboard-${USER}")) {
                    assertTrue(row.getField(1).equals("1MB"));
                    assertTrue(row.getField(11).equals("1.00h"));
                    assertTrue(row.getField(12).equals("global.user-${USER}"));
                    break;
                }
            }
        }
    }

    private static MaterializedResult getResourceGroupSpecs(DistributedQueryRunner queryRunner)
    {
        return queryRunner.execute(adminSession(), RESOURCE_GROUP_SPECS_QUERY);
    }
}
