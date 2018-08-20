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
package com.facebook.presto.connector.system;

import com.facebook.presto.execution.resourceGroups.InternalResourceGroupManager;
import com.facebook.presto.resourceGroups.ResourceGroupManagerPlugin;
import com.facebook.presto.server.ResourceGroupInfo;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupState;
import com.facebook.presto.spi.resourceGroups.SchedulingPolicy;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.spi.resourceGroups.ResourceGroupState.CAN_RUN;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.FAIR;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.QUERY_PRIORITY;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestResourceGroupInfoSystemTable
{
    private static final String RESOURCE_GROUP_INFO_QUERY = "SELECT" +
            "  resource_group_id," +
            "  resource_group_state," +
            "  scheduling_policy," +
            "  scheduling_weight," +
            "  soft_memory_limit_bytes," +
            "  soft_concurrency_limit," +
            "  hard_concurrency_limit," +
            "  max_queued_queries," +
            "  running_time_limit," +
            "  queued_time_limit," +
            "  CAST(0 AS BIGINT) as memory_usage_bytes," + // Set to 0 otherwise result is non-deterministic
            "  queued_queries," +
            "  CAST(0 AS BIGINT) as running_queries," + // Set to 0 otherwise result is non-deterministic
            "  eligible_subgroups" +
            "  FROM system.runtime.resource_group_info";

    @Test
    public void testResourceGroupInfoSystemTable()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder().build()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            InternalResourceGroupManager<?> manager = queryRunner.getCoordinator().getResourceGroupManager().get();
            manager.setConfigurationManager("file", ImmutableMap.of(
                    "resource-groups.config-file", this.getClass().getClassLoader().getResource("resource_group_config_all_parameters.json").getPath()));
            queryRunner.execute(testSessionBuilder().setCatalog("tpch").setSchema("tiny").setSource("dashboard-foo").build(), "SELECT COUNT(*) FROM nation");
            queryRunner.execute(testSessionBuilder().setCatalog("tpch").setSchema("tiny").setSource("adhoc-foo").build(), "SELECT COUNT(*) FROM nation");
            queryRunner.execute(testSessionBuilder().setCatalog("tpch").setSchema("tiny").setSource("test").build(), "SELECT COUNT(*) FROM nation");
            List<MaterializedRow> rows = queryRunner.execute(testSessionBuilder().setCatalog("tpch").setSchema("tiny").setSource("test").build(), RESOURCE_GROUP_INFO_QUERY).getMaterializedRows();
            Set<ResourceGroupInfo> expected = ImmutableSet.<ResourceGroupInfo>builder()
                    .add(new ResourceGroupInfo(
                            new ResourceGroupId("global"),
                            CAN_RUN,
                            FAIR,
                            1,
                            DataSize.valueOf("1MB"),
                            99,
                            100,
                            1000,
                            Duration.valueOf("1h"),
                            Duration.valueOf("3h"),
                            DataSize.valueOf("0B"),
                            0,
                            0,
                            0,
                            null,
                            null))
                    .add(new ResourceGroupInfo(
                            new ResourceGroupId(ImmutableList.of("global", "user-user")),
                            CAN_RUN,
                            QUERY_PRIORITY,
                            1,
                            DataSize.valueOf("1MB"),
                            3,
                            4,
                            3,
                            Duration.valueOf("1h"),
                            Duration.valueOf("3h"),
                            DataSize.valueOf("0B"),
                            0,
                            0,
                            0,
                            null,
                            null))
                    .add(new ResourceGroupInfo(
                            new ResourceGroupId(ImmutableList.of("global", "test")),
                            CAN_RUN,
                            FAIR,
                            1,
                            DataSize.valueOf("1MB"),
                            2,
                            3,
                            3,
                            Duration.valueOf("1h"),
                            Duration.valueOf("3h"),
                            DataSize.valueOf("0B"),
                            0,
                            0,
                            0,
                            null,
                            null))
                    .add(new ResourceGroupInfo(
                            new ResourceGroupId(ImmutableList.of("global", "user-user", "dashboard-user")),
                            CAN_RUN,
                            QUERY_PRIORITY,
                            1,
                            DataSize.valueOf("1MB"),
                            1,
                            2,
                            4,
                            Duration.valueOf("1h"),
                            Duration.valueOf("3h"),
                            DataSize.valueOf("0B"),
                            0,
                            0,
                            0,
                            null,
                            null))
                    .add(new ResourceGroupInfo(
                            new ResourceGroupId(ImmutableList.of("global", "user-user", "adhoc-user")),
                            CAN_RUN,
                            QUERY_PRIORITY,
                            1,
                            DataSize.valueOf("1MB"),
                            2,
                            3,
                            3,
                            Duration.valueOf("1h"),
                            Duration.valueOf("3h"),
                            DataSize.valueOf("0B"),
                            0,
                            0,
                            0,
                            null,
                            null))
                    .build();
            ImmutableSet.Builder<ResourceGroupInfo> builder = ImmutableSet.builder();

            for (MaterializedRow row : rows) {
                builder.add(
                        new ResourceGroupInfo(
                                new ResourceGroupId((List<String>) row.getField(0)),
                                ResourceGroupState.valueOf(row.getField(1).toString()),
                                SchedulingPolicy.valueOf(row.getField(2).toString()),
                                toIntExact((Long) row.getField(3)),
                                row.getField(4) == null ? null : DataSize.succinctBytes((long) row.getField(4)),
                                toIntExact((Long) row.getField(5)),
                                toIntExact((Long) row.getField(6)),
                                toIntExact((Long) row.getField(7)),
                                row.getField(8) == null ? null : Duration.succinctNanos(1_000_000L * (Long) row.getField(8)),
                                row.getField(9) == null ? null : Duration.succinctNanos(1_000_000L * (Long) row.getField(9)),
                                row.getField(10) == null ? null : DataSize.succinctBytes((long) row.getField(10)),
                                toIntExact((Long) row.getField(11)),
                                toIntExact((Long) row.getField(12)),
                                toIntExact((Long) row.getField(13)),
                                null,
                                null));
            }
            assertEquals(builder.build(), expected);
        }
    }
}
