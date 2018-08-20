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

import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.server.ResourceGroupInfo;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.ArrayType;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.Optional;

import static com.facebook.presto.connector.system.QuerySystemTable.resourceGroupIdToBlock;
import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.SystemTable.Distribution.ALL_COORDINATORS;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class ResourceGroupInfoSystemTable
        implements SystemTable
{
    public static final SchemaTableName RESOURCE_GROUP_INFO_TABLE_NAME = new SchemaTableName("runtime", "resource_group_info");

    public static final ConnectorTableMetadata RESOURCE_GROUP_INFO_TABLE = tableMetadataBuilder(RESOURCE_GROUP_INFO_TABLE_NAME)
            .column("resource_group_id", new ArrayType(createUnboundedVarcharType()))
            .column("resource_group_state", createUnboundedVarcharType())
            .column("scheduling_policy", createUnboundedVarcharType())
            .column("scheduling_weight", BIGINT)
            .column("soft_memory_limit_bytes", BIGINT)
            .column("soft_concurrency_limit", BIGINT)
            .column("hard_concurrency_limit", BIGINT)
            .column("max_queued_queries", BIGINT)
            .column("running_time_limit", BIGINT)
            .column("queued_time_limit", BIGINT)
            .column("memory_usage_bytes", BIGINT)
            .column("queued_queries", BIGINT)
            .column("running_queries", BIGINT)
            .column("eligible_subgroups", BIGINT)
            .build();

    private final ResourceGroupManager<?> resourceGroupManager;

    @Inject
    public ResourceGroupInfoSystemTable(ResourceGroupManager resourceGroupManager)
    {
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return ALL_COORDINATORS;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return RESOURCE_GROUP_INFO_TABLE;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(RESOURCE_GROUP_INFO_TABLE);
        for (ResourceGroupInfo resourceGroupInfo : resourceGroupManager.getAllResourceGroupInfos()) {
            table.addRow(
                    resourceGroupIdToBlock(resourceGroupInfo.getId()),
                    resourceGroupInfo.getState().toString(),
                    resourceGroupInfo.getSchedulingPolicy().toString(),
                    resourceGroupInfo.getSchedulingWeight(),
                    toBytes(resourceGroupInfo.getSoftMemoryLimit()),
                    resourceGroupInfo.getSoftConcurrencyLimit(),
                    resourceGroupInfo.getHardConcurrencyLimit(),
                    resourceGroupInfo.getMaxQueuedQueries(),
                    Optional.ofNullable(resourceGroupInfo.getRunningTimeLimit()).map(Duration::toMillis).orElse(null),
                    Optional.ofNullable(resourceGroupInfo.getQueuedTimeLimit()).map(Duration::toMillis).orElse(null),
                    toBytes(resourceGroupInfo.getMemoryUsage()),
                    resourceGroupInfo.getNumQueuedQueries(),
                    resourceGroupInfo.getNumRunningQueries(),
                    resourceGroupInfo.getNumEligibleSubGroups());
        }
        return table.build().cursor();
    }

    static Long toBytes(DataSize datasize)
    {
        if (datasize == null) {
            return null;
        }
        return datasize.toBytes();
    }
}
