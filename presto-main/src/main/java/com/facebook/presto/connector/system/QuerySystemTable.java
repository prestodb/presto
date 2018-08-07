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

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.SystemTable.Distribution.ALL_COORDINATORS;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class QuerySystemTable
        implements SystemTable
{
    public static final SchemaTableName QUERY_TABLE_NAME = new SchemaTableName("runtime", "queries");

    private final ConnectorTableMetadata queryTableMetadata;
    private final MapType sessionPropertiesType;
    private final QueryManager queryManager;
    private final String nodeId;

    @Inject
    public QuerySystemTable(QueryManager queryManager, NodeInfo nodeInfo, TypeManager typeManager)
    {
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(nodeInfo, "nodeInfo is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.nodeId = nodeInfo.getNodeId();
        this.sessionPropertiesType = (MapType) typeManager.getParameterizedType(StandardTypes.MAP,
                ImmutableList.<TypeSignatureParameter>builder()
                        .add(TypeSignatureParameter.of(createUnboundedVarcharType().getTypeSignature()))
                        .add(TypeSignatureParameter.of(createUnboundedVarcharType().getTypeSignature()))
                        .build());
        this.queryTableMetadata = tableMetadataBuilder(QUERY_TABLE_NAME)
            .column("node_id", createUnboundedVarcharType())
            .column("query_id", createUnboundedVarcharType())
            .column("state", createUnboundedVarcharType())
            .column("user", createUnboundedVarcharType())
            .column("source", createUnboundedVarcharType())
            .column("query", createUnboundedVarcharType())
            .column("resource_group_id", new ArrayType(createUnboundedVarcharType()))
            .column("session_properties", sessionPropertiesType)
            .column("queued_time_ms", BIGINT)
            .column("analysis_time_ms", BIGINT)
            .column("distributed_planning_time_ms", BIGINT)

            .column("created", TIMESTAMP)
            .column("started", TIMESTAMP)
            .column("last_heartbeat", TIMESTAMP)
            .column("end", TIMESTAMP)
            .build();
    }

    @Override
    public Distribution getDistribution()
    {
        return ALL_COORDINATORS;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return queryTableMetadata;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        Builder table = InMemoryRecordSet.builder(queryTableMetadata);
        for (QueryInfo queryInfo : queryManager.getAllQueryInfo()) {
            QueryStats queryStats = queryInfo.getQueryStats();
            table.addRow(
                    nodeId,
                    queryInfo.getQueryId().toString(),
                    queryInfo.getState().toString(),
                    queryInfo.getSession().getUser(),
                    queryInfo.getSession().getSource().orElse(null),
                    queryInfo.getQuery(),
                    queryInfo.getResourceGroupId().map(QuerySystemTable::resourceGroupIdToBlock).orElse(null),
                    sessionPropertiesToBlock(queryInfo.getSession().getSystemProperties()),
                    toMillis(queryStats.getQueuedTime()),
                    toMillis(queryStats.getAnalysisTime()),
                    toMillis(queryStats.getDistributedPlanningTime()),

                    toTimeStamp(queryStats.getCreateTime()),
                    toTimeStamp(queryStats.getExecutionStartTime()),
                    toTimeStamp(queryStats.getLastHeartbeat()),
                    toTimeStamp(queryStats.getEndTime()));
        }
        return table.build().cursor();
    }

    static Block resourceGroupIdToBlock(ResourceGroupId resourceGroupId)
    {
        requireNonNull(resourceGroupId, "resourceGroupId is null");
        List<String> segments = resourceGroupId.getSegments();
        BlockBuilder blockBuilder = createUnboundedVarcharType().createBlockBuilder(null, segments.size());
        for (String segment : segments) {
            createUnboundedVarcharType().writeSlice(blockBuilder, utf8Slice(segment));
        }
        return blockBuilder.build();
    }

    private Block sessionPropertiesToBlock(Map<String, String> properties)
    {
        requireNonNull(properties, "properties is null");
        BlockBuilder blockBuilder = sessionPropertiesType.createBlockBuilder(null, properties.size());
        BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
        Type keyType = sessionPropertiesType.getTypeParameters().get(0);
        Type valueType = sessionPropertiesType.getTypeParameters().get(1);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            appendToBlockBuilder(keyType, entry.getKey(), entryBuilder);
            appendToBlockBuilder(valueType, entry.getValue(), entryBuilder);
        }
        blockBuilder.closeEntry();
        return sessionPropertiesType.getObject(blockBuilder, 0);
    }

    private static void appendToBlockBuilder(Type type, String value, BlockBuilder blockBuilder)
    {
        // Copied from StructuralTestUtil
        if (value == null) {
            blockBuilder.appendNull();
        }
        else {
            type.writeSlice(blockBuilder, utf8Slice(value));
        }
    }

    private static Long toMillis(Duration duration)
    {
        if (duration == null) {
            return null;
        }
        return duration.toMillis();
    }

    private static Long toTimeStamp(DateTime dateTime)
    {
        if (dateTime == null) {
            return null;
        }
        return dateTime.getMillis();
    }
}
