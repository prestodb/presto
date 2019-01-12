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
package io.prestosql.connector.system;

import io.airlift.units.Duration;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryManager;
import io.prestosql.execution.QueryStats;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.InMemoryRecordSet;
import io.prestosql.spi.connector.InMemoryRecordSet.Builder;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.type.ArrayType;
import org.joda.time.DateTime;

import javax.inject.Inject;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.prestosql.spi.connector.SystemTable.Distribution.ALL_COORDINATORS;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class QuerySystemTable
        implements SystemTable
{
    public static final SchemaTableName QUERY_TABLE_NAME = new SchemaTableName("runtime", "queries");

    public static final ConnectorTableMetadata QUERY_TABLE = tableMetadataBuilder(QUERY_TABLE_NAME)
            .column("query_id", createUnboundedVarcharType())
            .column("state", createUnboundedVarcharType())
            .column("user", createUnboundedVarcharType())
            .column("source", createUnboundedVarcharType())
            .column("query", createUnboundedVarcharType())
            .column("resource_group_id", new ArrayType(createUnboundedVarcharType()))

            .column("queued_time_ms", BIGINT)
            .column("analysis_time_ms", BIGINT)
            .column("distributed_planning_time_ms", BIGINT)

            .column("created", TIMESTAMP)
            .column("started", TIMESTAMP)
            .column("last_heartbeat", TIMESTAMP)
            .column("end", TIMESTAMP)
            .build();

    private final QueryManager queryManager;

    @Inject
    public QuerySystemTable(QueryManager queryManager)
    {
        this.queryManager = queryManager;
    }

    @Override
    public Distribution getDistribution()
    {
        return ALL_COORDINATORS;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return QUERY_TABLE;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        Builder table = InMemoryRecordSet.builder(QUERY_TABLE);
        List<QueryInfo> queryInfos = queryManager.getQueries().stream()
                .map(BasicQueryInfo::getQueryId)
                .map(queryId -> {
                    try {
                        return queryManager.getFullQueryInfo(queryId);
                    }
                    catch (NoSuchElementException e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(toImmutableList());
        for (QueryInfo queryInfo : queryInfos) {
            QueryStats queryStats = queryInfo.getQueryStats();
            table.addRow(
                    queryInfo.getQueryId().toString(),
                    queryInfo.getState().toString(),
                    queryInfo.getSession().getUser(),
                    queryInfo.getSession().getSource().orElse(null),
                    queryInfo.getQuery(),
                    queryInfo.getResourceGroupId().map(QuerySystemTable::resourceGroupIdToBlock).orElse(null),

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

    private static Block resourceGroupIdToBlock(ResourceGroupId resourceGroupId)
    {
        requireNonNull(resourceGroupId, "resourceGroupId is null");
        List<String> segments = resourceGroupId.getSegments();
        BlockBuilder blockBuilder = createUnboundedVarcharType().createBlockBuilder(null, segments.size());
        for (String segment : segments) {
            createUnboundedVarcharType().writeSlice(blockBuilder, utf8Slice(segment));
        }
        return blockBuilder.build();
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
