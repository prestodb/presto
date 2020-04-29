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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.inject.Inject;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.SystemTable.Distribution.ALL_COORDINATORS;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
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
