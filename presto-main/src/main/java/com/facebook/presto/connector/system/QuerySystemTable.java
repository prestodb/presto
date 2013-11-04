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
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.google.common.collect.ImmutableList;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.metadata.MetadataUtil.columnTypeGetter;
import static com.facebook.presto.spi.ColumnType.LONG;
import static com.facebook.presto.spi.ColumnType.STRING;
import static com.google.common.collect.Iterables.transform;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class QuerySystemTable
        implements SystemTable
{
    public static final SchemaTableName QUERY_TABLE_NAME = new SchemaTableName("sys", "query");

    public static final ConnectorTableMetadata QUERY_TABLE = tableMetadataBuilder(QUERY_TABLE_NAME)
            .column("node_id", STRING)
            .column("query_id", STRING)
            .column("state", STRING)
            .column("user", STRING)
            .column("query", STRING)

            .column("queued_time_ms", LONG)
            .column("analysis_time_ms", LONG)
            .column("distributed_planning_time_ms", LONG)

            .column("created", LONG)
            .column("started", LONG)
            .column("last_heartbeat", LONG)
            .column("end", LONG)
            .build();

    private final QueryManager queryManager;
    private final String nodeId;

    @Inject
    public QuerySystemTable(QueryManager queryManager, NodeInfo nodeInfo)
    {
        this.queryManager = queryManager;
        this.nodeId = nodeInfo.getNodeId();
    }

    @Override
    public boolean isDistributed()
    {
        return true;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return QUERY_TABLE;
    }

    @Override
    public List<ColumnType> getColumnTypes()
    {
        return ImmutableList.copyOf(transform(QUERY_TABLE.getColumns(), columnTypeGetter()));
    }

    @Override
    public RecordCursor cursor()
    {
        Builder table = InMemoryRecordSet.builder(QUERY_TABLE);
        for (QueryInfo queryInfo : queryManager.getAllQueryInfo()) {
            QueryStats queryStats = queryInfo.getQueryStats();
            table.addRow(
                    nodeId,
                    queryInfo.getQueryId().toString(),
                    queryInfo.getState().toString(),
                    queryInfo.getSession().getUser(),
                    queryInfo.getQuery(),

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

    private Long toMillis(Duration duration)
    {
        if (duration == null) {
            return null;
        }
        return duration.toMillis();
    }

    private Long toTimeStamp(DateTime dateTime)
    {
        if (dateTime == null) {
            return null;
        }
        return MILLISECONDS.toSeconds(dateTime.getMillis());
    }
}
