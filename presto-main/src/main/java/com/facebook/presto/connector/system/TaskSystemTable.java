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

import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.metadata.MetadataUtil.columnTypeGetter;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Iterables.transform;

public class TaskSystemTable
        implements SystemTable
{
    public static final SchemaTableName TASK_TABLE_NAME = new SchemaTableName("sys", "task");

    public static final ConnectorTableMetadata TASK_TABLE = tableMetadataBuilder(TASK_TABLE_NAME)
            .column("node_id", VARCHAR)

            .column("task_id", VARCHAR)
            .column("stage_id", VARCHAR)
            .column("query_id", VARCHAR)
            .column("state", VARCHAR)

            .column("splits", BIGINT)
            .column("queued_splits", BIGINT)
            .column("running_splits", BIGINT)
            .column("completed_splits", BIGINT)

            .column("split_scheduled_time_ms", BIGINT)
            .column("split_cpu_time_ms", BIGINT)
            .column("split_user_time_ms", BIGINT)
            .column("split_blocked_time_ms", BIGINT)

            .column("raw_input_bytes", BIGINT)
            .column("raw_input_rows", BIGINT)

            .column("processed_input_bytes", BIGINT)
            .column("processed_input_rows", BIGINT)

            .column("output_bytes", BIGINT)
            .column("output_rows", BIGINT)

            .column("created", BIGINT)
            .column("start", BIGINT)
            .column("last_heartbeat", BIGINT)
            .column("end", BIGINT)
            .build();

    private final TaskManager taskManager;
    private final String nodeId;

    @Inject
    public TaskSystemTable(TaskManager taskManager, NodeInfo nodeInfo)
    {
        this.taskManager = taskManager;
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
        return TASK_TABLE;
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return ImmutableList.copyOf(transform(TASK_TABLE.getColumns(), columnTypeGetter()));
    }

    @Override
    public RecordCursor cursor()
    {
        Builder table = InMemoryRecordSet.builder(TASK_TABLE);
        for (TaskInfo taskInfo : taskManager.getAllTaskInfo(false)) {
            TaskStats stats = taskInfo.getStats();
            table.addRow(
                    nodeId,

                    taskInfo.getTaskId().toString(),
                    taskInfo.getTaskId().getStageId().toString(),
                    taskInfo.getTaskId().getQueryId().toString(),
                    taskInfo.getState().toString(),

                    (long) stats.getTotalDrivers(),
                    (long) stats.getQueuedDrivers(),
                    (long) stats.getRunningDrivers(),
                    (long) stats.getCompletedDrivers(),

                    toMillis(stats.getTotalScheduledTime()),
                    toMillis(stats.getTotalCpuTime()),
                    toMillis(stats.getTotalUserTime()),
                    toMillis(stats.getTotalBlockedTime()),

                    toBytes(stats.getRawInputDataSize()),
                    stats.getRawInputPositions(),

                    toBytes(stats.getProcessedInputDataSize()),
                    stats.getProcessedInputPositions(),

                    toBytes(stats.getOutputDataSize()),
                    stats.getOutputPositions(),

                    toTimeStamp(stats.getCreateTime()),
                    toTimeStamp(stats.getFirstStartTime()),
                    toTimeStamp(taskInfo.getLastHeartbeat()),
                    toTimeStamp(stats.getEndTime()));
        }
        return table.build().cursor();
    }

    private static Long toMillis(Duration duration)
    {
        if (duration == null) {
            return null;
        }
        return duration.toMillis();
    }

    private static Long toBytes(DataSize dataSize)
    {
        if (dataSize == null) {
            return null;
        }
        return dataSize.toBytes();
    }

    private static Long toTimeStamp(DateTime dateTime)
    {
        if (dateTime == null) {
            return null;
        }
        return dateTime.getMillis();
    }
}
