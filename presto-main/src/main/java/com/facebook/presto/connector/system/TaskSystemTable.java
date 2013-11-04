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
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.google.common.collect.ImmutableList;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
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

public class TaskSystemTable
        implements SystemTable
{
    public static final SchemaTableName TASK_TABLE_NAME = new SchemaTableName("sys", "task");

    public static final ConnectorTableMetadata TASK_TABLE = tableMetadataBuilder(TASK_TABLE_NAME)
            .column("node_id", STRING)

            .column("task_id", STRING)
            .column("stage_id", STRING)
            .column("query_id", STRING)
            .column("state", STRING)

            .column("splits", LONG)
            .column("queued_splits", LONG)
            .column("running_splits", LONG)
            .column("completed_splits", LONG)

            .column("split_scheduled_time_ms", LONG)
            .column("split_cpu_time_ms", LONG)
            .column("split_user_time_ms", LONG)
            .column("split_blocked_time_ms", LONG)

            .column("raw_input_bytes", LONG)
            .column("raw_input_rows", LONG)

            .column("processed_input_bytes", LONG)
            .column("processed_input_rows", LONG)

            .column("output_bytes", LONG)
            .column("output_rows", LONG)

            .column("created", LONG)
            .column("start", LONG)
            .column("last_heartbeat", LONG)
            .column("end", LONG)
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
    public List<ColumnType> getColumnTypes()
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
                    toTimeStamp(stats.getStartTime()),
                    toTimeStamp(taskInfo.getLastHeartbeat()),
                    toTimeStamp(stats.getEndTime()));
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

    private Long toBytes(DataSize dataSize)
    {
        if (dataSize == null) {
            return null;
        }
        return dataSize.toBytes();
    }

    private Long toTimeStamp(DateTime dateTime)
    {
        if (dateTime == null) {
            return null;
        }
        return MILLISECONDS.toSeconds(dateTime.getMillis());
    }
}
