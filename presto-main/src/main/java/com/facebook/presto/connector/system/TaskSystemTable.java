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

import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.inject.Inject;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.SystemTable.Distribution.ALL_NODES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class TaskSystemTable
        implements SystemTable
{
    public static final SchemaTableName TASK_TABLE_NAME = new SchemaTableName("runtime", "tasks");

    public static final ConnectorTableMetadata TASK_TABLE = tableMetadataBuilder(TASK_TABLE_NAME)
            .column("node_id", createUnboundedVarcharType())

            .column("task_id", createUnboundedVarcharType())
            .column("stage_execution_id", createUnboundedVarcharType())
            .column("stage_id", createUnboundedVarcharType())
            .column("query_id", createUnboundedVarcharType())
            .column("state", createUnboundedVarcharType())

            .column("splits", BIGINT)
            .column("queued_splits", BIGINT)
            .column("running_splits", BIGINT)
            .column("completed_splits", BIGINT)

            .column("split_scheduled_time_ms", BIGINT)
            .column("split_cpu_time_ms", BIGINT)
            .column("split_blocked_time_ms", BIGINT)

            .column("raw_input_bytes", BIGINT)
            .column("raw_input_rows", BIGINT)

            .column("processed_input_bytes", BIGINT)
            .column("processed_input_rows", BIGINT)

            .column("output_bytes", BIGINT)
            .column("output_rows", BIGINT)

            .column("physical_written_bytes", BIGINT)

            .column("created", TIMESTAMP)
            .column("start", TIMESTAMP)
            .column("last_heartbeat", TIMESTAMP)
            .column("end", TIMESTAMP)
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
    public Distribution getDistribution()
    {
        return ALL_NODES;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return TASK_TABLE;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        Builder table = InMemoryRecordSet.builder(TASK_TABLE);
        for (TaskInfo taskInfo : taskManager.getAllTaskInfo()) {
            TaskStats stats = taskInfo.getStats();
            TaskStatus taskStatus = taskInfo.getTaskStatus();
            table.addRow(
                    nodeId,

                    taskInfo.getTaskId().toString(),
                    taskInfo.getTaskId().getStageExecutionId().toString(),
                    taskInfo.getTaskId().getStageExecutionId().getStageId().toString(),
                    taskInfo.getTaskId().getQueryId().toString(),
                    taskStatus.getState().toString(),

                    (long) stats.getTotalDrivers(),
                    (long) stats.getQueuedDrivers(),
                    (long) stats.getRunningDrivers(),
                    (long) stats.getCompletedDrivers(),

                    NANOSECONDS.toMillis(stats.getTotalScheduledTimeInNanos()),
                    NANOSECONDS.toMillis(stats.getTotalCpuTimeInNanos()),
                    NANOSECONDS.toMillis(stats.getTotalBlockedTimeInNanos()),

                    stats.getRawInputDataSizeInBytes(),
                    stats.getRawInputPositions(),

                    stats.getProcessedInputDataSizeInBytes(),
                    stats.getProcessedInputPositions(),

                    stats.getOutputDataSizeInBytes(),
                    stats.getOutputPositions(),

                    stats.getPhysicalWrittenDataSizeInBytes(),

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
