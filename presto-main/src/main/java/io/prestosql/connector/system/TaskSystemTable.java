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

import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.execution.TaskInfo;
import io.prestosql.execution.TaskManager;
import io.prestosql.execution.TaskStatus;
import io.prestosql.operator.TaskStats;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.InMemoryRecordSet;
import io.prestosql.spi.connector.InMemoryRecordSet.Builder;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;
import org.joda.time.DateTime;

import javax.inject.Inject;

import static io.prestosql.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.prestosql.spi.connector.SystemTable.Distribution.ALL_NODES;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;

public class TaskSystemTable
        implements SystemTable
{
    public static final SchemaTableName TASK_TABLE_NAME = new SchemaTableName("runtime", "tasks");

    public static final ConnectorTableMetadata TASK_TABLE = tableMetadataBuilder(TASK_TABLE_NAME)
            .column("node_id", createUnboundedVarcharType())

            .column("task_id", createUnboundedVarcharType())
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

                    taskStatus.getTaskId().toString(),
                    taskStatus.getTaskId().getStageId().toString(),
                    taskStatus.getTaskId().getQueryId().toString(),
                    taskStatus.getState().toString(),

                    (long) stats.getTotalDrivers(),
                    (long) stats.getQueuedDrivers(),
                    (long) stats.getRunningDrivers(),
                    (long) stats.getCompletedDrivers(),

                    toMillis(stats.getTotalScheduledTime()),
                    toMillis(stats.getTotalCpuTime()),
                    toMillis(stats.getTotalBlockedTime()),

                    toBytes(stats.getRawInputDataSize()),
                    stats.getRawInputPositions(),

                    toBytes(stats.getProcessedInputDataSize()),
                    stats.getProcessedInputPositions(),

                    toBytes(stats.getOutputDataSize()),
                    stats.getOutputPositions(),

                    toBytes(stats.getPhysicalWrittenDataSize()),

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
