package com.facebook.presto.connector.system;

import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.metadata.InMemoryRecordSet;
import com.facebook.presto.metadata.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableMetadata;
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

    public static final TableMetadata TASK_TABLE = tableMetadataBuilder(TASK_TABLE_NAME)
            .column("node_id", STRING)

            .column("task_id", STRING)
            .column("stage_id", STRING)
            .column("query_id", STRING)
            .column("state", STRING)

            .column("splits", LONG)
            .column("queued_splits", LONG)
            .column("started_splits", LONG)
            .column("running_splits", LONG)
            .column("completed_splits", LONG)

            .column("split_wall_time_ms", LONG)
            .column("split_cpu_time_ms", LONG)
            .column("split_user_time_ms", LONG)

            .column("sink_buffer_wait_time", LONG)
            .column("exchange_wait_time", LONG)

            .column("input_bytes", LONG)
            .column("input_rows", LONG)

            .column("completed_bytes", LONG)
            .column("completed_rows", LONG)

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
    public TableMetadata getTableMetadata()
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
            table.addRow(
                    nodeId,

                    taskInfo.getTaskId().toString(),
                    taskInfo.getTaskId().getStageId().toString(),
                    taskInfo.getTaskId().getQueryId().toString(),
                    taskInfo.getState().toString(),

                    (long) taskInfo.getStats().getSplits(),
                    (long) taskInfo.getStats().getQueuedSplits(),
                    (long) taskInfo.getStats().getStartedSplits(),
                    (long) taskInfo.getStats().getRunningSplits(),
                    (long) taskInfo.getStats().getCompletedSplits(),

                    toMillis(taskInfo.getStats().getSplitWallTime()),
                    toMillis(taskInfo.getStats().getSplitCpuTime()),
                    toMillis(taskInfo.getStats().getSplitUserTime()),

                    toMillis(taskInfo.getStats().getSinkBufferWaitTime()),
                    toMillis(taskInfo.getStats().getExchangeWaitTime()),

                    toBytes(taskInfo.getStats().getInputDataSize()),
                    taskInfo.getStats().getInputPositionCount(),

                    toBytes(taskInfo.getStats().getCompletedDataSize()),
                    taskInfo.getStats().getCompletedPositionCount(),

                    toBytes(taskInfo.getStats().getOutputDataSize()),
                    taskInfo.getStats().getOutputPositionCount(),

                    toTimeStamp(taskInfo.getStats().getCreateTime()),
                    toTimeStamp(taskInfo.getStats().getExecutionStartTime()),
                    toTimeStamp(taskInfo.getStats().getLastHeartbeat()),
                    toTimeStamp(taskInfo.getStats().getEndTime()));
        }
        return table.build().cursor();
    }

    private Long toMillis(Duration duration)
    {
        if (duration == null) {
            return null;
        }
        return (long) duration.toMillis();
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
