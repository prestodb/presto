package com.facebook.presto.server.thrift;

import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;
import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.server.InternalCommunicationConfig;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.server.smile.SmileCodec;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import static com.facebook.presto.server.smile.JsonCodecWrapper.wrapJsonCodec;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

// The RPC server for task on worker. The endpoint should be similar to those in TaskResource
@ThriftService
public class TaskThriftService
{
    private final TaskManager taskManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestJsonCodec;
    private final JsonCodec<TaskInfo> taskInfoJsonCodec;

    @Inject
    public TaskThriftService(
            TaskManager taskManager,
            SessionPropertyManager sessionPropertyManager,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestJsonCodec,
            JsonCodec<TaskInfo> taskInfoJsonCodec,
            InternalCommunicationConfig communicationConfig)
    {
        checkArgument(!requireNonNull(communicationConfig, "communicationConfig is null").isBinaryTransportEnabled());

        this.taskManager = requireNonNull(taskManager);
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager);
        this.taskUpdateRequestJsonCodec = requireNonNull(taskUpdateRequestJsonCodec);
        this.taskInfoJsonCodec = requireNonNull(taskInfoJsonCodec);
    }

    // for now directly communicate with serialized bytes
    @ThriftMethod
    public byte[] createOrUpdateTask(String taskId, byte[] serializedTaskUpdateRequest, boolean summarizeTaskInfo) {
        TaskUpdateRequest taskUpdateRequest = taskUpdateRequestJsonCodec.fromJson(serializedTaskUpdateRequest);

        Session session = taskUpdateRequest.getSession().toSession(sessionPropertyManager, taskUpdateRequest.getExtraCredentials());
        TaskInfo taskInfo = taskManager.updateTask(session,
                TaskId.valueOf(taskId),
                taskUpdateRequest.getFragment(),
                taskUpdateRequest.getSources(),
                taskUpdateRequest.getOutputIds(),
                taskUpdateRequest.getTotalPartitions());

        if (summarizeTaskInfo) {
            taskInfo = taskInfo.summarize();
        }

        return taskInfoJsonCodec.toJsonBytes(taskInfo);
    }
}
