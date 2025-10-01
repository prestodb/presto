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
package com.facebook.presto.server.remotetask;

import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.server.TaskUpdateRequest;
import com.google.common.util.concurrent.ListenableFuture;

// TaskResult is in the same package, so no import needed

/**
 * Thrift service interface for Presto task management.
 * This interface corresponds to the PrestoThrift service defined in the C++ implementation.
 */
@ThriftService(value = "PrestoThrift", idlName = "PrestoThrift")
public interface PrestoThriftService
{
    /**
     * Placeholder method for compatibility.
     */
    @ThriftMethod("fake")
    void fake();

    /**
     * Create or update task - corresponds to POST /v1/task/{taskId}
     * @param taskId The ID of the task to create or update
     * @param taskUpdateRequest The task update request containing session info, plan fragment, etc.
     * @return TaskInfo containing the task information
     */
    @ThriftMethod("createOrUpdateTask")
    ListenableFuture<TaskInfo> createOrUpdateTask(
            @ThriftField(name = "taskId") String taskId,
            @ThriftField(name = "taskUpdateRequest") TaskUpdateRequest taskUpdateRequest);

    /**
     * Get task status - corresponds to GET /v1/task/{taskId}/status
     * @param taskId The ID of the task to get status for
     * @return TaskStatus containing the current task status
     */
    @ThriftMethod("getTaskStatus")
    ListenableFuture<TaskStatus> getTaskStatus(@ThriftField(name = "taskId") String taskId);

    /**
     * Get task info - corresponds to GET /v1/task/{taskId}
     * @param taskId The ID of the task to get info for
     * @param summarizeTaskInfo Whether to return summarized task info
     * @return TaskInfo containing the complete task information
     */
    @ThriftMethod("getTaskInfo")
    ListenableFuture<TaskInfo> getTaskInfo(
            @ThriftField(name = "taskId") String taskId,
            @ThriftField(name = "summarizeTaskInfo") boolean summarizeTaskInfo);
}
