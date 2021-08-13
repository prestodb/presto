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
package com.facebook.presto.server;

import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.spi.QueryId;
import com.google.inject.Inject;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Path("v1/taskInfo")
@RolesAllowed("ADMIN")
public class TaskInfoResource
{
    private final QueryManager queryManager;

    @Inject
    public TaskInfoResource(QueryManager queryManager)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
    }

    @GET
    @Path("{taskId}")
    public TaskInfo getTaskInfo(@PathParam("taskId") TaskId taskId)
            throws NotFoundException
    {
        QueryId queryId = taskId.getQueryId();
        try {
            Optional<StageInfo> stageInfo = queryManager.getFullQueryInfo(queryId).getOutputStage();

            if (stageInfo.isPresent()) {
                Optional<StageInfo> stage = stageInfo.get().getStageWithStageId(taskId.getStageExecutionId().getStageId());
                if (stage.isPresent()) {
                    Optional<TaskInfo> taskInfo = stage.get().getLatestAttemptExecutionInfo().getTasks().stream()
                            .filter(info -> info.getTaskId().equals(taskId))
                            .findFirst();

                    if (taskInfo.isPresent()) {
                        return taskInfo.get();
                    }
                }
            }
            throw new NotFoundException("TaskInfo not found for task id " + taskId.toString());
        }
        catch (Exception e) {
            throw new NotFoundException(e);
        }
    }
}
