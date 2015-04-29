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
package com.facebook.presto.memory;

import com.facebook.presto.execution.TaskManager;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import static java.util.Objects.requireNonNull;

/**
 * Manages memory pools on this worker node
 */
@Path("/v1/memory")
public class MemoryResource
{
    private final LocalMemoryManager memoryManager;
    private final TaskManager taskManager;

    @Inject
    public MemoryResource(LocalMemoryManager memoryManager, TaskManager taskManager)
    {
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.taskManager = requireNonNull(taskManager, "taskManager is null");
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public MemoryInfo getMemoryInfo(MemoryPoolAssignmentsRequest request)
    {
        taskManager.updateMemoryPoolAssignments(request);
        return memoryManager.getInfo();
    }
}
