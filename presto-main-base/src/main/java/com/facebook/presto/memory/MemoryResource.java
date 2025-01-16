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
import com.facebook.presto.spi.memory.MemoryPoolInfo;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import static com.facebook.presto.PrestoMediaTypes.APPLICATION_JACKSON_SMILE;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.RESERVED_POOL;
import static com.facebook.presto.server.security.RoleType.INTERNAL;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

/**
 * Manages memory pools on this worker node
 */
@Path("/v1/memory")
@RolesAllowed(INTERNAL)
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
    @Produces({APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    @Consumes({APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    public MemoryInfo getMemoryInfo(MemoryPoolAssignmentsRequest request)
    {
        taskManager.updateMemoryPoolAssignments(request);
        return memoryManager.getInfo();
    }

    @GET
    @Path("{poolId}")
    @Produces({APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    @Consumes({APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    public Response getMemoryInfo(@PathParam("poolId") String poolId)
    {
        if (GENERAL_POOL.getId().equals(poolId)) {
            return toSuccessfulResponse(memoryManager.getGeneralPool().getInfo());
        }

        if (RESERVED_POOL.getId().equals(poolId) && memoryManager.getReservedPool().isPresent()) {
            return toSuccessfulResponse(memoryManager.getReservedPool().get().getInfo());
        }

        return Response.status(NOT_FOUND).build();
    }

    private Response toSuccessfulResponse(MemoryPoolInfo memoryInfo)
    {
        return Response.ok()
                .entity(memoryInfo)
                .build();
    }
}
