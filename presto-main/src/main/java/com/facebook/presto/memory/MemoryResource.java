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
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;

import static com.facebook.presto.PrestoMediaTypes.APPLICATION_JACKSON_SMILE;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.RESERVED_POOL;
import static com.facebook.presto.server.security.RoleType.INTERNAL;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;
import static java.util.Objects.requireNonNull;

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
