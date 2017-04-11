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
package com.facebook.presto.raptor.backup;

import io.airlift.node.NodeInfo;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.facebook.presto.raptor.backup.HttpBackupStore.CONTENT_XXH64;
import static com.facebook.presto.raptor.backup.HttpBackupStore.PRESTO_ENVIRONMENT;
import static java.lang.Long.parseUnsignedLong;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.GONE;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/")
public class TestingHttpBackupResource
{
    private final String environment;

    @GuardedBy("this")
    private final Map<UUID, byte[]> shards = new HashMap<>();

    @Inject
    public TestingHttpBackupResource(NodeInfo nodeInfo)
    {
        this(nodeInfo.getEnvironment());
    }

    public TestingHttpBackupResource(String environment)
    {
        this.environment = requireNonNull(environment, "environment is null");
    }

    @HEAD
    @Path("{uuid}")
    public synchronized Response headRequest(
            @HeaderParam(PRESTO_ENVIRONMENT) String environment,
            @PathParam("uuid") UUID uuid)
    {
        checkEnvironment(environment);
        if (!shards.containsKey(uuid)) {
            return Response.status(NOT_FOUND).build();
        }
        if (shards.get(uuid) == null) {
            return Response.status(GONE).build();
        }
        return Response.noContent().build();
    }

    @GET
    @Path("{uuid}")
    @Produces(APPLICATION_OCTET_STREAM)
    public synchronized Response getRequest(
            @HeaderParam(PRESTO_ENVIRONMENT) String environment,
            @PathParam("uuid") UUID uuid)
    {
        checkEnvironment(environment);
        if (!shards.containsKey(uuid)) {
            return Response.status(NOT_FOUND).build();
        }
        byte[] bytes = shards.get(uuid);
        if (bytes == null) {
            return Response.status(GONE).build();
        }
        return Response.ok(bytes).build();
    }

    @PUT
    @Path("{uuid}")
    public synchronized Response putRequest(
            @HeaderParam(PRESTO_ENVIRONMENT) String environment,
            @HeaderParam(CONTENT_XXH64) String hexHash,
            @Context HttpServletRequest request,
            @PathParam("uuid") UUID uuid,
            byte[] bytes)
    {
        checkEnvironment(environment);
        if ((request.getContentLength() < 0) || (bytes.length != request.getContentLength())) {
            return Response.status(BAD_REQUEST).build();
        }
        if (parseUnsignedLong(hexHash, 16) != XxHash64.hash(Slices.wrappedBuffer(bytes))) {
            return Response.status(BAD_REQUEST).build();
        }
        if (shards.containsKey(uuid)) {
            byte[] existing = shards.get(uuid);
            if ((existing == null) || !Arrays.equals(bytes, existing)) {
                return Response.status(FORBIDDEN).build();
            }
        }
        shards.put(uuid, bytes);
        return Response.noContent().build();
    }

    @DELETE
    @Path("{uuid}")
    public synchronized Response deleteRequest(
            @HeaderParam(PRESTO_ENVIRONMENT) String environment,
            @PathParam("uuid") UUID uuid)
    {
        checkEnvironment(environment);
        if (!shards.containsKey(uuid)) {
            return Response.status(NOT_FOUND).build();
        }
        if (shards.get(uuid) == null) {
            return Response.status(GONE).build();
        }
        shards.put(uuid, null);
        return Response.noContent().build();
    }

    private void checkEnvironment(String environment)
    {
        if (!this.environment.equals(environment)) {
            throw new WebApplicationException(Response.status(FORBIDDEN).build());
        }
    }
}
