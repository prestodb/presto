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

import com.facebook.presto.metadata.LocalStorageManager;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

@Path("/v1/shard")
public class ShardResource
{
    private final LocalStorageManager storageManager;

    @Inject
    public ShardResource(LocalStorageManager storageManager)
    {
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
    }

    @DELETE
    @Path("{shardUuid}")
    public Response dropShard(@PathParam("shardUuid") UUID shardUuid)
    {
        storageManager.dropShard(shardUuid);
        return Response.status(Status.ACCEPTED).build();
    }

    @GET
    @Path("{shardUuid}")
    public Response shardStatus(@PathParam("shardUuid") UUID shardUuid)
    {
        if (storageManager.isShardActive(shardUuid)) {
            return Response.status(Status.ACCEPTED).build();
        }

        if (storageManager.shardExists(shardUuid)) {
            return Response.ok().build();
        }

        return Response.status(Status.NOT_FOUND).build();
    }
}
