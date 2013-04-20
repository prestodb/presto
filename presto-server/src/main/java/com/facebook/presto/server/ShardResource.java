package com.facebook.presto.server;

import com.facebook.presto.metadata.LocalStorageManager;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

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
    @Path("{shardId: \\d+}")
    public Response dropShard(@PathParam("shardId") long shardId)
    {
        storageManager.dropShard(shardId);
        return Response.status(Status.ACCEPTED).build();
    }

    @GET
    @Path("{shardId: \\d+}")
    public Response shardStatus(@PathParam("shardId") long shardId)
    {
        if (storageManager.isShardActive(shardId)) {
            return Response.status(Status.ACCEPTED).build();
        }

        if (storageManager.shardExists(shardId)) {
            return Response.ok().build();
        }

        return Response.status(Status.NOT_FOUND).build();
    }
}
