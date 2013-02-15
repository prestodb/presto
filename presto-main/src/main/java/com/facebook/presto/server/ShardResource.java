package com.facebook.presto.server;

import com.facebook.presto.importer.LocalShardManager;
import com.facebook.presto.importer.ShardImport;
import com.facebook.presto.metadata.StorageManager;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.ws.rs.core.Response.Status;

@Path("/v1/shard")
public class ShardResource
{
    private final LocalShardManager localShardManager;
    private final StorageManager storageManager;

    @Inject
    public ShardResource(LocalShardManager localShardManager, StorageManager storageManager)
    {
        this.localShardManager = checkNotNull(localShardManager, "shardImporter is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
    }

    @PUT
    @Path("{shardId: \\d+}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createShard(@PathParam("shardId") long shardId, ShardImport shardImport)
    {
        localShardManager.importShard(shardId, shardImport);

        return Response.status(Status.ACCEPTED).build();
    }

    @DELETE
    @Path("{shardId: \\d+}")
    public Response dropShard(@PathParam("shardId") long shardId)
    {
        localShardManager.dropShard(shardId);

        return Response.status(Status.ACCEPTED).build();
    }

    @GET
    @Path("{shardId: \\d+}")
    public Response shardStatus(@PathParam("shardId") long shardId)
    {
        if (localShardManager.isShardActive(shardId)) {
            return Response.status(Status.ACCEPTED).build();
        }

        if (storageManager.shardExists(shardId)) {
            return Response.ok().build();
        }

        return Response.status(Status.NOT_FOUND).build();
    }
}
