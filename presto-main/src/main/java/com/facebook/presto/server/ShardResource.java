package com.facebook.presto.server;

import com.facebook.presto.importer.ShardImporter;
import com.facebook.presto.metadata.StorageManager;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
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
    private final ShardImporter shardImporter;
    private final StorageManager storageManager;

    @Inject
    public ShardResource(ShardImporter shardImporter, StorageManager storageManager)
    {
        this.shardImporter = checkNotNull(shardImporter, "shardImporter is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
    }

    @PUT
    @Path("{shardId: \\d+}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createShard(@PathParam("shardId") long shardId, ShardImport shardImport)
    {
        shardImporter.importShard(shardId, shardImport);

        return Response.status(Status.ACCEPTED).build();
    }

    @GET
    @Path("{shardId: \\d+}")
    public Response shardStatus(@PathParam("shardId") long shardId)
    {
        if (shardImporter.isShardImporting(shardId)) {
            return Response.status(Status.ACCEPTED).build();
        }

        if (storageManager.shardExists(shardId)) {
            return Response.ok().build();
        }

        return Response.status(Status.NOT_FOUND).build();
    }
}
