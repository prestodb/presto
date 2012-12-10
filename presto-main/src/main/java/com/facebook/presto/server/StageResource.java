/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.StageManager;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.List;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manage queries scheduled on this node
 */
@Path("/v1/stage")
public class StageResource
{
    private final StageManager stageManager;

    @Inject
    public StageResource(StageManager stageManager)
    {
        this.stageManager = checkNotNull(stageManager, "stageManager is null");
    }

    @GET
    public List<StageInfo> getAllStages()
            throws InterruptedException
    {
        return stageManager.getAllStage();
    }

    @GET
    @Path("{stageId}")
    public Response getStage(@PathParam("stageId") String stageId)
            throws InterruptedException
    {
        checkNotNull(stageId, "stageId is null");

        try {
            StageInfo stage = stageManager.getStage(stageId);
            return Response.ok(stage).build();
        }
        catch (NoSuchElementException e) {
            return Response.status(Status.GONE).build();
        }
    }

    @DELETE
    @Path("{stageId}")
    public void cancelStage(@PathParam("stageId") String stageId)
    {
        checkNotNull(stageId, "stageId is null");
        stageManager.cancelStage(stageId);
    }
}
