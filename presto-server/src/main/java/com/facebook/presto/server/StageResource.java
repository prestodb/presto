/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.StageId;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import static com.google.common.base.Preconditions.checkNotNull;

@Path("/v1/stage")
public class StageResource
{
    private final QueryManager queryManager;

    @Inject
    public StageResource(QueryManager queryManager)
    {
        this.queryManager = checkNotNull(queryManager, "queryManager is null");
    }

    @DELETE
    @Path("{stageId}")
    public void cancelStage(@PathParam("stageId") StageId stageId)
    {
        checkNotNull(stageId, "stageId is null");
        queryManager.cancelStage(stageId);
    }
}
