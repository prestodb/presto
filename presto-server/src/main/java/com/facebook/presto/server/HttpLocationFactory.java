/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.metadata.Node;
import com.google.common.base.Preconditions;
import io.airlift.http.server.HttpServerInfo;

import javax.inject.Inject;
import java.net.URI;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;

public class HttpLocationFactory
        implements LocationFactory
{
    private final URI baseUri;

    @Inject
    public HttpLocationFactory(HttpServerInfo httpServerInfo)
    {
        this(httpServerInfo.getHttpUri());
    }

    public HttpLocationFactory(URI baseUri)
    {
        this.baseUri = baseUri;
    }

    @Override

    public URI createQueryLocation(QueryId queryId)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        return uriBuilderFrom(baseUri)
                .appendPath("/v1/query")
                .appendPath(queryId.toString())
                .build();
    }

    @Override
    public URI createStageLocation(StageId stageId)
    {
        Preconditions.checkNotNull(stageId, "stageId is null");
        return uriBuilderFrom(baseUri)
                .appendPath("v1/stage")
                .appendPath(stageId.toString())
                .build();
    }

    @Override
    public URI createTaskLocation(Node node, TaskId taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        return uriBuilderFrom(node.getHttpUri())
                .appendPath("/v1/task")
                .appendPath(taskId.toString())
                .build();
    }
}
