/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

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

    public URI createQueryLocation(String queryId)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        return uriBuilderFrom(baseUri)
                .appendPath("/v1/query")
                .appendPath(queryId)
                .build();
    }

    @Override
    public URI createStageLocation(String queryId, String stageId)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        return uriBuilderFrom(baseUri)
                .appendPath("v1/query")
                .appendPath(queryId)
                .appendPath("stage")
                .appendPath(stageId)
                .build();
    }

    @Override
    public URI createTaskLocation(Node node, String taskId)
    {
        Preconditions.checkNotNull(taskId, "taskId is null");
        return uriBuilderFrom(node.getHttpUri())
                .appendPath("/v1/task")
                .appendPath(taskId)
                .build();
    }
}
