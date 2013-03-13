/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.split.Split;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.Multimap;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import java.net.URI;
import java.util.Set;

public class HttpRemoteTaskFactory
        implements RemoteTaskFactory
{
    private final AsyncHttpClient httpClient;
    private final LocationFactory locationFactory;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<QueryFragmentRequest> queryFragmentRequestCodec;
    private final JsonCodec<Split> splitCodec;

    @Inject
    public HttpRemoteTaskFactory(@ForScheduler AsyncHttpClient httpClient,
            LocationFactory locationFactory,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<QueryFragmentRequest> queryFragmentRequestCodec,
            JsonCodec<Split> splitCodec)
    {
        this.httpClient = httpClient;
        this.locationFactory = locationFactory;
        this.taskInfoCodec = taskInfoCodec;
        this.queryFragmentRequestCodec = queryFragmentRequestCodec;
        this.splitCodec = splitCodec;
    }

    @Override
    public RemoteTask createRemoteTask(Session session,
            String queryId,
            String stageId,
            String taskId,
            Node node,
            PlanFragment fragment,
            Multimap<PlanNodeId, URI> initialExchangeLocations,
            Set<String> initialOutputIds)
    {
        return new HttpRemoteTask(session,
                queryId,
                stageId,
                taskId,
                node,
                locationFactory.createTaskLocation(node, taskId),
                fragment,
                initialExchangeLocations,
                initialOutputIds,
                httpClient,
                taskInfoCodec,
                queryFragmentRequestCodec,
                splitCodec);
    }
}
