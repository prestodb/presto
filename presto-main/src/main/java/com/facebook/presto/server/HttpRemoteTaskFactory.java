/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.execution.ExchangePlanFragmentSource;
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
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

public class HttpRemoteTaskFactory
        implements RemoteTaskFactory
{
    private final HttpClient httpClient;
    private final LocationFactory locationFactory;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<QueryFragmentRequest> queryFragmentRequestCodec;
    private final JsonCodec<Split> splitCodec;

    @Inject
    public HttpRemoteTaskFactory(@ForScheduler HttpClient httpClient,
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
            Map<PlanNodeId, ExchangePlanFragmentSource> exchangeSources,
            List<String> outputIds)
    {
        return new HttpRemoteTask(session,
                queryId,
                stageId,
                taskId,
                locationFactory.createTaskLocation(node, taskId),
                fragment,
                exchangeSources,
                outputIds,
                httpClient,
                taskInfoCodec,
                queryFragmentRequestCodec,
                splitCodec);
    }
}
