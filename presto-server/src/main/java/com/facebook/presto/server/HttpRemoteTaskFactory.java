/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.split.Split;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.OutputReceiver;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.Multimap;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import javax.inject.Inject;
import java.net.URI;
import java.util.Map;
import java.util.Set;

public class HttpRemoteTaskFactory
        implements RemoteTaskFactory
{
    private final AsyncHttpClient httpClient;
    private final LocationFactory locationFactory;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final int maxConsecutiveErrorCount;
    private final Duration minErrorDuration;

    @Inject
    public HttpRemoteTaskFactory(QueryManagerConfig config,
            @ForScheduler AsyncHttpClient httpClient,
            LocationFactory locationFactory,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec)
    {
        this.httpClient = httpClient;
        this.locationFactory = locationFactory;
        this.taskInfoCodec = taskInfoCodec;
        this.taskUpdateRequestCodec = taskUpdateRequestCodec;
        this.maxConsecutiveErrorCount = config.getRemoteTaskMaxConsecutiveErrorCount();
        this.minErrorDuration = config.getRemoteTaskMinErrorDuration();
    }

    @Override
    public RemoteTask createRemoteTask(Session session,
            TaskId taskId,
            Node node,
            PlanFragment fragment,
            Split initialSplit,
            Map<PlanNodeId, OutputReceiver> outputReceivers,
            Multimap<PlanNodeId, URI> initialExchangeLocations,
            Set<String> initialOutputIds)
    {
        return new HttpRemoteTask(session,
                taskId,
                node,
                locationFactory.createTaskLocation(node, taskId),
                fragment,
                initialSplit,
                outputReceivers,
                initialExchangeLocations,
                initialOutputIds,
                httpClient,
                maxConsecutiveErrorCount,
                minErrorDuration,
                taskInfoCodec,
                taskUpdateRequestCodec
        );
    }
}
