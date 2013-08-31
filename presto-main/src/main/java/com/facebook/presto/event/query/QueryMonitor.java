package com.facebook.presto.event.query;

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.noperator.DriverStats;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.airlift.event.client.EventClient;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class QueryMonitor
{
    private static final Logger log = Logger.get(QueryMonitor.class);

    private final ObjectMapper objectMapper;
    private final EventClient eventClient;
    private final String environment;

    @Inject
    public QueryMonitor(ObjectMapper objectMapper, EventClient eventClient, NodeInfo nodeInfo)
    {
        this.objectMapper = checkNotNull(objectMapper, "objectMapper is null");
        this.eventClient = checkNotNull(eventClient, "eventClient is null");
        this.environment = checkNotNull(nodeInfo, "nodeInfo is null").getEnvironment();
    }

    public void createdEvent(QueryInfo queryInfo)
    {
        eventClient.post(
                new QueryCreatedEvent(
                        queryInfo.getQueryId(),
                        queryInfo.getSession().getUser(),
                        queryInfo.getSession().getSource(),
                        environment,
                        queryInfo.getSession().getCatalog(),
                        queryInfo.getSession().getSchema(),
                        queryInfo.getSession().getRemoteUserAddress(),
                        queryInfo.getSession().getUserAgent(),
                        queryInfo.getSelf(),
                        queryInfo.getQuery(),
                        queryInfo.getQueryStats().getCreateTime()
                )
        );
    }

    public void completionEvent(QueryInfo queryInfo)
    {
        try {
            QueryStats queryStats = queryInfo.getQueryStats();
            eventClient.post(
                    new QueryCompletionEvent(
                            queryInfo.getQueryId(),
                            queryInfo.getSession().getUser(),
                            queryInfo.getSession().getSource(),
                            environment,
                            queryInfo.getSession().getCatalog(),
                            queryInfo.getSession().getSchema(),
                            queryInfo.getSession().getRemoteUserAddress(),
                            queryInfo.getSession().getUserAgent(),
                            queryInfo.getState(),
                            queryInfo.getSelf(),
                            queryInfo.getFieldNames(),
                            queryInfo.getQuery(),
                            queryStats.getCreateTime(),
                            queryStats.getExecutionStartTime(),
                            queryStats.getEndTime(),
                            queryStats.getQueuedTime(),
                            queryStats.getAnalysisTime(),
                            queryStats.getDistributedPlanningTime(),
                            queryStats.getTotalScheduledTime(),
                            queryStats.getTotalCpuTime(),
                            queryStats.getInputDataSize(),
                            queryStats.getInputPositions(),
                            queryStats.getTotalDrivers(),
                            objectMapper.writeValueAsString(queryInfo.getOutputStage()),
                            objectMapper.writeValueAsString(queryInfo.getFailureInfo())
                    )
            );
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }

    public void splitCompletionEvent(TaskId taskId, DriverStats driverStats)
    {
        Duration timeToStart = null;
        if (driverStats.getStartTime() != null) {
            timeToStart = new Duration(driverStats.getStartTime().getMillis() - driverStats.getCreateTime().getMillis(), MILLISECONDS);
        }
        Duration timeToEnd = null;
        if (driverStats.getEndTime() != null) {
            timeToEnd = new Duration(driverStats.getEndTime().getMillis() - driverStats.getCreateTime().getMillis(), MILLISECONDS);
        }

        try {
            eventClient.post(
                    new SplitCompletionEvent(
                            taskId.getQueryId(),
                            taskId.getStageId(),
                            taskId,
                            environment,
                            driverStats.getQueuedTime(),
                            driverStats.getStartTime(),
                            timeToStart,
                            timeToEnd,
                            driverStats.getInputDataSize(),
                            driverStats.getInputPositions(),
                            driverStats.getElapsedTime(),
                            driverStats.getTotalCpuTime(),
                            driverStats.getTotalUserTime(),
                            objectMapper.writeValueAsString(driverStats)
                    )
            );
        }
        catch (JsonProcessingException e) {
            log.error(e, "Error posting split completion event for task %s", taskId);
        }
    }
}
