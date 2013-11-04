/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.event.query;

import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.operator.DriverStats;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.airlift.event.client.EventClient;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;

import javax.annotation.Nullable;

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
            FailureInfo failureInfo = queryInfo.getFailureInfo();

            String failureType = failureInfo == null ? null : failureInfo.getType();
            String failureMessage = failureInfo == null ? null : failureInfo.getMessage();

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
                            queryStats.getRawInputDataSize(),
                            queryStats.getRawInputPositions(),
                            queryStats.getTotalDrivers(),
                            failureType,
                            failureMessage,
                            objectMapper.writeValueAsString(queryInfo.getOutputStage()),
                            objectMapper.writeValueAsString(queryInfo.getFailureInfo()),
                            objectMapper.writeValueAsString(queryInfo.getInputs())
                    )
            );
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }

    public void splitCompletionEvent(TaskId taskId, DriverStats driverStats)
    {
        splitCompletionEvent(taskId, driverStats, null, null);
    }

    public void splitFailedEvent(TaskId taskId, DriverStats driverStats, Throwable cause)
    {
        splitCompletionEvent(taskId, driverStats, cause.getClass().getName(), cause.getMessage());
    }

    private void splitCompletionEvent(TaskId taskId, DriverStats driverStats, @Nullable String failureType, @Nullable String failureMessage)
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
                            driverStats.getRawInputDataSize(),
                            driverStats.getRawInputPositions(),
                            driverStats.getElapsedTime(),
                            driverStats.getTotalCpuTime(),
                            driverStats.getTotalUserTime(),
                            failureType,
                            failureMessage,
                            objectMapper.writeValueAsString(driverStats)
                    )
            );
        }
        catch (JsonProcessingException e) {
            log.error(e, "Error posting split completion event for task %s", taskId);
        }
    }
}
