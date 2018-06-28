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
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.Column;
import com.facebook.presto.execution.Input;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.DriverStats;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.TableFinishInfo;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
import com.facebook.presto.spi.eventlistener.QueryInputMetadata;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryOutputMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.facebook.presto.spi.eventlistener.SplitFailureInfo;
import com.facebook.presto.spi.eventlistener.SplitStatistics;
import com.facebook.presto.spi.eventlistener.StageCpuDistribution;
import com.facebook.presto.transaction.TransactionId;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.stats.Distribution;
import io.airlift.stats.Distribution.DistributionSnapshot;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.cost.PlanNodeCostEstimate.UNKNOWN_COST;
import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.textDistributedPlan;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.time.Duration.ofMillis;
import static java.time.Instant.ofEpochMilli;
import static java.util.Objects.requireNonNull;

public class QueryMonitor
{
    private static final Logger log = Logger.get(QueryMonitor.class);

    private final JsonCodec<StageInfo> stageInfoCodec;
    private final ObjectMapper objectMapper;
    private final EventListenerManager eventListenerManager;
    private final String serverVersion;
    private final String serverAddress;
    private final String environment;
    private final SessionPropertyManager sessionPropertyManager;
    private final FunctionRegistry functionRegistry;
    private final int maxJsonLimit;

    @Inject
    public QueryMonitor(
            ObjectMapper objectMapper,
            JsonCodec<StageInfo> stageInfoCodec,
            EventListenerManager eventListenerManager,
            NodeInfo nodeInfo,
            NodeVersion nodeVersion,
            SessionPropertyManager sessionPropertyManager,
            Metadata metadata,
            QueryMonitorConfig config)
    {
        this.eventListenerManager = requireNonNull(eventListenerManager, "eventListenerManager is null");
        this.stageInfoCodec = requireNonNull(stageInfoCodec, "stageInfoCodec is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.serverVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        this.serverAddress = requireNonNull(nodeInfo, "nodeInfo is null").getExternalAddress();
        this.environment = requireNonNull(nodeInfo, "nodeInfo is null").getEnvironment();
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.functionRegistry = requireNonNull(metadata, "metadata is null").getFunctionRegistry();
        this.maxJsonLimit = toIntExact(requireNonNull(config, "config is null").getMaxOutputStageJsonSize().toBytes());
    }

    public void queryCreatedEvent(QueryInfo queryInfo)
    {
        eventListenerManager.queryCreated(
                new QueryCreatedEvent(
                        queryInfo.getQueryStats().getCreateTime().toDate().toInstant(),
                        new QueryContext(
                                queryInfo.getSession().getUser(),
                                queryInfo.getSession().getPrincipal(),
                                queryInfo.getSession().getRemoteUserAddress(),
                                queryInfo.getSession().getUserAgent(),
                                queryInfo.getSession().getClientInfo(),
                                queryInfo.getSession().getClientTags(),
                                queryInfo.getSession().getClientCapabilities(),
                                queryInfo.getSession().getSource(),
                                queryInfo.getSession().getCatalog(),
                                queryInfo.getSession().getSchema(),
                                queryInfo.getResourceGroupId(),
                                mergeSessionAndCatalogProperties(queryInfo),
                                queryInfo.getSession().getResourceEstimates(),
                                serverAddress,
                                serverVersion,
                                environment),
                        new QueryMetadata(
                                queryInfo.getQueryId().toString(),
                                queryInfo.getSession().getTransactionId().map(TransactionId::toString),
                                queryInfo.getQuery(),
                                queryInfo.getState().toString(),
                                queryInfo.getSelf(),
                                Optional.empty(),
                                Optional.empty())));
    }

    public void queryCompletedEvent(QueryInfo queryInfo)
    {
        try {
            Optional<QueryFailureInfo> queryFailureInfo = Optional.empty();

            if (queryInfo.getFailureInfo() != null) {
                FailureInfo failureInfo = queryInfo.getFailureInfo();
                Optional<TaskInfo> failedTask = queryInfo.getOutputStage().flatMap(QueryMonitor::findFailedTask);

                queryFailureInfo = Optional.of(new QueryFailureInfo(
                        queryInfo.getErrorCode(),
                        Optional.ofNullable(failureInfo.getType()),
                        Optional.ofNullable(failureInfo.getMessage()),
                        failedTask.map(task -> task.getTaskStatus().getTaskId().toString()),
                        failedTask.map(task -> task.getTaskStatus().getSelf().getHost()),
                        objectMapper.writeValueAsString(queryInfo.getFailureInfo())));
            }

            ImmutableList.Builder<QueryInputMetadata> inputs = ImmutableList.builder();
            for (Input input : queryInfo.getInputs()) {
                inputs.add(new QueryInputMetadata(
                        input.getConnectorId().getCatalogName(),
                        input.getSchema(),
                        input.getTable(),
                        input.getColumns().stream()
                                .map(Column::getName).collect(Collectors.toList()),
                        input.getConnectorInfo()));
            }

            QueryStats queryStats = queryInfo.getQueryStats();

            Optional<QueryOutputMetadata> output = Optional.empty();
            if (queryInfo.getOutput().isPresent()) {
                Optional<TableFinishInfo> tableFinishInfo = queryStats.getOperatorSummaries().stream()
                        .map(OperatorStats::getInfo)
                        .filter(TableFinishInfo.class::isInstance)
                        .map(TableFinishInfo.class::cast)
                        .findFirst();

                output = Optional.of(
                        new QueryOutputMetadata(
                                queryInfo.getOutput().get().getConnectorId().getCatalogName(),
                                queryInfo.getOutput().get().getSchema(),
                                queryInfo.getOutput().get().getTable(),
                                tableFinishInfo.map(TableFinishInfo::getConnectorOutputMetadata),
                                tableFinishInfo.map(TableFinishInfo::isJsonLengthLimitExceeded)));
            }

            ImmutableList.Builder<String> operatorSummaries = ImmutableList.builder();
            for (OperatorStats summary : queryInfo.getQueryStats().getOperatorSummaries()) {
                operatorSummaries.add(objectMapper.writeValueAsString(summary));
            }

            Optional<String> plan = Optional.empty();
            try {
                if (queryInfo.getOutputStage().isPresent()) {
                    // Stats and costs are suppress, since transaction is already completed
                    plan = Optional.of(textDistributedPlan(
                            queryInfo.getOutputStage().get(),
                            functionRegistry,
                            (node, sourceStats, lookup, session, types) -> UNKNOWN_STATS,
                            (node, stats, lookup, session, types) -> UNKNOWN_COST,
                            queryInfo.getSession().toSession(sessionPropertyManager),
                            false));
                }
            }
            catch (Exception e) {
                // don't fail to create event if the plan can not be created
                log.debug(e, "Error creating explain plan");
            }

            eventListenerManager.queryCompleted(
                    new QueryCompletedEvent(
                            new QueryMetadata(
                                    queryInfo.getQueryId().toString(),
                                    queryInfo.getSession().getTransactionId().map(TransactionId::toString),
                                    queryInfo.getQuery(),
                                    queryInfo.getState().toString(),
                                    queryInfo.getSelf(),
                                    plan,
                                    queryInfo.getOutputStage().flatMap(stage -> stageInfoCodec.toJsonWithLengthLimit(stage, maxJsonLimit))),
                            new QueryStatistics(
                                    ofMillis(queryStats.getTotalCpuTime().toMillis()),
                                    ofMillis(queryStats.getTotalScheduledTime().toMillis()),
                                    ofMillis(queryStats.getQueuedTime().toMillis()),
                                    Optional.ofNullable(queryStats.getAnalysisTime()).map(duration -> ofMillis(duration.toMillis())),
                                    Optional.ofNullable(queryStats.getDistributedPlanningTime()).map(duration -> ofMillis(duration.toMillis())),
                                    queryStats.getPeakUserMemoryReservation().toBytes(),
                                    queryStats.getPeakTotalMemoryReservation().toBytes(),
                                    queryStats.getPeakTaskTotalMemory().toBytes(),
                                    queryStats.getRawInputDataSize().toBytes(),
                                    queryStats.getRawInputPositions(),
                                    queryStats.getOutputDataSize().toBytes(),
                                    queryStats.getOutputPositions(),
                                    queryStats.getLogicalWrittenDataSize().toBytes(),
                                    queryStats.getWrittenPositions(),
                                    queryStats.getCumulativeUserMemory(),
                                    queryStats.getStageGcStatistics(),
                                    queryStats.getCompletedDrivers(),
                                    queryInfo.isCompleteInfo(),
                                    getCpuDistributions(queryInfo),
                                    operatorSummaries.build()),
                            new QueryContext(
                                    queryInfo.getSession().getUser(),
                                    queryInfo.getSession().getPrincipal(),
                                    queryInfo.getSession().getRemoteUserAddress(),
                                    queryInfo.getSession().getUserAgent(),
                                    queryInfo.getSession().getClientInfo(),
                                    queryInfo.getSession().getClientTags(),
                                    queryInfo.getSession().getClientCapabilities(),
                                    queryInfo.getSession().getSource(),
                                    queryInfo.getSession().getCatalog(),
                                    queryInfo.getSession().getSchema(),
                                    queryInfo.getResourceGroupId(),
                                    mergeSessionAndCatalogProperties(queryInfo),
                                    queryInfo.getSession().getResourceEstimates(),
                                    serverAddress,
                                    serverVersion,
                                    environment),
                            new QueryIOMetadata(inputs.build(), output),
                            queryFailureInfo,
                            ofEpochMilli(queryStats.getCreateTime().getMillis()),
                            ofEpochMilli(queryStats.getExecutionStartTime().getMillis()),
                            ofEpochMilli(queryStats.getEndTime().getMillis())));

            logQueryTimeline(queryInfo);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static Optional<TaskInfo> findFailedTask(StageInfo stageInfo)
    {
        for (StageInfo subStage : stageInfo.getSubStages()) {
            Optional<TaskInfo> task = findFailedTask(subStage);
            if (task.isPresent()) {
                return task;
            }
        }
        return stageInfo.getTasks().stream()
                .filter(taskInfo -> taskInfo.getTaskStatus().getState() == TaskState.FAILED)
                .findFirst();
    }

    private static Map<String, String> mergeSessionAndCatalogProperties(QueryInfo queryInfo)
    {
        ImmutableMap.Builder<String, String> mergedProperties = ImmutableMap.builder();
        mergedProperties.putAll(queryInfo.getSession().getSystemProperties());
        for (Map.Entry<ConnectorId, Map<String, String>> catalogEntry : queryInfo.getSession().getCatalogProperties().entrySet()) {
            for (Map.Entry<String, String> entry : catalogEntry.getValue().entrySet()) {
                mergedProperties.put(catalogEntry.getKey().getCatalogName() + "." + entry.getKey(), entry.getValue());
            }
        }
        return mergedProperties.build();
    }

    private static void logQueryTimeline(QueryInfo queryInfo)
    {
        try {
            QueryStats queryStats = queryInfo.getQueryStats();
            DateTime queryStartTime = queryStats.getCreateTime();
            DateTime queryEndTime = queryStats.getEndTime();

            // query didn't finish cleanly
            if (queryStartTime == null || queryEndTime == null) {
                return;
            }

            // planning duration -- start to end of planning
            long planning = queryStats.getTotalPlanningTime() == null ? 0 : queryStats.getTotalPlanningTime().toMillis();

            List<StageInfo> stages = StageInfo.getAllStages(queryInfo.getOutputStage());
            // long lastSchedulingCompletion = 0;
            long firstTaskStartTime = queryEndTime.getMillis();
            long lastTaskStartTime = queryStartTime.getMillis() + planning;
            long lastTaskEndTime = queryStartTime.getMillis() + planning;
            for (StageInfo stage : stages) {
                // only consider leaf stages
                if (!stage.getSubStages().isEmpty()) {
                    continue;
                }

                for (TaskInfo taskInfo : stage.getTasks()) {
                    TaskStats taskStats = taskInfo.getStats();

                    DateTime firstStartTime = taskStats.getFirstStartTime();
                    if (firstStartTime != null) {
                        firstTaskStartTime = Math.min(firstStartTime.getMillis(), firstTaskStartTime);
                    }

                    DateTime lastStartTime = taskStats.getLastStartTime();
                    if (lastStartTime != null) {
                        lastTaskStartTime = max(lastStartTime.getMillis(), lastTaskStartTime);
                    }

                    DateTime endTime = taskStats.getEndTime();
                    if (endTime != null) {
                        lastTaskEndTime = max(endTime.getMillis(), lastTaskEndTime);
                    }
                }
            }

            long elapsed = queryEndTime.getMillis() - queryStartTime.getMillis();
            long scheduling = firstTaskStartTime - queryStartTime.getMillis() - planning;
            long running = lastTaskEndTime - firstTaskStartTime;
            long finishing = queryEndTime.getMillis() - lastTaskEndTime;

            log.info("TIMELINE: Query %s :: Transaction:[%s] :: elapsed %sms :: planning %sms :: scheduling %sms :: running %sms :: finishing %sms :: begin %s :: end %s",
                    queryInfo.getQueryId(),
                    queryInfo.getSession().getTransactionId().map(TransactionId::toString).orElse(""),
                    max(elapsed, 0),
                    max(planning, 0),
                    max(scheduling, 0),
                    max(running, 0),
                    max(finishing, 0),
                    queryStartTime,
                    queryEndTime);
        }
        catch (Exception e) {
            log.error(e, "Error logging query timeline");
        }
    }

    public void splitCompletedEvent(TaskId taskId, DriverStats driverStats)
    {
        splitCompletedEvent(taskId, driverStats, null, null);
    }

    public void splitFailedEvent(TaskId taskId, DriverStats driverStats, Throwable cause)
    {
        splitCompletedEvent(taskId, driverStats, cause.getClass().getName(), cause.getMessage());
    }

    private void splitCompletedEvent(TaskId taskId, DriverStats driverStats, @Nullable String failureType, @Nullable String failureMessage)
    {
        Optional<Duration> timeToStart = Optional.empty();
        if (driverStats.getStartTime() != null) {
            timeToStart = Optional.of(ofMillis(driverStats.getStartTime().getMillis() - driverStats.getCreateTime().getMillis()));
        }

        Optional<Duration> timeToEnd = Optional.empty();
        if (driverStats.getEndTime() != null) {
            timeToEnd = Optional.of(ofMillis(driverStats.getEndTime().getMillis() - driverStats.getCreateTime().getMillis()));
        }

        Optional<SplitFailureInfo> splitFailureMetadata = Optional.empty();
        if (failureType != null) {
            splitFailureMetadata = Optional.of(new SplitFailureInfo(failureType, failureMessage != null ? failureMessage : ""));
        }

        try {
            eventListenerManager.splitCompleted(
                    new SplitCompletedEvent(
                            taskId.getQueryId().toString(),
                            taskId.getStageId().toString(),
                            Integer.toString(taskId.getId()),
                            driverStats.getCreateTime().toDate().toInstant(),
                            Optional.ofNullable(driverStats.getStartTime()).map(startTime -> startTime.toDate().toInstant()),
                            Optional.ofNullable(driverStats.getEndTime()).map(endTime -> endTime.toDate().toInstant()),
                            new SplitStatistics(
                                    ofMillis(driverStats.getTotalCpuTime().toMillis()),
                                    ofMillis(driverStats.getElapsedTime().toMillis()),
                                    ofMillis(driverStats.getQueuedTime().toMillis()),
                                    ofMillis(driverStats.getTotalUserTime().toMillis()),
                                    ofMillis(driverStats.getRawInputReadTime().toMillis()),
                                    driverStats.getRawInputPositions(),
                                    driverStats.getRawInputDataSize().toBytes(),
                                    timeToStart,
                                    timeToEnd),
                            splitFailureMetadata,
                            objectMapper.writeValueAsString(driverStats)));
        }
        catch (JsonProcessingException e) {
            log.error(e, "Error processing split completion event for task %s", taskId);
        }
    }

    private static List<StageCpuDistribution> getCpuDistributions(QueryInfo queryInfo)
    {
        if (!queryInfo.getOutputStage().isPresent()) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<StageCpuDistribution> builder = ImmutableList.builder();
        populateDistribution(queryInfo.getOutputStage().get(), builder);

        return builder.build();
    }

    private static void populateDistribution(StageInfo stageInfo, ImmutableList.Builder<StageCpuDistribution> distributions)
    {
        distributions.add(computeCpuDistribution(stageInfo));
        for (StageInfo subStage : stageInfo.getSubStages()) {
            populateDistribution(subStage, distributions);
        }
    }

    private static StageCpuDistribution computeCpuDistribution(StageInfo stageInfo)
    {
        Distribution cpuDistribution = new Distribution();

        for (TaskInfo taskInfo : stageInfo.getTasks()) {
            cpuDistribution.add(taskInfo.getStats().getTotalCpuTime().toMillis());
        }

        DistributionSnapshot snapshot = cpuDistribution.snapshot();

        return new StageCpuDistribution(
                stageInfo.getStageId().getId(),
                stageInfo.getTasks().size(),
                snapshot.getP25(),
                snapshot.getP50(),
                snapshot.getP75(),
                snapshot.getP90(),
                snapshot.getP95(),
                snapshot.getP99(),
                snapshot.getMin(),
                snapshot.getMax(),
                (long) snapshot.getTotal(),
                snapshot.getTotal() / snapshot.getCount());
    }
}
