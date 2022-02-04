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
package com.facebook.presto.event;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.airlift.stats.Distribution;
import com.facebook.airlift.stats.Distribution.DistributionSnapshot;
import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.Column;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.Input;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.StageExecutionStats;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.OperatorInfo;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.TableFinishInfo;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.eventlistener.OperatorStatistics;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
import com.facebook.presto.spi.eventlistener.QueryInputMetadata;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryOutputMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.facebook.presto.spi.eventlistener.ResourceDistribution;
import com.facebook.presto.spi.eventlistener.StageStatistics;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.transaction.TransactionId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.jsonDistributedPlan;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.textDistributedPlan;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.time.Duration.ofMillis;
import static java.time.Instant.ofEpochMilli;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class QueryMonitor
{
    private static final Logger log = Logger.get(QueryMonitor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String EMPTY_STRING = "";

    private final JsonCodec<StageInfo> stageInfoCodec;
    private final JsonCodec<ExecutionFailureInfo> executionFailureInfoCodec;
    private final JsonCodec<OperatorInfo> operatorInfoCodec;
    private final EventListenerManager eventListenerManager;
    private final String serverVersion;
    private final String serverAddress;
    private final String environment;
    private final SessionPropertyManager sessionPropertyManager;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final int maxJsonLimit;

    @Inject
    public QueryMonitor(
            JsonCodec<StageInfo> stageInfoCodec,
            JsonCodec<ExecutionFailureInfo> executionFailureInfoCodec,
            JsonCodec<OperatorInfo> operatorInfoCodec,
            EventListenerManager eventListenerManager,
            NodeInfo nodeInfo,
            NodeVersion nodeVersion,
            SessionPropertyManager sessionPropertyManager,
            Metadata metadata,
            QueryMonitorConfig config)
    {
        this.eventListenerManager = requireNonNull(eventListenerManager, "eventListenerManager is null");
        this.stageInfoCodec = requireNonNull(stageInfoCodec, "stageInfoCodec is null");
        this.operatorInfoCodec = requireNonNull(operatorInfoCodec, "operatorInfoCodec is null");
        this.executionFailureInfoCodec = requireNonNull(executionFailureInfoCodec, "executionFailureInfoCodec is null");
        this.serverVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        this.serverAddress = requireNonNull(nodeInfo, "nodeInfo is null").getExternalAddress();
        this.environment = requireNonNull(nodeInfo, "nodeInfo is null").getEnvironment();
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.functionAndTypeManager = requireNonNull(metadata, "metadata is null").getFunctionAndTypeManager();
        this.maxJsonLimit = toIntExact(requireNonNull(config, "config is null").getMaxOutputStageJsonSize().toBytes());
    }

    public void queryCreatedEvent(BasicQueryInfo queryInfo)
    {
        eventListenerManager.queryCreated(
                new QueryCreatedEvent(
                        queryInfo.getQueryStats().getCreateTime().toDate().toInstant(),
                        createQueryContext(queryInfo.getSession(), queryInfo.getResourceGroupId()),
                        new QueryMetadata(
                                queryInfo.getQueryId().toString(),
                                queryInfo.getSession().getTransactionId().map(TransactionId::toString),
                                queryInfo.getQuery(),
                                queryInfo.getPreparedQuery(),
                                QUEUED.toString(),
                                queryInfo.getSelf(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                queryInfo.getSession().getTraceToken())));
    }

    public void queryImmediateFailureEvent(BasicQueryInfo queryInfo, ExecutionFailureInfo failure)
    {
        eventListenerManager.queryCompleted(new QueryCompletedEvent(
                new QueryMetadata(
                        queryInfo.getQueryId().toString(),
                        queryInfo.getSession().getTransactionId().map(TransactionId::toString),
                        queryInfo.getQuery(),
                        queryInfo.getPreparedQuery(),
                        queryInfo.getState().toString(),
                        queryInfo.getSelf(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(),
                        queryInfo.getSession().getTraceToken()),
                new QueryStatistics(
                        ofMillis(0),
                        ofMillis(0),
                        ofMillis(0),
                        ofMillis(queryInfo.getQueryStats().getWaitingForPrerequisitesTime().toMillis()),
                        ofMillis(queryInfo.getQueryStats().getQueuedTime().toMillis()),
                        ofMillis(0),
                        ofMillis(0),
                        ofMillis(0),
                        ofMillis(0),
                        ofMillis(0),
                        Optional.empty(),
                        ofMillis(0),
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        true,
                        new RuntimeStats()),
                createQueryContext(queryInfo.getSession(), queryInfo.getResourceGroupId()),
                new QueryIOMetadata(ImmutableList.of(), Optional.empty()),
                createQueryFailureInfo(failure, Optional.empty()),
                ImmutableList.of(),
                queryInfo.getQueryType(),
                ImmutableList.of(),
                ofEpochMilli(queryInfo.getQueryStats().getCreateTime().getMillis()),
                ofEpochMilli(queryInfo.getQueryStats().getEndTime().getMillis()),
                ofEpochMilli(queryInfo.getQueryStats().getEndTime().getMillis()),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                EMPTY_STRING));

        logQueryTimeline(queryInfo);
    }

    public void queryCompletedEvent(QueryInfo queryInfo)
    {
        QueryStats queryStats = queryInfo.getQueryStats();
        ImmutableList.Builder<StageStatistics> stageStatisticsBuilder = ImmutableList.builder();
        if (queryInfo.getOutputStage().isPresent()) {
            computeStageStatistics(queryInfo.getOutputStage().get(), stageStatisticsBuilder);
        }

        eventListenerManager.queryCompleted(
                new QueryCompletedEvent(
                        createQueryMetadata(queryInfo),
                        createQueryStatistics(queryInfo),
                        createQueryContext(queryInfo.getSession(), queryInfo.getResourceGroupId()),
                        getQueryIOMetadata(queryInfo),
                        createQueryFailureInfo(queryInfo.getFailureInfo(), queryInfo.getOutputStage()),
                        queryInfo.getWarnings(),
                        queryInfo.getQueryType(),
                        queryInfo.getFailedTasks().orElse(ImmutableList.of()).stream()
                                .map(TaskId::toString)
                                .collect(toImmutableList()),
                        ofEpochMilli(queryStats.getCreateTime().getMillis()),
                        ofEpochMilli(queryStats.getExecutionStartTime().getMillis()),
                        ofEpochMilli(queryStats.getEndTime() != null ? queryStats.getEndTime().getMillis() : 0),
                        stageStatisticsBuilder.build(),
                        createOperatorStatistics(queryInfo),
                        queryInfo.getExpandedQuery(),
                        createQueryPlanWithStats(queryInfo)));

        logQueryTimeline(queryInfo);
    }

    private QueryMetadata createQueryMetadata(QueryInfo queryInfo)
    {
        return new QueryMetadata(
                queryInfo.getQueryId().toString(),
                queryInfo.getSession().getTransactionId().map(TransactionId::toString),
                queryInfo.getQuery(),
                queryInfo.getPreparedQuery(),
                queryInfo.getState().toString(),
                queryInfo.getSelf(),
                createTextQueryPlan(queryInfo),
                createJsonQueryPlan(queryInfo),
                queryInfo.getOutputStage().flatMap(stage -> stageInfoCodec.toJsonWithLengthLimit(stage, maxJsonLimit)),
                queryInfo.getRuntimeOptimizedStages().orElse(ImmutableList.of()).stream()
                        .map(stageId -> String.valueOf(stageId.getId()))
                        .collect(toImmutableList()),
                queryInfo.getSession().getTraceToken());
    }

    private List<OperatorStatistics> createOperatorStatistics(QueryInfo queryInfo)
    {
        return queryInfo.getQueryStats().getOperatorSummaries().stream()
                .map(operatorSummary -> new OperatorStatistics(
                        operatorSummary.getStageId(),
                        operatorSummary.getStageExecutionId(),
                        operatorSummary.getPipelineId(),
                        operatorSummary.getOperatorId(),
                        operatorSummary.getPlanNodeId(),
                        operatorSummary.getOperatorType(),
                        operatorSummary.getTotalDrivers(),
                        operatorSummary.getAddInputCalls(),
                        operatorSummary.getAddInputWall(),
                        operatorSummary.getAddInputCpu(),
                        operatorSummary.getAddInputAllocation(),
                        operatorSummary.getRawInputDataSize(),
                        operatorSummary.getRawInputPositions(),
                        operatorSummary.getInputDataSize(),
                        operatorSummary.getInputPositions(),
                        operatorSummary.getSumSquaredInputPositions(),
                        operatorSummary.getGetOutputCalls(),
                        operatorSummary.getGetOutputWall(),
                        operatorSummary.getGetOutputCpu(),
                        operatorSummary.getGetOutputAllocation(),
                        operatorSummary.getOutputDataSize(),
                        operatorSummary.getOutputPositions(),
                        operatorSummary.getPhysicalWrittenDataSize(),
                        operatorSummary.getBlockedWall(),
                        operatorSummary.getFinishCalls(),
                        operatorSummary.getFinishWall(),
                        operatorSummary.getFinishCpu(),
                        operatorSummary.getFinishAllocation(),
                        operatorSummary.getUserMemoryReservation(),
                        operatorSummary.getRevocableMemoryReservation(),
                        operatorSummary.getSystemMemoryReservation(),
                        operatorSummary.getPeakUserMemoryReservation(),
                        operatorSummary.getPeakSystemMemoryReservation(),
                        operatorSummary.getPeakTotalMemoryReservation(),
                        operatorSummary.getSpilledDataSize(),
                        Optional.ofNullable(operatorSummary.getInfo()).map(operatorInfoCodec::toJson),
                        operatorSummary.getRuntimeStats()))
                .collect(toImmutableList());
    }

    private QueryStatistics createQueryStatistics(QueryInfo queryInfo)
    {
        QueryStats queryStats = queryInfo.getQueryStats();

        return new QueryStatistics(
                ofMillis(queryStats.getTotalCpuTime().toMillis()),
                ofMillis(queryStats.getRetriedCpuTime().toMillis()),
                ofMillis(queryStats.getTotalScheduledTime().toMillis()),
                ofMillis(queryStats.getWaitingForPrerequisitesTime().toMillis()),
                ofMillis(queryStats.getQueuedTime().toMillis()),
                ofMillis(queryStats.getResourceWaitingTime().toMillis()),
                ofMillis(queryStats.getSemanticAnalyzingTime().toMillis()),
                ofMillis(queryStats.getColumnAccessPermissionCheckingTime().toMillis()),
                ofMillis(queryStats.getDispatchingTime().toMillis()),
                ofMillis(queryStats.getTotalPlanningTime().toMillis()),
                Optional.of(ofMillis(queryStats.getAnalysisTime().toMillis())),
                ofMillis(queryStats.getExecutionTime().toMillis()),
                queryStats.getPeakRunningTasks(),
                queryStats.getPeakUserMemoryReservation().toBytes(),
                queryStats.getPeakTotalMemoryReservation().toBytes(),
                queryStats.getPeakTaskUserMemory().toBytes(),
                queryStats.getPeakTaskTotalMemory().toBytes(),
                queryStats.getPeakNodeTotalMemory().toBytes(),
                queryStats.getRawInputDataSize().toBytes(),
                queryStats.getRawInputPositions(),
                queryStats.getOutputDataSize().toBytes(),
                queryStats.getOutputPositions(),
                queryStats.getWrittenOutputLogicalDataSize().toBytes(),
                queryStats.getWrittenOutputPositions(),
                queryStats.getWrittenIntermediatePhysicalDataSize().toBytes(),
                queryStats.getSpilledDataSize().toBytes(),
                queryStats.getCumulativeUserMemory(),
                queryStats.getCumulativeTotalMemory(),
                queryStats.getCompletedDrivers(),
                queryInfo.isCompleteInfo(),
                queryStats.getRuntimeStats());
    }

    private QueryContext createQueryContext(SessionRepresentation session, Optional<ResourceGroupId> resourceGroup)
    {
        return new QueryContext(
                session.getUser(),
                session.getPrincipal(),
                session.getRemoteUserAddress(),
                session.getUserAgent(),
                session.getClientInfo(),
                session.getClientTags(),
                session.getSource(),
                session.getCatalog(),
                session.getSchema(),
                resourceGroup,
                mergeSessionAndCatalogProperties(session),
                session.getResourceEstimates(),
                serverAddress,
                serverVersion,
                environment);
    }

    private Optional<String> createTextQueryPlan(QueryInfo queryInfo)
    {
        try {
            if (queryInfo.getOutputStage().isPresent()) {
                return Optional.of(textDistributedPlan(
                        queryInfo.getOutputStage().get(),
                        functionAndTypeManager,
                        queryInfo.getSession().toSession(sessionPropertyManager),
                        false));
            }
        }
        catch (Exception e) {
            // Sometimes it is expected to fail. For example if generated plan is too long.
            // Don't fail to create event if the plan can not be created.
            log.warn(e, "Error creating explain plan for query %s", queryInfo.getQueryId());
        }
        return Optional.empty();
    }

    private Optional<String> createJsonQueryPlan(QueryInfo queryInfo)
    {
        try {
            if (queryInfo.getOutputStage().isPresent()) {
                return Optional.of(jsonDistributedPlan(
                        queryInfo.getOutputStage().get()));
            }
        }
        catch (Exception e) {
            // Don't fail to create event if the plan can not be created
            log.warn(e, "Error creating json plan for query %s: %s", queryInfo.getQueryId(), e);
        }
        return Optional.empty();
    }

    private static QueryIOMetadata getQueryIOMetadata(QueryInfo queryInfo)
    {
        ImmutableList.Builder<QueryInputMetadata> inputs = ImmutableList.builder();
        for (Input input : queryInfo.getInputs()) {
            inputs.add(new QueryInputMetadata(
                    input.getConnectorId().getCatalogName(),
                    input.getSchema(),
                    input.getTable(),
                    input.getColumns().stream()
                            .map(Column::getName).collect(Collectors.toList()),
                    input.getConnectorInfo(),
                    input.getStatistics()));
        }

        Optional<QueryOutputMetadata> output = Optional.empty();
        if (queryInfo.getOutput().isPresent()) {
            Optional<TableFinishInfo> tableFinishInfo = queryInfo.getQueryStats().getOperatorSummaries().stream()
                    .map(OperatorStats::getInfo)
                    .filter(TableFinishInfo.class::isInstance)
                    .map(TableFinishInfo.class::cast)
                    .findFirst();

            output = Optional.of(
                    new QueryOutputMetadata(
                            queryInfo.getOutput().get().getConnectorId().getCatalogName(),
                            queryInfo.getOutput().get().getSchema(),
                            queryInfo.getOutput().get().getTable(),
                            tableFinishInfo.map(TableFinishInfo::getSerializedConnectorOutputMetadata),
                            tableFinishInfo.map(TableFinishInfo::isJsonLengthLimitExceeded)));
        }
        return new QueryIOMetadata(inputs.build(), output);
    }

    private Optional<QueryFailureInfo> createQueryFailureInfo(ExecutionFailureInfo failureInfo, Optional<StageInfo> outputStage)
    {
        if (failureInfo == null) {
            return Optional.empty();
        }

        Optional<TaskInfo> failedTask = outputStage.flatMap(QueryMonitor::findFailedTask);

        return Optional.of(new QueryFailureInfo(
                failureInfo.getErrorCode(),
                Optional.ofNullable(failureInfo.getType()),
                Optional.ofNullable(failureInfo.getMessage()),
                failedTask.map(task -> task.getTaskId().toString()),
                failedTask.map(task -> task.getTaskStatus().getSelf().getHost()),
                executionFailureInfoCodec.toJson(failureInfo)));
    }

    private static Optional<TaskInfo> findFailedTask(StageInfo stageInfo)
    {
        for (StageInfo subStage : stageInfo.getSubStages()) {
            Optional<TaskInfo> task = findFailedTask(subStage);
            if (task.isPresent()) {
                return task;
            }
        }
        return stageInfo.getLatestAttemptExecutionInfo().getTasks().stream()
                .filter(taskInfo -> taskInfo.getTaskStatus().getState() == TaskState.FAILED)
                .findFirst();
    }

    private static Map<String, String> mergeSessionAndCatalogProperties(SessionRepresentation session)
    {
        Map<String, String> mergedProperties = new LinkedHashMap<>(session.getSystemProperties());

        // Either processed or unprocessed catalog properties, but not both.  Instead of trying to enforces this while
        // firing events, allow both to be set and if there is a duplicate favor the processed properties.
        for (Map.Entry<String, Map<String, String>> catalogEntry : session.getUnprocessedCatalogProperties().entrySet()) {
            for (Map.Entry<String, String> entry : catalogEntry.getValue().entrySet()) {
                mergedProperties.put(catalogEntry.getKey() + "." + entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<ConnectorId, Map<String, String>> catalogEntry : session.getCatalogProperties().entrySet()) {
            for (Map.Entry<String, String> entry : catalogEntry.getValue().entrySet()) {
                mergedProperties.put(catalogEntry.getKey().getCatalogName() + "." + entry.getKey(), entry.getValue());
            }
        }
        return ImmutableMap.copyOf(mergedProperties);
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
            long planning = queryStats.getTotalPlanningTime().toMillis();

            List<StageInfo> stages = getAllStages(queryInfo.getOutputStage());
            // long lastSchedulingCompletion = 0;
            long firstTaskStartTime = queryEndTime.getMillis();
            long lastTaskStartTime = queryStartTime.getMillis() + planning;
            long lastTaskEndTime = queryStartTime.getMillis() + planning;
            for (StageInfo stage : stages) {
                // only consider leaf stages
                if (!stage.getSubStages().isEmpty()) {
                    continue;
                }

                for (TaskInfo taskInfo : stage.getLatestAttemptExecutionInfo().getTasks()) {
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

            long elapsed = max(queryEndTime.getMillis() - queryStartTime.getMillis(), 0);
            long scheduling = max(firstTaskStartTime - queryStartTime.getMillis() - planning, 0);
            long running = max(lastTaskEndTime - firstTaskStartTime, 0);
            long finishing = max(queryEndTime.getMillis() - lastTaskEndTime, 0);

            logQueryTimeline(
                    queryInfo.getQueryId(),
                    queryInfo.getSession().getTransactionId().map(TransactionId::toString).orElse(""),
                    elapsed,
                    planning,
                    scheduling,
                    running,
                    finishing,
                    queryStartTime,
                    queryEndTime);
        }
        catch (Exception e) {
            log.error(e, "Error logging query timeline");
        }
    }

    private static void logQueryTimeline(BasicQueryInfo queryInfo)
    {
        DateTime queryStartTime = queryInfo.getQueryStats().getCreateTime();
        DateTime queryEndTime = queryInfo.getQueryStats().getEndTime();

        // query didn't finish cleanly
        if (queryStartTime == null || queryEndTime == null) {
            return;
        }

        long elapsed = max(queryEndTime.getMillis() - queryStartTime.getMillis(), 0);

        logQueryTimeline(
                queryInfo.getQueryId(),
                queryInfo.getSession().getTransactionId().map(TransactionId::toString).orElse(""),
                elapsed,
                elapsed,
                0,
                0,
                0,
                queryStartTime,
                queryEndTime);
    }

    private static void logQueryTimeline(
            QueryId queryId,
            String transactionId,
            long elapsedMillis,
            long planningMillis,
            long schedulingMillis,
            long runningMillis,
            long finishingMillis,
            DateTime queryStartTime,
            DateTime queryEndTime)
    {
        log.info("TIMELINE: Query %s :: Transaction:[%s] :: elapsed %sms :: planning %sms :: scheduling %sms :: running %sms :: finishing %sms :: begin %s :: end %s",
                queryId,
                transactionId,
                elapsedMillis,
                planningMillis,
                schedulingMillis,
                runningMillis,
                finishingMillis,
                queryStartTime,
                queryEndTime);
    }

    private static ResourceDistribution createResourceDistribution(
            DistributionSnapshot distributionSnapshot)
    {
        return new ResourceDistribution(
                distributionSnapshot.getP25(),
                distributionSnapshot.getP50(),
                distributionSnapshot.getP75(),
                distributionSnapshot.getP90(),
                distributionSnapshot.getP95(),
                distributionSnapshot.getP99(),
                distributionSnapshot.getMin(),
                distributionSnapshot.getMax(),
                (long) distributionSnapshot.getTotal(),
                distributionSnapshot.getTotal() / distributionSnapshot.getCount());
    }

    private static void computeStageStatistics(
            StageInfo stageInfo,
            ImmutableList.Builder<StageStatistics> stageStatisticsBuilder)
    {
        Distribution cpuDistribution = new Distribution();
        Distribution memoryDistribution = new Distribution();

        StageExecutionInfo executionInfo = stageInfo.getLatestAttemptExecutionInfo();

        for (TaskInfo taskInfo : executionInfo.getTasks()) {
            cpuDistribution.add(NANOSECONDS.toMillis(taskInfo.getStats().getTotalCpuTimeInNanos()));
            memoryDistribution.add(taskInfo.getStats().getPeakTotalMemoryInBytes());
        }

        stageStatisticsBuilder.add(new StageStatistics(
                stageInfo.getStageId().getId(),
                executionInfo.getStats().getGcInfo().getStageExecutionId(),
                executionInfo.getTasks().size(),
                executionInfo.getStats().getTotalScheduledTime(),
                executionInfo.getStats().getTotalCpuTime(),
                executionInfo.getStats().getRetriedCpuTime(),
                executionInfo.getStats().getTotalBlockedTime(),
                executionInfo.getStats().getRawInputDataSize(),
                executionInfo.getStats().getProcessedInputDataSize(),
                executionInfo.getStats().getPhysicalWrittenDataSize(),
                executionInfo.getStats().getGcInfo(),
                createResourceDistribution(cpuDistribution.snapshot()),
                createResourceDistribution(memoryDistribution.snapshot())));

        stageInfo.getSubStages().forEach(subStage -> computeStageStatistics(subStage, stageStatisticsBuilder));
    }

    private static QueryPlanWithStageInfo computeStageStatisticsWithPlan(
            StageInfo stageInfo)
    {
        List<QueryPlanWithStageInfo> children = new ArrayList<QueryPlanWithStageInfo>();
        for (StageInfo subStageInfo : stageInfo.getSubStages()) {
            QueryPlanWithStageInfo subQueryPlanWithStageInfo = computeStageStatisticsWithPlan(subStageInfo);
            children.add(subQueryPlanWithStageInfo);
        }

        StageExecutionStats stageExecutionStats = stageInfo.getLatestAttemptExecutionInfo().getStats();

        return new QueryPlanWithStageInfo(
                stageInfo.getLatestAttemptExecutionInfo().getState(),

                stageInfo.getStageId(),
                stageInfo.getPlan().isPresent() ? stageInfo.getPlan().get().getId() : new PlanFragmentId(0),
                stageInfo.getPlan().get().getRoot().getId(),

                stageExecutionStats.getUserMemoryReservation(),

                stageExecutionStats.getTotalCpuTime(),
                stageExecutionStats.isFullyBlocked(),
                stageExecutionStats.getTotalBlockedTime(),

                stageExecutionStats.getQueuedDrivers(),
                stageExecutionStats.getRunningDrivers(),
                stageExecutionStats.getCompletedDrivers(),

                stageExecutionStats.getTotalLifespans(),
                stageExecutionStats.getCompletedLifespans(),

                stageExecutionStats.getRawInputDataSize(),
                stageExecutionStats.getRawInputPositions(),

                stageExecutionStats.getBufferedDataSize(),
                stageExecutionStats.getOutputDataSize(),
                stageExecutionStats.getOutputPositions(),

                stageInfo.getPlan().get().getJsonRepresentation().isPresent() ? stageInfo.getPlan().get().getJsonRepresentation().get() : "",
                children);
    }

    private String createQueryPlanWithStats(QueryInfo queryInfo)
    {
        String queryPlanWithStats = "";
        if (queryInfo.getOutputStage().isPresent()) {
            QueryPlanWithStageInfo queryPlanWithStageInfo = computeStageStatisticsWithPlan(queryInfo.getOutputStage().get());
            try {
                queryPlanWithStats = objectMapper.writeValueAsString(queryPlanWithStageInfo);
            }
            catch (JsonProcessingException e) {
                // Don't fail event if the plan can not be created
                log.warn(e, "Error creating json plan for query %s: %s", queryInfo.getQueryId(), e);
            }
        }
        return queryPlanWithStats;
    }

    public static class QueryPlanWithStageInfo
    {
        private final StageExecutionState state;

        private final StageId stageId;
        private final PlanFragmentId planId;
        private final PlanNodeId rootId;

        private final DataSize userMemoryReservation;

        private final Duration totalCpuTime;
        private final boolean fullyBlocked;
        private final Duration totalBlockedTime;

        private final DataSize bufferedDataSize;
        private final DataSize outputDataSize;
        private final long outputPositions;

        private final int queuedDrivers;
        private final int runningDrivers;
        private final int completedDrivers;

        private final int totalLifespans;
        private final int completedLifespans;

        private final DataSize rawInputDataSize;
        private final long rawInputPositions;

        private final String jsonPlan;
        private final List<QueryPlanWithStageInfo> subStages;

        @JsonCreator
        public QueryPlanWithStageInfo(
                @JsonProperty("state") StageExecutionState state,

                @JsonProperty("stageId") StageId stageId,
                @JsonProperty("planId") PlanFragmentId planId,
                @JsonProperty("rootId") PlanNodeId rootId,

                @JsonProperty("userMemoryReservation") DataSize userMemoryReservation,

                @JsonProperty("totalCpuTime") Duration totalCpuTime,
                @JsonProperty("fullyBlocked") boolean fullyBlocked,
                @JsonProperty("totalBlockedTime") Duration totalBlockedTime,

                @JsonProperty("queuedDrivers") int queuedDrivers,
                @JsonProperty("runningDrivers") int runningDrivers,
                @JsonProperty("completedDrivers") int completedDrivers,

                @JsonProperty("totalLifespans") int totalLifespans,
                @JsonProperty("completedLifespans") int completedLifespans,

                @JsonProperty("rawInputDataSize") DataSize rawInputDataSize,
                @JsonProperty("rawInputPositions") long rawInputPositions,

                @JsonProperty("bufferedDataSize") DataSize bufferedDataSize,
                @JsonProperty("outputDataSize") DataSize outputDataSize,
                @JsonProperty("outputPositions") long outputPositions,

                @JsonProperty("jsonPlan") String jsonPlan,
                @JsonProperty("subStages") List<QueryPlanWithStageInfo> subStages)
        {
            this.state = requireNonNull(state, "state is Null");
            this.stageId = requireNonNull(stageId, "stageId is null");
            this.planId = requireNonNull(planId, "planId is null");
            this.rootId = requireNonNull(rootId, "rootId is null");

            this.userMemoryReservation = requireNonNull(userMemoryReservation, "userMemoryReservation is null");

            this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
            this.totalBlockedTime = requireNonNull(totalBlockedTime, "totalBlockedTime is null");
            this.fullyBlocked = fullyBlocked;

            this.bufferedDataSize = requireNonNull(bufferedDataSize, "bufferedDataSize is null");
            this.outputDataSize = requireNonNull(outputDataSize, "outputDataSize is null");
            checkArgument(outputPositions >= 0, "outputPositions is negative");
            this.outputPositions = outputPositions;

            checkArgument(queuedDrivers >= 0, "queuedDrivers is negative");
            this.queuedDrivers = queuedDrivers;
            checkArgument(runningDrivers >= 0, "runningDrivers is negative");
            this.runningDrivers = runningDrivers;
            checkArgument(completedDrivers >= 0, "completedDrivers is negative");
            this.completedDrivers = completedDrivers;

            checkArgument(totalLifespans >= 0, "completedLifespans is negative");
            this.totalLifespans = totalLifespans;
            checkArgument(completedLifespans >= 0, "completedLifespans is negative");
            this.completedLifespans = completedLifespans;

            this.rawInputDataSize = requireNonNull(rawInputDataSize, "rawInputDataSize is null");
            checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
            this.rawInputPositions = rawInputPositions;

            this.jsonPlan = requireNonNull(jsonPlan, "jsonPlan is Null");
            this.subStages = ImmutableList.copyOf(requireNonNull(subStages, "subStages is null"));
        }

        @JsonProperty
        public StageExecutionState getStageExecutionState()
        {
            return state;
        }

        @JsonProperty
        public StageId getStageId()
        {
            return stageId;
        }

        @JsonProperty
        public PlanFragmentId getPlanId()
        {
            return planId;
        }

        @JsonProperty
        public PlanNodeId getRootId()
        {
            return rootId;
        }

        @JsonProperty
        public DataSize getUserMemoryReservation()
        {
            return userMemoryReservation;
        }

        @JsonProperty
        public Duration getTotalCpuTime()
        {
            return totalCpuTime;
        }

        @JsonProperty
        public Duration getTotalBlockedTime()
        {
            return totalBlockedTime;
        }

        @JsonProperty
        public boolean isFullyBlocked()
        {
            return fullyBlocked;
        }

        @JsonProperty
        public DataSize getBufferedDataSize()
        {
            return bufferedDataSize;
        }

        @JsonProperty
        public DataSize getOutputDataSize()
        {
            return outputDataSize;
        }

        @JsonProperty
        public long getOutputPositions()
        {
            return outputPositions;
        }

        @JsonProperty
        public int getQueuedDrivers()
        {
            return queuedDrivers;
        }

        @JsonProperty
        public int getRunningDrivers()
        {
            return runningDrivers;
        }

        @JsonProperty
        public int getCompletedDrivers()
        {
            return completedDrivers;
        }

        @JsonProperty
        public int getTotalLifespans()
        {
            return totalLifespans;
        }

        @JsonProperty
        public int getCompletedLifespans()
        {
            return completedLifespans;
        }

        @JsonProperty
        public DataSize getRawInputDataSize()
        {
            return rawInputDataSize;
        }

        @JsonProperty
        public long getRawInputPositions()
        {
            return rawInputPositions;
        }

        @JsonProperty
        public String getJsonPlan()
        {
            return jsonPlan;
        }

        @JsonProperty
        public List<QueryPlanWithStageInfo> getSubStages()
        {
            return subStages;
        }
    }
}
