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
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.cost.HistoryBasedPlanStatisticsManager;
import com.facebook.presto.cost.HistoryBasedPlanStatisticsTracker;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.Input;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageExecutionInfo;
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
import com.facebook.presto.server.BasicQueryStats;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.eventlistener.Column;
import com.facebook.presto.spi.eventlistener.OperatorStatistics;
import com.facebook.presto.spi.eventlistener.OutputColumnMetadata;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
import com.facebook.presto.spi.eventlistener.QueryInputMetadata;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryOutputMetadata;
import com.facebook.presto.spi.eventlistener.QueryProgressEvent;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.facebook.presto.spi.eventlistener.QueryUpdatedEvent;
import com.facebook.presto.spi.eventlistener.ResourceDistribution;
import com.facebook.presto.spi.eventlistener.StageStatistics;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.statistics.PlanStatisticsWithSourceInfo;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.CanonicalPlanWithInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import jakarta.inject.Inject;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.airlift.units.DataSize.succinctBytes;
import static com.facebook.presto.SystemSessionProperties.logQueryPlansUsedInHistoryBasedOptimizer;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.graphvizDistributedPlan;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.jsonDistributedPlan;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.textDistributedPlan;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.Double.NaN;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.time.Duration.ofMillis;
import static java.time.Instant.ofEpochMilli;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class QueryMonitor
{
    private static final Logger log = Logger.get(QueryMonitor.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final JsonCodec<StageInfo> stageInfoCodec;
    private final JsonCodec<ExecutionFailureInfo> executionFailureInfoCodec;
    private final JsonCodec<OperatorInfo> operatorInfoCodec;
    private final EventListenerManager eventListenerManager;
    private final String serverVersion;
    private final String serverAddress;
    private final String environment;
    private final SessionPropertyManager sessionPropertyManager;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final HistoryBasedPlanStatisticsTracker historyBasedPlanStatisticsTracker;
    private final int maxJsonLimit;
    private final String workerType;

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
            QueryMonitorConfig config,
            HistoryBasedPlanStatisticsManager historyBasedPlanStatisticsManager,
            FeaturesConfig featuresConfig)
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
        this.historyBasedPlanStatisticsTracker = requireNonNull(historyBasedPlanStatisticsManager, "historyBasedPlanStatisticsManager is null").getHistoryBasedPlanStatisticsTracker();
        this.maxJsonLimit = toIntExact(requireNonNull(config, "config is null").getMaxOutputStageJsonSize().toBytes());
        this.workerType = requireNonNull(featuresConfig, "featuresConfig is null").isNativeExecutionEnabled() ? "Prestissimo" : "Presto";
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
                                queryInfo.getQueryHash(),
                                queryInfo.getPreparedQuery(),
                                QUEUED.toString(),
                                queryInfo.getSelf(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                queryInfo.getSession().getTraceToken(),
                                Optional.empty())));
    }

    public void queryUpdatedEvent(QueryInfo queryInfo)
    {
        eventListenerManager.queryUpdated(new QueryUpdatedEvent(createQueryMetadata(queryInfo)));
    }

    public void publishQueryProgressEvent(long monotonicallyIncreasingEventId, BasicQueryInfo queryInfo)
    {
        eventListenerManager.publishQueryProgress(new QueryProgressEvent(
                monotonicallyIncreasingEventId,
                new QueryMetadata(
                        queryInfo.getQueryId().toString(),
                        queryInfo.getSession().getTransactionId().map(TransactionId::toString),
                        queryInfo.getQuery(),
                        queryInfo.getQueryHash(),
                        queryInfo.getPreparedQuery(),
                        queryInfo.getState().toString(),
                        queryInfo.getSelf(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(),
                        queryInfo.getSession().getTraceToken(),
                        Optional.empty()),
                createQueryStatistics(queryInfo),
                createQueryContext(queryInfo.getSession(), queryInfo.getResourceGroupId()),
                queryInfo.getQueryType(),
                ofEpochMilli(queryInfo.getQueryStats().getCreateTime().getMillis())));
    }

    public void queryImmediateFailureEvent(BasicQueryInfo queryInfo, ExecutionFailureInfo failure)
    {
        eventListenerManager.queryCompleted(new QueryCompletedEvent(
                new QueryMetadata(
                        queryInfo.getQueryId().toString(),
                        queryInfo.getSession().getTransactionId().map(TransactionId::toString),
                        queryInfo.getQuery(),
                        queryInfo.getQueryHash(),
                        queryInfo.getPreparedQuery(),
                        queryInfo.getState().toString(),
                        queryInfo.getSelf(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(),
                        queryInfo.getSession().getTraceToken(),
                        Optional.empty()),
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
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty()));

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
                        createPlanStatistics(queryInfo.getPlanStatsAndCosts()),
                        historyBasedPlanStatisticsTracker.getQueryStats(queryInfo).values().stream().collect(toImmutableList()),
                        getPlanHash(queryInfo.getPlanCanonicalInfo()),
                        historyBasedPlanStatisticsTracker.getCanonicalPlan(queryInfo.getQueryId()),
                        logQueryPlansUsedInHistoryBasedOptimizer(queryInfo.getSession().toSession(sessionPropertyManager)) ? serializeStatsEquivalentPlan(historyBasedPlanStatisticsTracker.getStatsEquivalentPlanRootNode(queryInfo.getQueryId())) : Optional.empty(),
                        queryInfo.getExpandedQuery(),
                        queryInfo.getOptimizerInformation(),
                        queryInfo.getCteInformationList(),
                        queryInfo.getScalarFunctions(),
                        queryInfo.getAggregateFunctions(),
                        queryInfo.getWindowFunctions(),
                        queryInfo.getPrestoSparkExecutionContext(),
                        getPlanHash(queryInfo.getPlanCanonicalInfo(), historyBasedPlanStatisticsTracker.getStatsEquivalentPlanRootNode(queryInfo.getQueryId())),
                        Optional.of(queryInfo.getPlanIdNodeMap())));

        logQueryTimeline(queryInfo);
    }

    private List<PlanStatisticsWithSourceInfo> createPlanStatistics(StatsAndCosts planStatsAndCosts)
    {
        return planStatsAndCosts.getStats().entrySet().stream().map(entry -> entry.getValue().toPlanStatisticsWithSourceInfo(entry.getKey())).collect(toImmutableList());
    }

    private Map<PlanNodeId, Map<PlanCanonicalizationStrategy, String>> getPlanHash(List<CanonicalPlanWithInfo> canonicalPlanWithInfos)
    {
        Map<PlanNodeId, Map<PlanCanonicalizationStrategy, String>> planNodeIdStrategyHashMap = new HashMap<>();
        for (CanonicalPlanWithInfo canonicalPlanWithInfo : canonicalPlanWithInfos) {
            PlanCanonicalizationStrategy strategy = canonicalPlanWithInfo.getCanonicalPlan().getStrategy();
            PlanNodeId planNodeId = canonicalPlanWithInfo.getCanonicalPlan().getPlan().getId();
            String hash = canonicalPlanWithInfo.getInfo().getHash();
            if (!planNodeIdStrategyHashMap.containsKey(planNodeId)) {
                planNodeIdStrategyHashMap.put(planNodeId, new HashMap<>());
            }
            planNodeIdStrategyHashMap.get(planNodeId).put(strategy, hash);
        }
        return planNodeIdStrategyHashMap;
    }

    private QueryMetadata createQueryMetadata(QueryInfo queryInfo)
    {
        return new QueryMetadata(
                queryInfo.getQueryId().toString(),
                queryInfo.getSession().getTransactionId().map(TransactionId::toString),
                queryInfo.getQuery(),
                queryInfo.getQueryHash(),
                queryInfo.getPreparedQuery(),
                queryInfo.getState().toString(),
                queryInfo.getSelf(),
                createTextQueryPlan(queryInfo),
                createJsonQueryPlan(queryInfo),
                createGraphvizQueryPlan(queryInfo),
                queryInfo.getOutputStage().flatMap(stage -> stageInfoCodec.toJsonWithLengthLimit(stage, maxJsonLimit)),
                queryInfo.getRuntimeOptimizedStages().orElse(ImmutableList.of()).stream()
                        .map(stageId -> String.valueOf(stageId.getId()))
                        .collect(toImmutableList()),
                queryInfo.getSession().getTraceToken(),
                Optional.ofNullable(queryInfo.getUpdateType()));
    }

    private List<OperatorStatistics> createOperatorStatistics(QueryInfo queryInfo)
    {
        Map<PlanNodeId, PlanNodeStatsEstimate> estimateMap = queryInfo.getPlanStatsAndCosts().getStats();
        Map<PlanNodeId, PlanNode> planNodeIdMap = queryInfo.getPlanIdNodeMap();
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
                        succinctBytes(operatorSummary.getAddInputAllocationInBytes()),
                        succinctBytes(operatorSummary.getRawInputDataSizeInBytes()),
                        operatorSummary.getRawInputPositions(),
                        succinctBytes(operatorSummary.getInputDataSizeInBytes()),
                        operatorSummary.getInputPositions(),
                        operatorSummary.getSumSquaredInputPositions(),
                        operatorSummary.getGetOutputCalls(),
                        operatorSummary.getGetOutputWall(),
                        operatorSummary.getGetOutputCpu(),
                        succinctBytes(operatorSummary.getGetOutputAllocationInBytes()),
                        succinctBytes(operatorSummary.getOutputDataSizeInBytes()),
                        operatorSummary.getOutputPositions(),
                        succinctBytes(operatorSummary.getPhysicalWrittenDataSizeInBytes()),
                        operatorSummary.getBlockedWall(),
                        operatorSummary.getFinishCalls(),
                        operatorSummary.getFinishWall(),
                        operatorSummary.getFinishCpu(),
                        succinctBytes(operatorSummary.getFinishAllocationInBytes()),
                        succinctBytes(operatorSummary.getUserMemoryReservationInBytes()),
                        succinctBytes(operatorSummary.getRevocableMemoryReservationInBytes()),
                        succinctBytes(operatorSummary.getSystemMemoryReservationInBytes()),
                        succinctBytes(operatorSummary.getPeakUserMemoryReservationInBytes()),
                        succinctBytes(operatorSummary.getPeakSystemMemoryReservationInBytes()),
                        succinctBytes(operatorSummary.getPeakTotalMemoryReservationInBytes()),
                        succinctBytes(operatorSummary.getSpilledDataSizeInBytes()),
                        Optional.ofNullable(operatorSummary.getInfo()).map(operatorInfoCodec::toJson),
                        operatorSummary.getRuntimeStats(),
                        getPlanNodeEstimateOutputSize(operatorSummary.getPlanNodeId(), estimateMap, planNodeIdMap),
                        estimateMap.containsKey(operatorSummary.getPlanNodeId()) ? estimateMap.get(operatorSummary.getPlanNodeId()).getOutputRowCount() : NaN))
                .collect(toImmutableList());
    }

    private double getPlanNodeEstimateOutputSize(PlanNodeId nodeId, Map<PlanNodeId, PlanNodeStatsEstimate> estimateMap, Map<PlanNodeId, PlanNode> planNodeIdMap)
    {
        if (!estimateMap.containsKey(nodeId)) {
            return NaN;
        }
        checkArgument(planNodeIdMap.containsKey(nodeId), "plan node does not exist in planNodeIdMap");
        return estimateMap.get(nodeId).getOutputSizeInBytes(planNodeIdMap.get(nodeId));
    }

    private QueryStatistics createQueryStatistics(QueryInfo queryInfo)
    {
        QueryStats queryStats = queryInfo.getQueryStats();

        return new QueryStatistics(
                ofMillis(queryStats.getTotalCpuTime().toMillis()),
                ofMillis(queryStats.getRetriedCpuTime().toMillis()),
                ofMillis(queryStats.getElapsedTime().toMillis()),
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
                queryStats.getShuffledDataSize().toBytes(),
                queryStats.getShuffledPositions(),
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
                queryInfo.isFinalQueryInfo(),
                queryStats.getRuntimeStats());
    }

    private QueryStatistics createQueryStatistics(BasicQueryInfo basicQueryInfo)
    {
        BasicQueryStats queryStats = basicQueryInfo.getQueryStats();

        return new QueryStatistics(
                ofMillis(queryStats.getTotalCpuTime().toMillis()),
                ofMillis(0),
                ofMillis(queryStats.getElapsedTime().toMillis()),
                ofMillis(queryStats.getWaitingForPrerequisitesTime().toMillis()),
                ofMillis(queryStats.getQueuedTime().toMillis()),
                ofMillis(0),
                ofMillis(0),
                ofMillis(0),
                ofMillis(0),
                ofMillis(0),
                Optional.of(ofMillis(0)),
                ofMillis(queryStats.getExecutionTime().toMillis()),
                queryStats.getPeakRunningTasks(),
                queryStats.getPeakUserMemoryReservation().toBytes(),
                queryStats.getPeakTotalMemoryReservation().toBytes(),
                0,
                0,
                0,
                0,
                0,
                queryStats.getRawInputDataSize().toBytes(),
                queryStats.getRawInputPositions(),
                0,
                0,
                0,
                0,
                0,
                0,
                queryStats.getCumulativeUserMemory(),
                queryStats.getCumulativeTotalMemory(),
                queryStats.getCompletedDrivers(),
                false,
                new RuntimeStats());
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
                environment,
                workerType);
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
                        queryInfo.getOutputStage().get(),
                        functionAndTypeManager,
                        queryInfo.getSession().toSession(sessionPropertyManager)));
            }
        }
        catch (Exception e) {
            // Don't fail to create event if the plan can not be created
            log.warn(e, "Error creating json plan for query %s: %s", queryInfo.getQueryId(), e);
        }
        return Optional.empty();
    }

    private Optional<String> createGraphvizQueryPlan(QueryInfo queryInfo)
    {
        try {
            if (queryInfo.getOutputStage().isPresent()) {
                return Optional.of(graphvizDistributedPlan(
                        queryInfo.getOutputStage().get(),
                        functionAndTypeManager,
                        queryInfo.getSession().toSession(sessionPropertyManager)));
            }
        }
        catch (Exception e) {
            // Don't fail to create event if the graphviz plan can not be created
            log.warn(e, "Error creating graphviz plan for query %s: %s", queryInfo.getQueryId(), e);
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
                            .map(column -> new Column(column.getName(), column.getType()))
                            .collect(Collectors.toList()),
                    input.getConnectorInfo(),
                    input.getStatistics(),
                    input.getSerializedCommitOutput()));
        }

        Optional<QueryOutputMetadata> output = Optional.empty();
        if (queryInfo.getOutput().isPresent()) {
            Optional<TableFinishInfo> tableFinishInfo = queryInfo.getQueryStats().getOperatorSummaries().stream()
                    .map(OperatorStats::getInfo)
                    .filter(TableFinishInfo.class::isInstance)
                    .map(TableFinishInfo.class::cast)
                    .findFirst();

            Optional<List<OutputColumnMetadata>> outputColumnsMetadata = queryInfo.getOutput().get().getColumns()
                    .map(columns -> columns.stream()
                            .map(column -> new OutputColumnMetadata(
                                    column.getColumnName(),
                                    column.getColumnType(),
                                    column.getSourceColumns()))
                            .collect(toImmutableList()));

            output = Optional.of(
                    new QueryOutputMetadata(
                            queryInfo.getOutput().get().getConnectorId().getCatalogName(),
                            queryInfo.getOutput().get().getSchema(),
                            queryInfo.getOutput().get().getTable(),
                            tableFinishInfo.map(TableFinishInfo::getSerializedConnectorOutputMetadata),
                            tableFinishInfo.map(TableFinishInfo::isJsonLengthLimitExceeded),
                            queryInfo.getOutput().get().getSerializedCommitOutput(),
                            outputColumnsMetadata));
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
            long queryStartTime = queryStats.getCreateTimeInMillis();
            long queryEndTime = queryStats.getEndTimeInMillis();

            // query didn't finish cleanly
            if (queryStartTime == 0 || queryEndTime == 0) {
                return;
            }

            // planning duration -- start to end of planning
            long planning = queryStats.getTotalPlanningTime().toMillis();

            List<StageInfo> stages = getAllStages(queryInfo.getOutputStage());
            // long lastSchedulingCompletion = 0;
            long firstTaskStartTime = queryEndTime;
            long lastTaskStartTime = queryStartTime + planning;
            long lastTaskEndTime = queryStartTime + planning;
            for (StageInfo stage : stages) {
                // only consider leaf stages
                if (!stage.getSubStages().isEmpty()) {
                    continue;
                }

                for (TaskInfo taskInfo : stage.getLatestAttemptExecutionInfo().getTasks()) {
                    TaskStats taskStats = taskInfo.getStats();

                    long firstStartTimeInMillis = taskStats.getFirstStartTimeInMillis();
                    if (firstStartTimeInMillis != 0) {
                        firstTaskStartTime = Math.min(firstStartTimeInMillis, firstTaskStartTime);
                    }

                    long lastStartTimeInMillis = taskStats.getLastStartTimeInMillis();
                    if (lastStartTimeInMillis != 0) {
                        lastTaskStartTime = max(lastStartTimeInMillis, lastTaskStartTime);
                    }

                    long endTimeInMillis = taskStats.getEndTimeInMillis();
                    if (endTimeInMillis != 0) {
                        lastTaskEndTime = max(endTimeInMillis, lastTaskEndTime);
                    }
                }
            }

            long elapsed = max(queryEndTime - queryStartTime, 0);
            long scheduling = max(firstTaskStartTime - queryStartTime - planning, 0);
            long running = max(lastTaskEndTime - firstTaskStartTime, 0);
            long finishing = max(queryEndTime - lastTaskEndTime, 0);

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
        long queryStartTimeInMillis = queryInfo.getQueryStats().getCreateTimeInMillis();
        long queryEndTimeInMillis = queryInfo.getQueryStats().getEndTimeInMillis();

        // query didn't finish cleanly
        if (queryStartTimeInMillis == 0 || queryEndTimeInMillis == 0) {
            return;
        }

        long elapsed = max(queryEndTimeInMillis - queryStartTimeInMillis, 0);

        logQueryTimeline(
                queryInfo.getQueryId(),
                queryInfo.getSession().getTransactionId().map(TransactionId::toString).orElse(""),
                elapsed,
                elapsed,
                0,
                0,
                0,
                queryStartTimeInMillis,
                queryEndTimeInMillis);
    }

    private static void logQueryTimeline(
            QueryId queryId,
            String transactionId,
            long elapsedMillis,
            long planningMillis,
            long schedulingMillis,
            long runningMillis,
            long finishingMillis,
            long queryStartTimeInMillis,
            long queryEndTimeInMillis)
    {
        log.info("TIMELINE: Query %s :: Transaction:[%s] :: elapsed %sms :: planning %sms :: scheduling %sms :: running %sms :: finishing %sms :: begin %sms :: end %sms",
                queryId,
                transactionId,
                elapsedMillis,
                planningMillis,
                schedulingMillis,
                runningMillis,
                finishingMillis,
                queryStartTimeInMillis,
                queryEndTimeInMillis);
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
                succinctBytes(executionInfo.getStats().getRawInputDataSizeInBytes()),
                succinctBytes(executionInfo.getStats().getProcessedInputDataSizeInBytes()),
                succinctBytes(executionInfo.getStats().getPhysicalWrittenDataSizeInBytes()),
                executionInfo.getStats().getGcInfo(),
                createResourceDistribution(cpuDistribution.snapshot()),
                createResourceDistribution(memoryDistribution.snapshot())));

        stageInfo.getSubStages().forEach(subStage -> computeStageStatistics(subStage, stageStatisticsBuilder));
    }

    private Map<PlanCanonicalizationStrategy, String> getPlanHash(List<CanonicalPlanWithInfo> canonicalPlanWithInfos, Optional<PlanNode> root)
    {
        if (root.isPresent()) {
            return canonicalPlanWithInfos.stream().filter(x -> x.getCanonicalPlan().getPlan().equals(root.get())).collect(toImmutableMap(x -> x.getCanonicalPlan().getStrategy(), x -> x.getInfo().getHash(), (a, b) -> a));
        }
        return ImmutableMap.of();
    }

    private Optional<String> serializeStatsEquivalentPlan(Optional<PlanNode> root)
    {
        if (root.isPresent()) {
            try {
                return Optional.of(OBJECT_MAPPER.writeValueAsString(root));
            }
            catch (JsonProcessingException ignored) {
            }
        }
        return Optional.empty();
    }
}
