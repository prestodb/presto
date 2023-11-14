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
package io.ahana.eventplugin;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.OperatorStatistics;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.facebook.presto.spi.eventlistener.StageStatistics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.ahana.eventplugin.QueryEventListenerFactory.QUERYEVENT_JDBC_URI;
import static io.ahana.eventplugin.QueryEventListenerFactory.QUERYEVENT_JDBC_USER;
import static io.ahana.eventplugin.QueryEventListenerFactory.QUERY_EVENT_JDBC_PWD;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.Executors.newCachedThreadPool;

public final class QueryEventListener
        implements EventListener
{
    private static final DateTimeFormatter DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final Logger logger;
    private final boolean trackEventCreated;
    private final boolean trackEventCompleted;
    private final boolean trackEventCompletedSplit;
    private final String instanceId;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String clusterName;

    private boolean sendToWebSocketServer;
    private boolean useMysqlServiceCollector;
    private WebSocketCollectorChannel webSocketCollectorChannel;
    private Map<String, String> config = new HashMap<>();
    private MySQLWriter mySQLWriter;

    public QueryEventListener(
            final String clusterName,
            final LoggerContext loggerContext,
            final boolean sendToWebSocketServer,
            final String webSockerCollectUrl,
            final boolean trackEventCreated,
            final boolean trackEventCompleted,
            final boolean trackEventCompletedSplit,
            final boolean useMysqlServiceCollector,
            Map<String, String> config)
    {
        this(clusterName, loggerContext, sendToWebSocketServer, webSockerCollectUrl, trackEventCreated, trackEventCompleted, trackEventCompletedSplit);
        this.useMysqlServiceCollector = useMysqlServiceCollector;
        this.config.putAll(config);

        if (this.useMysqlServiceCollector) {
            mySQLWriter = new MySQLWriter(config);
        }
    }

    public QueryEventListener(
            final String clusterName,
            final LoggerContext loggerContext,
            final boolean sendToWebSocketServer,
            final String webSockerCollectUrl,
            final boolean trackEventCreated,
            final boolean trackEventCompleted,
            final boolean trackEventCompletedSplit)
    {
        this.instanceId = UUID.randomUUID().toString();
        this.clusterName = clusterName;
        this.trackEventCreated = trackEventCreated;
        this.trackEventCompleted = trackEventCompleted;
        this.trackEventCompletedSplit = trackEventCompletedSplit;
        this.logger = loggerContext.getLogger(QueryEventListener.class.getName());

        if (sendToWebSocketServer) {
            this.webSocketCollectorChannel = new WebSocketCollectorChannel(webSockerCollectUrl);
            this.webSocketCollectorChannel.connect();
        }

        this.mapper.registerModule(new Jdk8Module());
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        if (!trackEventCreated) {
            return;
        }

        //2023-01-10 : Since there have been several SPI changes to the OSS Presto SPI interfaces
        //We temporarily stop logging to the websocket server. This is okay since our primary means of verifying
        //results is thru the MySQL server
        //tracetoWebSocketServer_QueryCreatedEvent(queryCreatedEvent);

        // Post to mysql service
        if (useMysqlServiceCollector) {
            mySQLWriter.post(queryCreatedEvent);
        }
    }

    private void tracetoWebSocketServer_QueryCreatedEvent(QueryCreatedEvent queryCreatedEvent)
    {
        String query = queryCreatedEvent.getMetadata().getQuery();
        QueryMetadata queryMetadata = new QueryMetadata(
                queryCreatedEvent.getMetadata().getQueryId(),
                queryCreatedEvent.getMetadata().getTransactionId(),
                flatten(query),
                queryCreatedEvent.getMetadata().getQueryHash(),
                queryCreatedEvent.getMetadata().getPreparedQuery(),
                queryCreatedEvent.getMetadata().getQueryState(),
                queryCreatedEvent.getMetadata().getUri(),
                Optional.of(""),
                queryCreatedEvent.getMetadata().getJsonPlan(),
                Optional.of(""),
                queryCreatedEvent.getMetadata().getRuntimeOptimizedStages(),
                queryCreatedEvent.getMetadata().getTracingId());

        QueryCreatedEvent queryCreatedEvent1 = new QueryCreatedEvent(
                queryCreatedEvent.getCreateTime(),
                queryCreatedEvent.getContext(),
                queryMetadata);

        try {
            String eventPayload = this.mapper.writeValueAsString(new QueryEvent(this.instanceId, this.clusterName, queryCreatedEvent1, null,
                    null, flatten(queryCreatedEvent.getMetadata().getPlan().orElse("null")), 0, 0, 0, 0, 0));
            logger.info(eventPayload);
            if (sendToWebSocketServer) {
                this.webSocketCollectorChannel.sendMessage(eventPayload);
            }
        }
        catch (JsonProcessingException e) {
            logger.warn("Failed to serialize query log event", e);
        }
    }

    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        if (!trackEventCompleted) {
            return;
        }

        //2023-01-10 : Since there have been several SPI changes to the OSS Presto SPI interfaces
        //We temporarily stop logging to the websocket server. This is okay since our primary means of verifying
        //results is thru the MySQL server
        //traceToWebSocketServer_QueryCompletedEvent(queryCompletedEvent);

        // Post to mysql service
        if (useMysqlServiceCollector) {
            mySQLWriter.post(queryCompletedEvent);
        }
    }

    private void traceToWebSocketServer_QueryCompletedEvent(QueryCompletedEvent queryCompletedEvent)
    {
        String query = queryCompletedEvent.getMetadata().getQuery();
        QueryMetadata queryMetadata = new QueryMetadata(
                queryCompletedEvent.getMetadata().getQueryId(),
                queryCompletedEvent.getMetadata().getTransactionId(),
                flatten(query),
                queryCompletedEvent.getMetadata().getQueryHash(),
                queryCompletedEvent.getMetadata().getPreparedQuery(),
                queryCompletedEvent.getMetadata().getQueryState(),
                queryCompletedEvent.getMetadata().getUri(),
                Optional.of(""),
                queryCompletedEvent.getMetadata().getJsonPlan(),
                Optional.of(""),
                queryCompletedEvent.getMetadata().getRuntimeOptimizedStages(),
                queryCompletedEvent.getMetadata().getTracingId());

        QueryCompletedEvent queryCompletedEvent1 = new QueryCompletedEvent(
                queryMetadata,
                queryCompletedEvent.getStatistics(),
                queryCompletedEvent.getContext(),
                queryCompletedEvent.getIoMetadata(),
                queryCompletedEvent.getFailureInfo(),
                queryCompletedEvent.getWarnings(),
                queryCompletedEvent.getQueryType(),
                queryCompletedEvent.getFailedTasks(),
                queryCompletedEvent.getCreateTime(),
                queryCompletedEvent.getExecutionStartTime(),
                queryCompletedEvent.getEndTime(),
                queryCompletedEvent.getStageStatistics(),
                queryCompletedEvent.getOperatorStatistics(),
                queryCompletedEvent.getPlanStatisticsRead(),
                queryCompletedEvent.getPlanStatisticsWritten(),
                queryCompletedEvent.getExpandedQuery());

        try {
            // Logging for payload
            String eventPayload = this.mapper.writeValueAsString(new QueryEvent(this.instanceId, this.clusterName, null, queryCompletedEvent1,
                    null, flatten(queryCompletedEvent.getMetadata().getPlan().orElse("null")), getTimeValue(queryCompletedEvent.getStatistics().getCpuTime()),
                    getTimeValue(queryCompletedEvent.getStatistics().getRetriedCpuTime()), getTimeValue(queryCompletedEvent.getStatistics().getWallTime()), getTimeValue(queryCompletedEvent.getStatistics().getQueuedTime()),
                    getTimeValue(queryCompletedEvent.getStatistics().getAnalysisTime().orElse(null))));
            logger.info(eventPayload);
            if (sendToWebSocketServer) {
                this.webSocketCollectorChannel.sendMessage(eventPayload);
            }
        }
        catch (JsonProcessingException e) {
            logger.warn("Failed to serialize query log event", e);
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        if (!trackEventCompletedSplit) {
            return;
        }

        try {
            String eventPayload = this.mapper.writeValueAsString(new QueryEvent(this.instanceId, this.clusterName, null, null,
                    splitCompletedEvent, null, getTimeValue(splitCompletedEvent.getStatistics().getCpuTime()), 0,
                    getTimeValue(splitCompletedEvent.getStatistics().getWallTime()), getTimeValue(splitCompletedEvent.getStatistics().getQueuedTime()), 0));
            logger.info(eventPayload);
            if (sendToWebSocketServer) {
                this.webSocketCollectorChannel.sendMessage(eventPayload);
            }
        }
        catch (JsonProcessingException e) {
            logger.warn("Failed to serialize query log event", e);
        }
    }

    private String flatten(String query)
    {
        return (Optional.ofNullable(query).isPresent())
                ? query.replaceAll("\n", "<<>>") : "";
    }

    private long getTimeValue(Duration duration)
    {
        return Optional.ofNullable(duration).isPresent()
                ? duration.toMillis() : 0L;
    }

    private class MySQLWriter
    {
        private static final int MAX_THREADS_FOR_SQL_UPDATE = 10;
        private final Executor executor = new BoundedExecutor(newCachedThreadPool(daemonThreadsNamed("mysql-writer-%s")), MAX_THREADS_FOR_SQL_UPDATE);

        private final Jdbi dbi;
        private PrestoQueryStatsDao dao;

        MySQLWriter(Map<String, String> config)
        {
            dbi = Jdbi.create(
                    config.get(QUERYEVENT_JDBC_URI),
                    config.get(QUERYEVENT_JDBC_USER),
                    config.get(QUERY_EVENT_JDBC_PWD));
            dbi.installPlugin(new SqlObjectPlugin());
            dao = dbi.onDemand(PrestoQueryStatsDao.class);
        }

        private synchronized void executeSqlUpdate(Runnable runnable) throws PrestoException
        {
            executor.execute(() -> {
                try {
                    runnable.run();
                }
                catch (RuntimeException ex) {
                    logger.warn(ex.getMessage(), "Failed to write query stats for query to MySQL.");
                    throw new PrestoException(StandardErrorCode.DISTRIBUTED_TRACING_ERROR, "Failed to write query stats for query to MySQL. \n", ex.getCause());
                }
            });
        }

        private void post(QueryCompletedEvent queryCompletedEvent)
        {
            /**
             * EXPORT TO TABLE: presto_query_plans
             */
            postQueryPlans(queryCompletedEvent);
            /**
             *  EXPORT TO TABLE: presto_query_stage_stats
             */
            postQueryStageStats(queryCompletedEvent.getMetadata().getQueryId(), queryCompletedEvent.getStageStatistics(), queryCompletedEvent.getCreateTime());
            /**
             * EXPORT TO TABLE: presto_query_operator_stats
             */
            postQueryOperatorStats(
                    queryCompletedEvent.getMetadata().getQueryId(),
                    queryCompletedEvent.getOperatorStatistics(),
                    queryCompletedEvent.getCreateTime());
            /**
             * EXPORT TO TABLE: presto_query_statistics
             */
            postQueryStatistics(queryCompletedEvent);
        }

        private void post(QueryCreatedEvent queryCreatedEvent)
        {
            /**
             * EXPORT TO TABLE: presto_query_creation_info
             */
            postQueryCreationInfo(queryCreatedEvent);
        }

        private void postQueryCreationInfo(QueryCreatedEvent queryCreatedEvent)
        {
            executeSqlUpdate(() -> dao.insertQueryCreationInfo(
                    queryCreatedEvent.getMetadata().getQueryId(),
                    queryCreatedEvent.getMetadata().getQuery(),
                    queryCreatedEvent.getCreateTime().atZone(UTC).format(DATETIME_FORMAT),
                    queryCreatedEvent.getContext().getSchema().orElse("null"),
                    queryCreatedEvent.getContext().getCatalog().orElse("null"),
                    queryCreatedEvent.getContext().getEnvironment(),
                    queryCreatedEvent.getContext().getUser(),
                    queryCreatedEvent.getContext().getRemoteClientAddress().orElse("null"),
                    queryCreatedEvent.getContext().getSource().orElse("null"),
                    queryCreatedEvent.getContext().getUserAgent().orElse("null"),
                    queryCreatedEvent.getMetadata().getUri().toASCIIString(),
                    queryCreatedEvent.getContext().getSessionProperties().toString(),
                    queryCreatedEvent.getContext().getServerVersion(),
                    queryCreatedEvent.getContext().getClientInfo().orElse("null"),
                    queryCreatedEvent.getContext().getResourceGroupId().toString(),
                    queryCreatedEvent.getContext().getPrincipal().orElse("null"),
                    queryCreatedEvent.getMetadata().getTransactionId().toString(),
                    queryCreatedEvent.getContext().getClientTags().toString(),
                    queryCreatedEvent.getContext().getResourceEstimates().toString(),
                    queryCreatedEvent.getCreateTime().atZone(UTC).format(DATETIME_FORMAT)));
        }

        private void postQueryPlans(QueryCompletedEvent queryCompletedEvent)
        {
            executeSqlUpdate(() -> dao.insertQueryPlans(
                    queryCompletedEvent.getMetadata().getQueryId(),
                    queryCompletedEvent.getMetadata().getQuery(),
                    queryCompletedEvent.getMetadata().getPlan().orElse("null"),
                    queryCompletedEvent.getMetadata().getJsonPlan().orElse("null"),
                    queryCompletedEvent.getContext().getEnvironment(),
                    queryCompletedEvent.getCreateTime().atZone(UTC).format(DATETIME_FORMAT)));
        }

        private void postQueryStatistics(QueryCompletedEvent queryCompletedEvent)
        {
            boolean queryFailed = queryCompletedEvent.getFailureInfo().isPresent();
            if (queryFailed) {
                executeSqlUpdate(() -> dao.insertQueryStatistics(
                        queryCompletedEvent.getMetadata().getQueryId(),
                        queryCompletedEvent.getMetadata().getQuery(),
                        queryCompletedEvent.getQueryType().isPresent() ? queryCompletedEvent.getQueryType().get().toString() : null,
                        queryCompletedEvent.getContext().getSchema().orElse(null),
                        queryCompletedEvent.getContext().getCatalog().orElse(null),
                        queryCompletedEvent.getContext().getEnvironment(),
                        queryCompletedEvent.getContext().getUser(),
                        queryCompletedEvent.getContext().getRemoteClientAddress().orElse(null),
                        queryCompletedEvent.getContext().getSource().orElse(null),
                        queryCompletedEvent.getContext().getUserAgent().orElse(null),
                        queryCompletedEvent.getMetadata().getUri().toASCIIString(),
                        queryCompletedEvent.getContext().getSessionProperties().toString(),
                        queryCompletedEvent.getContext().getServerVersion(),
                        queryCompletedEvent.getContext().getClientInfo().orElse(null),
                        queryCompletedEvent.getContext().getResourceGroupId().isPresent() ? queryCompletedEvent.getContext().getResourceGroupId().get().toString() : null,
                        queryCompletedEvent.getContext().getPrincipal().orElse(null),
                        queryCompletedEvent.getMetadata().getTransactionId().orElse(null),
                        queryCompletedEvent.getContext().getClientTags().toString(),
                        queryCompletedEvent.getContext().getResourceEstimates().toString(),
                        queryCompletedEvent.getCreateTime().atZone(UTC).format(DATETIME_FORMAT),
                        queryCompletedEvent.getEndTime().atZone(UTC).format(DATETIME_FORMAT),
                        queryCompletedEvent.getExecutionStartTime().atZone(UTC).format(DATETIME_FORMAT),
                        queryCompletedEvent.getMetadata().getQueryState(),
                        queryCompletedEvent.getFailureInfo().get().getFailureMessage().orElse(null),
                        queryCompletedEvent.getFailureInfo().get().getFailureType().orElse(null),
                        queryCompletedEvent.getFailureInfo().get().getFailuresJson(),
                        queryCompletedEvent.getFailureInfo().get().getFailureTask().orElse(null),
                        queryCompletedEvent.getFailureInfo().get().getFailureHost().orElse(null),
                        queryCompletedEvent.getFailureInfo().get().getErrorCode().getCode(),
                        queryCompletedEvent.getFailureInfo().get().getErrorCode().getName(),
                        queryCompletedEvent.getFailureInfo().get().getErrorCode().getType().toString(),
                        Arrays.toString(queryCompletedEvent.getWarnings().toArray()),
                        queryCompletedEvent.getStatistics().getCompletedSplits(),
                        queryCompletedEvent.getStatistics().getAnalysisTime().isPresent() ? queryCompletedEvent.getStatistics().getAnalysisTime().get().toMillis() : 0,
                        queryCompletedEvent.getStatistics().getQueuedTime().toMillis(),
                        queryCompletedEvent.getStatistics().getWallTime().toMillis(),
                        queryCompletedEvent.getContext().getResourceEstimates().getExecutionTime().isPresent() ? queryCompletedEvent.getContext().getResourceEstimates().getExecutionTime().get().toMillis() : 0,
                        queryCompletedEvent.getStatistics().getCpuTime().toMillis() == 0 ? 0 : queryCompletedEvent.getStatistics().getTotalBytes() / queryCompletedEvent.getStatistics().getCpuTime().toMillis(),
                        queryCompletedEvent.getContext().getResourceEstimates().getExecutionTime().isPresent() ? queryCompletedEvent.getStatistics().getTotalBytes() / queryCompletedEvent.getContext().getResourceEstimates().getExecutionTime().get().toMillis() : 0,
                        queryCompletedEvent.getStatistics().getTotalRows() == 0 ? 0 : queryCompletedEvent.getStatistics().getTotalRows() / queryCompletedEvent.getStatistics().getCpuTime().toMillis(),
                        queryCompletedEvent.getStatistics().getTotalBytes(),
                        queryCompletedEvent.getStatistics().getTotalRows(),
                        queryCompletedEvent.getStatistics().getOutputRows(),
                        queryCompletedEvent.getStatistics().getOutputBytes(),
                        queryCompletedEvent.getStatistics().getWrittenOutputRows(),
                        queryCompletedEvent.getStatistics().getWrittenOutputBytes(),
                        queryCompletedEvent.getStatistics().getCumulativeMemory(),
                        queryCompletedEvent.getStatistics().getPeakUserMemoryBytes(),
                        queryCompletedEvent.getStatistics().getPeakTotalNonRevocableMemoryBytes(),
                        queryCompletedEvent.getStatistics().getPeakTaskTotalMemory(),
                        queryCompletedEvent.getStatistics().getPeakUserMemoryBytes(),
                        queryCompletedEvent.getStatistics().getWrittenOutputBytes(),
                        queryCompletedEvent.getStatistics().getPeakNodeTotalMemory(),
                        queryCompletedEvent.getStatistics().getCpuTime().toMillis(),
                        queryCompletedEvent.getStageStatistics().size(),
                        queryCompletedEvent.getStatistics().getCumulativeMemory(),
                        queryCompletedEvent.getCreateTime().atZone(UTC).format(DATETIME_FORMAT)));
            }
            else {
                executeSqlUpdate(() -> dao.insertQueryStatistics(
                        queryCompletedEvent.getMetadata().getQueryId(),
                        queryCompletedEvent.getMetadata().getQuery(),
                        queryCompletedEvent.getQueryType().isPresent() ? queryCompletedEvent.getQueryType().get().toString() : null,
                        queryCompletedEvent.getContext().getSchema().orElse(null),
                        queryCompletedEvent.getContext().getCatalog().orElse(null),
                        queryCompletedEvent.getContext().getEnvironment(),
                        queryCompletedEvent.getContext().getUser(),
                        queryCompletedEvent.getContext().getRemoteClientAddress().orElse(null),
                        queryCompletedEvent.getContext().getSource().orElse(null),
                        queryCompletedEvent.getContext().getUserAgent().orElse(null),
                        queryCompletedEvent.getMetadata().getUri().toASCIIString(),
                        queryCompletedEvent.getContext().getSessionProperties().toString(),
                        queryCompletedEvent.getContext().getServerVersion(),
                        queryCompletedEvent.getContext().getClientInfo().orElse(null),
                        queryCompletedEvent.getContext().getResourceGroupId().isPresent() ? queryCompletedEvent.getContext().getResourceGroupId().get().toString() : null,
                        queryCompletedEvent.getContext().getPrincipal().orElse(null),
                        queryCompletedEvent.getMetadata().getTransactionId().orElse(null),
                        queryCompletedEvent.getContext().getClientTags().toString(),
                        queryCompletedEvent.getContext().getResourceEstimates().toString(),
                        queryCompletedEvent.getCreateTime().atZone(UTC).format(DATETIME_FORMAT),
                        queryCompletedEvent.getEndTime().atZone(UTC).format(DATETIME_FORMAT),
                        queryCompletedEvent.getExecutionStartTime().atZone(UTC).format(DATETIME_FORMAT),
                        queryCompletedEvent.getMetadata().getQueryState(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        0,
                        null,
                        null,
                        Arrays.toString(queryCompletedEvent.getWarnings().toArray()),
                        queryCompletedEvent.getStatistics().getCompletedSplits(),
                        queryCompletedEvent.getStatistics().getAnalysisTime().isPresent() ? queryCompletedEvent.getStatistics().getAnalysisTime().get().toMillis() : 0,
                        queryCompletedEvent.getStatistics().getQueuedTime().toMillis(),
                        queryCompletedEvent.getStatistics().getWallTime().toMillis(),
                        queryCompletedEvent.getContext().getResourceEstimates().getExecutionTime().isPresent() ? queryCompletedEvent.getContext().getResourceEstimates().getExecutionTime().get().toMillis() : 0,
                        queryCompletedEvent.getStatistics().getCpuTime().toMillis() == 0 ? 0 : queryCompletedEvent.getStatistics().getTotalBytes() / queryCompletedEvent.getStatistics().getCpuTime().toMillis(),
                        queryCompletedEvent.getContext().getResourceEstimates().getExecutionTime().isPresent() ? queryCompletedEvent.getStatistics().getTotalBytes() / queryCompletedEvent.getContext().getResourceEstimates().getExecutionTime().get().toMillis() : 0,
                        queryCompletedEvent.getStatistics().getTotalRows() == 0 ? 0 : queryCompletedEvent.getStatistics().getTotalRows() / queryCompletedEvent.getStatistics().getCpuTime().toMillis(),
                        queryCompletedEvent.getStatistics().getTotalBytes(),
                        queryCompletedEvent.getStatistics().getTotalRows(),
                        queryCompletedEvent.getStatistics().getOutputRows(),
                        queryCompletedEvent.getStatistics().getOutputBytes(),
                        queryCompletedEvent.getStatistics().getWrittenOutputRows(),
                        queryCompletedEvent.getStatistics().getWrittenOutputBytes(),
                        queryCompletedEvent.getStatistics().getCumulativeMemory(),
                        queryCompletedEvent.getStatistics().getPeakUserMemoryBytes(),
                        queryCompletedEvent.getStatistics().getPeakTotalNonRevocableMemoryBytes(),
                        queryCompletedEvent.getStatistics().getPeakTaskTotalMemory(),
                        queryCompletedEvent.getStatistics().getPeakUserMemoryBytes(),
                        queryCompletedEvent.getStatistics().getWrittenOutputBytes(),
                        queryCompletedEvent.getStatistics().getPeakNodeTotalMemory(),
                        queryCompletedEvent.getStatistics().getCpuTime().toMillis(),
                        queryCompletedEvent.getStageStatistics().size(),
                        queryCompletedEvent.getStatistics().getCumulativeMemory(),
                        queryCompletedEvent.getCreateTime().atZone(UTC).format(DATETIME_FORMAT)));
            }
        }

        private void postQueryStageStats(String queryId, List<StageStatistics> stageStatistics, Instant createTime)
        {
            for (StageStatistics stageStatistic : stageStatistics) {
                executeSqlUpdate(() -> dao.insertQueryStageStats(
                        queryId,
                        stageStatistic.getStageId(),
                        stageStatistic.getStageExecutionId(),
                        stageStatistic.getTasks(),
                        stageStatistic.getTotalScheduledTime().toMillis(),
                        stageStatistic.getTotalCpuTime().toMillis(),
                        stageStatistic.getRetriedCpuTime().toMillis(),
                        stageStatistic.getTotalBlockedTime().toMillis(),
                        stageStatistic.getRawInputDataSize().toBytes(),
                        stageStatistic.getProcessedInputDataSize().toBytes(),
                        stageStatistic.getPhysicalWrittenDataSize().toBytes(),
                        stageStatistic.getGcStatistics().toString(),
                        stageStatistic.getCpuDistribution().toString(),
                        stageStatistic.getMemoryDistribution().toString(),
                        createTime.atZone(UTC).format(DATETIME_FORMAT)));
            }
        }

        private void postQueryOperatorStats(String queryId, List<OperatorStatistics> operatorStatistics, Instant createTime)
        {
            for (OperatorStatistics operatorStatistic : operatorStatistics) {
                executeSqlUpdate(() -> dao.insertQueryOperatorStats(
                        queryId,
                        operatorStatistic.getStageId(),
                        operatorStatistic.getStageExecutionId(),
                        operatorStatistic.getPipelineId(),
                        operatorStatistic.getOperatorId(),
                        operatorStatistic.getPlanNodeId().toString(),
                        operatorStatistic.getOperatorType(),
                        operatorStatistic.getTotalDrivers(),
                        operatorStatistic.getAddInputCalls(),
                        operatorStatistic.getAddInputWall().toMillis(),
                        operatorStatistic.getAddInputCpu().toMillis(),
                        operatorStatistic.getAddInputAllocation().toBytes(),
                        operatorStatistic.getRawInputDataSize().toBytes(),
                        operatorStatistic.getRawInputPositions(),
                        operatorStatistic.getInputDataSize().toBytes(),
                        operatorStatistic.getInputPositions(),
                        operatorStatistic.getSumSquaredInputPositions(),
                        operatorStatistic.getGetOutputCalls(),
                        operatorStatistic.getGetOutputWall().toMillis(),
                        operatorStatistic.getGetOutputCpu().toMillis(),
                        operatorStatistic.getGetOutputAllocation().toBytes(),
                        operatorStatistic.getOutputDataSize().toBytes(),
                        operatorStatistic.getOutputPositions(),
                        operatorStatistic.getPhysicalWrittenDataSize().toBytes(),
                        operatorStatistic.getBlockedWall().toMillis(),
                        operatorStatistic.getFinishCalls(),
                        operatorStatistic.getFinishWall().toMillis(),
                        operatorStatistic.getFinishCpu().toMillis(),
                        operatorStatistic.getFinishAllocation().toBytes(),
                        operatorStatistic.getUserMemoryReservation().toBytes(),
                        operatorStatistic.getRevocableMemoryReservation().toBytes(),
                        operatorStatistic.getSystemMemoryReservation().toBytes(),
                        operatorStatistic.getPeakUserMemoryReservation().toBytes(),
                        operatorStatistic.getPeakSystemMemoryReservation().toBytes(),
                        operatorStatistic.getPeakTotalMemoryReservation().toBytes(),
                        operatorStatistic.getSpilledDataSize().toBytes(),
                        operatorStatistic.getInfo().orElse("null"),
                        createTime.atZone(UTC).format(DATETIME_FORMAT)));
            }
        }
    }
}
