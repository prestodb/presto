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
package com.facebook.presto.server.protocol;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StageStats;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageExecutionStats;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.ws.rs.core.Response;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_ADDED_PREPARE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_ADDED_SESSION_FUNCTION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_DEALLOCATED_PREPARE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_REMOVED_SESSION_FUNCTION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_ROLE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID;
import static java.lang.String.format;

public final class QueryResourceUtil
{
    private static final JsonCodec<SqlFunctionId> SQL_FUNCTION_ID_JSON_CODEC = jsonCodec(SqlFunctionId.class);
    private static final JsonCodec<SqlInvokedFunction> SQL_INVOKED_FUNCTION_JSON_CODEC = jsonCodec(SqlInvokedFunction.class);

    private QueryResourceUtil() {}

    public static Response toResponse(Query query, QueryResults queryResults, boolean compressionEnabled)
    {
        Response.ResponseBuilder response = Response.ok(queryResults);

        // add set catalog and schema
        query.getSetCatalog().ifPresent(catalog -> response.header(PRESTO_SET_CATALOG, catalog));
        query.getSetSchema().ifPresent(schema -> response.header(PRESTO_SET_SCHEMA, schema));

        // add set session properties
        query.getSetSessionProperties()
                .forEach((key, value) -> response.header(PRESTO_SET_SESSION, key + '=' + urlEncode(value)));

        // add clear session properties
        query.getResetSessionProperties()
                .forEach(name -> response.header(PRESTO_CLEAR_SESSION, name));

        // add set roles
        query.getSetRoles()
                .forEach((key, value) -> response.header(PRESTO_SET_ROLE, key + '=' + urlEncode(value.toString())));

        // add added prepare statements
        for (Map.Entry<String, String> entry : query.getAddedPreparedStatements().entrySet()) {
            String encodedKey = urlEncode(entry.getKey());
            String encodedValue = urlEncode(entry.getValue());
            response.header(PRESTO_ADDED_PREPARE, encodedKey + '=' + encodedValue);
        }

        // add deallocated prepare statements
        for (String name : query.getDeallocatedPreparedStatements()) {
            response.header(PRESTO_DEALLOCATED_PREPARE, urlEncode(name));
        }

        // add new transaction ID
        query.getStartedTransactionId()
                .ifPresent(transactionId -> response.header(PRESTO_STARTED_TRANSACTION_ID, transactionId));

        // add clear transaction ID directive
        if (query.isClearTransactionId()) {
            response.header(PRESTO_CLEAR_TRANSACTION_ID, true);
        }

        if (!compressionEnabled) {
            response.encoding("identity");
        }

        // add added session functions
        for (Map.Entry<SqlFunctionId, SqlInvokedFunction> entry : query.getAddedSessionFunctions().entrySet()) {
            response.header(PRESTO_ADDED_SESSION_FUNCTION, format(
                    "%s=%s",
                    urlEncode(SQL_FUNCTION_ID_JSON_CODEC.toJson(entry.getKey())),
                    urlEncode(SQL_INVOKED_FUNCTION_JSON_CODEC.toJson(entry.getValue()))));
        }

        // add removed session functions
        for (SqlFunctionId signature : query.getRemovedSessionFunctions()) {
            response.header(PRESTO_REMOVED_SESSION_FUNCTION, urlEncode(SQL_FUNCTION_ID_JSON_CODEC.toJson(signature)));
        }

        return response.build();
    }

    public static StatementStats toStatementStats(QueryInfo queryInfo)
    {
        QueryStats queryStats = queryInfo.getQueryStats();
        StageInfo outputStage = queryInfo.getOutputStage().orElse(null);

        return StatementStats.builder()
                .setState(queryInfo.getState().toString())
                .setWaitingForPrerequisites(queryInfo.getState() == QueryState.WAITING_FOR_PREREQUISITES)
                .setQueued(queryInfo.getState() == QueryState.QUEUED)
                .setScheduled(queryInfo.isScheduled())
                .setNodes(globalUniqueNodes(outputStage).size())
                .setTotalSplits(queryStats.getTotalDrivers())
                .setQueuedSplits(queryStats.getQueuedDrivers())
                .setRunningSplits(queryStats.getRunningDrivers() + queryStats.getBlockedDrivers())
                .setCompletedSplits(queryStats.getCompletedDrivers())
                .setCpuTimeMillis(queryStats.getTotalCpuTime().toMillis())
                .setWallTimeMillis(queryStats.getTotalScheduledTime().toMillis())
                .setWaitingForPrerequisitesTimeMillis(queryStats.getWaitingForPrerequisitesTime().toMillis())
                .setQueuedTimeMillis(queryStats.getQueuedTime().toMillis())
                .setElapsedTimeMillis(queryStats.getElapsedTime().toMillis())
                .setProcessedRows(queryStats.getRawInputPositions())
                .setProcessedBytes(queryStats.getRawInputDataSize().toBytes())
                .setPeakMemoryBytes(queryStats.getPeakUserMemoryReservation().toBytes())
                .setPeakTotalMemoryBytes(queryStats.getPeakTotalMemoryReservation().toBytes())
                .setPeakTaskTotalMemoryBytes(queryStats.getPeakTaskTotalMemory().toBytes())
                .setSpilledBytes(queryStats.getSpilledDataSize().toBytes())
                .setRootStage(toStageStats(outputStage))
                .setRuntimeStats(queryStats.getRuntimeStats())
                .build();
    }

    private static String urlEncode(String value)
    {
        try {
            return URLEncoder.encode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    public static Set<String> globalUniqueNodes(StageInfo stageInfo)
    {
        if (stageInfo == null) {
            return ImmutableSet.of();
        }
        ImmutableSet.Builder<String> nodes = ImmutableSet.builder();
        for (TaskInfo task : stageInfo.getLatestAttemptExecutionInfo().getTasks()) {
            // todo add nodeId to TaskInfo
            URI uri = task.getTaskStatus().getSelf();
            nodes.add(uri.getHost() + ":" + uri.getPort());
        }

        for (StageInfo subStage : stageInfo.getSubStages()) {
            nodes.addAll(globalUniqueNodes(subStage));
        }
        return nodes.build();
    }

    private static StageStats toStageStats(StageInfo stageInfo)
    {
        if (stageInfo == null) {
            return null;
        }

        StageExecutionInfo currentStageExecutionInfo = stageInfo.getLatestAttemptExecutionInfo();
        StageExecutionStats stageExecutionStats = currentStageExecutionInfo.getStats();

        ImmutableList.Builder<StageStats> subStages = ImmutableList.builder();
        for (StageInfo subStage : stageInfo.getSubStages()) {
            subStages.add(toStageStats(subStage));
        }

        Set<String> uniqueNodes = new HashSet<>();
        for (TaskInfo task : currentStageExecutionInfo.getTasks()) {
            // todo add nodeId to TaskInfo
            URI uri = task.getTaskStatus().getSelf();
            uniqueNodes.add(uri.getHost() + ":" + uri.getPort());
        }

        return StageStats.builder()
                .setStageId(String.valueOf(stageInfo.getStageId().getId()))
                .setState(currentStageExecutionInfo.getState().toString())
                .setDone(currentStageExecutionInfo.getState().isDone())
                .setNodes(uniqueNodes.size())
                .setTotalSplits(stageExecutionStats.getTotalDrivers())
                .setQueuedSplits(stageExecutionStats.getQueuedDrivers())
                .setRunningSplits(stageExecutionStats.getRunningDrivers() + stageExecutionStats.getBlockedDrivers())
                .setCompletedSplits(stageExecutionStats.getCompletedDrivers())
                .setCpuTimeMillis(stageExecutionStats.getTotalCpuTime().toMillis())
                .setWallTimeMillis(stageExecutionStats.getTotalScheduledTime().toMillis())
                .setProcessedRows(stageExecutionStats.getRawInputPositions())
                .setProcessedBytes(stageExecutionStats.getRawInputDataSize().toBytes())
                .setSubStages(subStages.build())
                .build();
    }
}
