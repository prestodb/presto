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
import com.facebook.airlift.log.Logger;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StageStats;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.ParameterKind;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
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
import com.google.common.collect.Sets;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.json.JsonCodec.mapJsonCodec;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_ADDED_PREPARE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_ADDED_SESSION_FUNCTION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_DEALLOCATED_PREPARE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREFIX_URL;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_REMOVED_SESSION_FUNCTION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_ROLE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID;
import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;

public final class QueryResourceUtil
{
    private static final Logger log = Logger.get(QueryResourceUtil.class);
    private static final JsonCodec<SqlFunctionId> SQL_FUNCTION_ID_JSON_CODEC = jsonCodec(SqlFunctionId.class);
    private static final JsonCodec<SqlInvokedFunction> SQL_INVOKED_FUNCTION_JSON_CODEC = jsonCodec(SqlInvokedFunction.class);
    private static final JsonCodec<List<Object>> LIST_JSON_CODEC = listJsonCodec(Object.class);
    private static final JsonCodec<Map<Object, Object>> MAP_JSON_CODEC = mapJsonCodec(Object.class, Object.class);

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

    public static Response toResponse(Query query, QueryResults queryResults, String xPrestoPrefixUri, boolean compressionEnabled, boolean nestedDataSerializationEnabled)
    {
        Iterable<List<Object>> queryResultsData = queryResults.getData();
        if (nestedDataSerializationEnabled) {
            queryResultsData = prepareJsonData(queryResults.getColumns(), queryResultsData);
        }
        QueryResults resultsClone = new QueryResults(
                queryResults.getId(),
                prependUri(queryResults.getInfoUri(), xPrestoPrefixUri),
                prependUri(queryResults.getPartialCancelUri(), xPrestoPrefixUri),
                prependUri(queryResults.getNextUri(), xPrestoPrefixUri),
                queryResults.getColumns(),
                queryResultsData,
                queryResults.getBinaryData(),
                queryResults.getStats(),
                queryResults.getError(),
                queryResults.getWarnings(),
                queryResults.getUpdateType(),
                queryResults.getUpdateCount());

        return toResponse(query, resultsClone, compressionEnabled);
    }

    public static void abortIfPrefixUrlInvalid(String xPrestoPrefixUrl)
    {
        if (xPrestoPrefixUrl != null) {
            try {
                URL url = new URL(xPrestoPrefixUrl);
            }
            catch (java.net.MalformedURLException e) {
                throw new WebApplicationException(
                        Response.status(Response.Status.BAD_REQUEST)
                                .type(TEXT_PLAIN_TYPE)
                                .entity(PRESTO_PREFIX_URL + " is not a valid URL")
                                .build());
            }
        }
    }

    public static URI prependUri(URI backendUri, String xPrestoPrefixUrl)
    {
        if (!isNullOrEmpty(xPrestoPrefixUrl) && (backendUri != null)) {
            String encodedBackendUri = Base64.getUrlEncoder().encodeToString(backendUri.toASCIIString().getBytes());

            try {
                return new URI(xPrestoPrefixUrl + encodedBackendUri);
            }
            catch (URISyntaxException e) {
                log.error(e, "Unable to add Proxy Prefix to URL");
            }
        }

        return backendUri;
    }

    public static StatementStats toStatementStats(QueryInfo queryInfo)
    {
        QueryStats queryStats = queryInfo.getQueryStats();
        StageInfo outputStage = queryInfo.getOutputStage().orElse(null);

        Set<String> globalUniqueNodes = new HashSet<>();
        StageStats rootStageStats = toStageStats(outputStage, globalUniqueNodes);

        return StatementStats.builder()
                .setState(queryInfo.getState().toString())
                .setWaitingForPrerequisites(queryInfo.getState() == QueryState.WAITING_FOR_PREREQUISITES)
                .setQueued(queryInfo.getState() == QueryState.QUEUED)
                .setScheduled(queryInfo.isScheduled())
                .setNodes(globalUniqueNodes.size())
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
                .setRootStage(rootStageStats)
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

    private static StageStats toStageStats(StageInfo stageInfo, Set<String> globalUniqueNodeIds)
    {
        if (stageInfo == null) {
            return null;
        }

        StageExecutionInfo currentStageExecutionInfo = stageInfo.getLatestAttemptExecutionInfo();
        StageExecutionStats stageExecutionStats = currentStageExecutionInfo.getStats();

        // Store current stage details into a builder
        StageStats.Builder builder = StageStats.builder()
                .setStageId(String.valueOf(stageInfo.getStageId().getId()))
                .setState(currentStageExecutionInfo.getState().toString())
                .setDone(currentStageExecutionInfo.getState().isDone())
                .setTotalSplits(stageExecutionStats.getTotalDrivers())
                .setQueuedSplits(stageExecutionStats.getQueuedDrivers())
                .setRunningSplits(stageExecutionStats.getRunningDrivers() + stageExecutionStats.getBlockedDrivers())
                .setCompletedSplits(stageExecutionStats.getCompletedDrivers())
                .setCpuTimeMillis(stageExecutionStats.getTotalCpuTime().toMillis())
                .setWallTimeMillis(stageExecutionStats.getTotalScheduledTime().toMillis())
                .setProcessedRows(stageExecutionStats.getRawInputPositions())
                .setProcessedBytes(stageExecutionStats.getRawInputDataSize().toBytes())
                .setNodes(countStageAndAddGlobalUniqueNodes(currentStageExecutionInfo.getTasks(), globalUniqueNodeIds));

        // Recurse into child stages to create their StageStats
        List<StageInfo> subStages = stageInfo.getSubStages();
        if (subStages.isEmpty()) {
            builder.setSubStages(ImmutableList.of());
        }
        else {
            ImmutableList.Builder<StageStats> subStagesBuilder = ImmutableList.builderWithExpectedSize(subStages.size());
            for (StageInfo subStage : subStages) {
                subStagesBuilder.add(toStageStats(subStage, globalUniqueNodeIds));
            }
            builder.setSubStages(subStagesBuilder.build());
        }

        return builder.build();
    }

    private static int countStageAndAddGlobalUniqueNodes(List<TaskInfo> tasks, Set<String> globalUniqueNodes)
    {
        Set<String> stageUniqueNodes = Sets.newHashSetWithExpectedSize(tasks.size());
        for (TaskInfo task : tasks) {
            String nodeId = task.getNodeId();
            stageUniqueNodes.add(nodeId);
            globalUniqueNodes.add(nodeId);
        }
        return stageUniqueNodes.size();
    }

    /**
     * Problem: As the type of data defined in QueryResult is `Iterable<List<Object>>`,
     * when jackson serialize a data with nested data structure in the response, the nested object won't
     * be serialized but be printed as string. For example the map will be printed as "{1=2}", which cannot
     * be recognized and deserialized by client side.
     * Solution: We pre-serialize the data nested in the Object to JSON by following parseTypeSignature,
     * only Objects contains nested structures will be pre-serialized, otherwise the object will simply
     * be returned. Then we can deserialize the JSON in the client side.
     */
    private static Iterable<List<Object>> prepareJsonData(List<Column> columns, Iterable<List<Object>> data)
    {
        if (data == null) {
            return null;
        }
        requireNonNull(columns, "columns is null");
        List<TypeSignature> signatures = columns.stream()
                .map(column -> parseTypeSignature(column.getType()))
                .collect(toList());
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        for (List<Object> row : data) {
            checkArgument(row.size() == columns.size(), "row/column size mismatch");
            List<Object> newRow = new ArrayList<>();
            for (int i = 0; i < row.size(); i++) {
                newRow.add(parseToJson(signatures.get(i), row.get(i)));
            }
            rows.add(unmodifiableList(newRow)); // allow nulls in list
        }
        return rows.build();
    }

    public static Object parseToJson(TypeSignature signature, Object value)
    {
        if (value == null) {
            return null;
        }
        if (signature.isDistinctType()) {
            return parseToJson(signature.getDistinctTypeInfo().getBaseType(), value);
        }
        if (signature.getBase().equals(ARRAY)) {
            List<Object> parsedValue = new ArrayList<>();
            for (Object object : List.class.cast(value)) {
                parsedValue.add(parseToJson(signature.getTypeParametersAsTypeSignatures().get(0), object));
            }
            return LIST_JSON_CODEC.toJson(parsedValue);
        }
        if (signature.getBase().equals(MAP)) {
            TypeSignature keySignature = signature.getTypeParametersAsTypeSignatures().get(0);
            TypeSignature valueSignature = signature.getTypeParametersAsTypeSignatures().get(1);
            Map<Object, Object> parsedValue = new HashMap<>(Map.class.cast(value).size());
            for (Map.Entry<?, ?> entry : (Set<Map.Entry<?, ?>>) Map.class.cast(value).entrySet()) {
                parsedValue.put(parseToJson(keySignature, entry.getKey()), parseToJson(valueSignature, entry.getValue()));
            }
            return MAP_JSON_CODEC.toJson(parsedValue);
        }
        if (signature.getBase().equals(ROW)) {
            List<Object> parsedValue = new ArrayList<>();
            for (int i = 0; i < List.class.cast(value).size(); i++) {
                Object object = List.class.cast(value).get(i);
                TypeSignatureParameter parameter = signature.getParameters().get(i);
                checkArgument(
                        parameter.getKind() == ParameterKind.NAMED_TYPE,
                        "Unexpected parameter [%s] for row type",
                        parameter);
                NamedTypeSignature namedTypeSignature = parameter.getNamedTypeSignature();
                parsedValue.add(parseToJson(namedTypeSignature.getTypeSignature(), object));
            }
            return LIST_JSON_CODEC.toJson(parsedValue);
        }
        return value;
    }
}
