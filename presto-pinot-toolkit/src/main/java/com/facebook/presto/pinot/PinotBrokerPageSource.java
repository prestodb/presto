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
package com.facebook.presto.pinot;

import com.facebook.airlift.http.client.Request;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.JsonType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.pinot.auth.PinotBrokerAuthenticationProvider;
import com.facebook.presto.pinot.query.PinotQueryGenerator;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.pinot.spi.utils.BytesUtils;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_EXCEPTION;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_INSUFFICIENT_SERVER_RESPONSE;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_REQUEST_GENERATOR_FAILURE;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNAUTHENTICATED_EXCEPTION;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNEXPECTED_RESPONSE;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static com.facebook.presto.pinot.PinotUtils.doWithRetries;
import static com.facebook.presto.pinot.PinotUtils.parseDouble;
import static com.facebook.presto.pinot.PinotUtils.parseTimestamp;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Long.parseLong;
import static java.util.Objects.requireNonNull;

public class PinotBrokerPageSource
        implements ConnectorPageSource
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final List<Class> SUPPORTED_PRESTO_COLUMN_TYPE_CLASSES = ImmutableList.of(
            FixedWidthType.class,
            VarcharType.class,
            JsonType.class,
            VarbinaryType.class);
    private static final String REQUEST_PAYLOAD_KEY = "sql";
    private static final String QUERY_URL_TEMPLATE = "%s://%s/query/sql";

    private final PinotQueryGenerator.GeneratedPinotQuery brokerSql;
    private final List<PinotColumnHandle> expectedHandles;

    protected final PinotConfig pinotConfig;
    protected final List<PinotColumnHandle> columnHandles;
    protected final PinotClusterInfoFetcher clusterInfoFetcher;
    protected final ConnectorSession session;
    protected final ObjectMapper objectMapper;
    protected final PinotBrokerAuthenticationProvider brokerAuthenticationProvider;

    protected boolean finished;
    protected long readTimeNanos;
    protected long completedBytes;

    public PinotBrokerPageSource(
            PinotConfig pinotConfig,
            ConnectorSession session,
            PinotQueryGenerator.GeneratedPinotQuery brokerSql,
            List<PinotColumnHandle> columnHandles,
            List<PinotColumnHandle> expectedHandles,
            PinotClusterInfoFetcher clusterInfoFetcher,
            ObjectMapper objectMapper,
            PinotBrokerAuthenticationProvider brokerAuthenticationProvider)
    {
        this.pinotConfig = requireNonNull(pinotConfig, "pinot config is null");
        this.clusterInfoFetcher = requireNonNull(clusterInfoFetcher, "cluster info fetcher is null");
        this.columnHandles = ImmutableList.copyOf(columnHandles);
        this.session = requireNonNull(session, "session is null");
        this.objectMapper = requireNonNull(objectMapper, "object mapper is null");
        this.brokerAuthenticationProvider = brokerAuthenticationProvider;
        this.expectedHandles = requireNonNull(expectedHandles, "expected handles is null");
        this.brokerSql = requireNonNull(brokerSql, "broker is null");
    }

    protected void setValue(Type type, BlockBuilder blockBuilder, JsonNode value)
    {
        if (blockBuilder == null) {
            return;
        }
        if (value == null) {
            blockBuilder.appendNull();
            return;
        }
        if (type instanceof ArrayType) {
            checkState(value.isArray());

            BlockBuilder childBuilder = blockBuilder.beginBlockEntry();
            ArrayNode arrayNode = (ArrayNode) value;
            for (int i = 0; i < arrayNode.size(); i++) {
                setValue(((ArrayType) type).getElementType(), childBuilder, asText(arrayNode.get(i)));
            }
            blockBuilder.closeEntry();
        }
        else {
            setValue(type, blockBuilder, asText(value));
        }
    }

    protected void setValue(Type type, BlockBuilder blockBuilder, String value)
    {
        if (blockBuilder == null) {
            return;
        }
        if (value == null) {
            blockBuilder.appendNull();
            return;
        }
        if (!isTypeSupportInPinot(type)) {
            throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "type '" + type + "' not supported");
        }
        if (type instanceof FixedWidthType) {
            completedBytes += ((FixedWidthType) type).getFixedSize();
            if (type instanceof BigintType) {
                type.writeLong(blockBuilder, parseDouble(value).longValue());
            }
            else if (type instanceof IntegerType) {
                blockBuilder.writeInt(parseDouble(value).intValue());
            }
            else if (type instanceof TinyintType) {
                blockBuilder.writeByte(parseDouble(value).byteValue());
            }
            else if (type instanceof SmallintType) {
                blockBuilder.writeShort(parseDouble(value).shortValue());
            }
            else if (type instanceof BooleanType) {
                type.writeBoolean(blockBuilder, parseBoolean(value));
            }
            else if (type instanceof DecimalType || type instanceof DoubleType) {
                type.writeDouble(blockBuilder, parseDouble(value));
            }
            else if (type instanceof TimestampType) {
                type.writeLong(blockBuilder, parseTimestamp(value));
            }
            else if (type instanceof DateType) {
                type.writeLong(blockBuilder, parseLong(value));
            }
            else {
                throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "type '" + type + "' not supported");
            }
        }
        else if (type instanceof VarbinaryType) {
            // Pinot broker convert bytes to hex string, so we need to decode the hex string back to bytes.
            type.writeSlice(blockBuilder, Slices.wrappedBuffer(BytesUtils.toBytes(value)));
        }
        else {
            Slice slice = Slices.utf8Slice(value);
            blockBuilder.writeBytes(slice, 0, slice.length()).closeEntry();
            completedBytes += slice.length();
        }
    }

    private boolean isTypeSupportInPinot(Type type)
    {
        for (Class clazz : SUPPORTED_PRESTO_COLUMN_TYPE_CLASSES) {
            if (clazz.isInstance(type)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getCompletedPositions()
    {
        return 0; // not available
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (finished) {
            return null;
        }

        long start = System.nanoTime();
        try {
            BlockAndTypeBuilder blockAndTypeBuilder = buildBlockAndTypeBuilder(columnHandles, brokerSql);
            int counter = issueQueryAndPopulate(
                    brokerSql,
                    Collections.unmodifiableList(blockAndTypeBuilder.getColumnBlockBuilders()),
                    Collections.unmodifiableList(blockAndTypeBuilder.getColumnTypes()));

            PageBuilder pageBuilder = blockAndTypeBuilder.getPageBuilder();
            pageBuilder.declarePositions(counter);
            Page page = pageBuilder.build();

            // TODO: Implement chunking if the result set is ginormous
            finished = true;

            return page;
        }
        finally {
            readTimeNanos += System.nanoTime() - start;
        }
    }

    protected void setRows(String query, List<BlockBuilder> blockBuilders, List<Type> types, JsonNode rows)
    {
        for (int rowNumber = 0; rowNumber < rows.size(); ++rowNumber) {
            JsonNode result = rows.get(rowNumber);
            if (result == null || result.size() < blockBuilders.size()) {
                throw new PinotException(
                    PINOT_UNEXPECTED_RESPONSE,
                    Optional.of(query),
                    String.format("Expected row of %d columns", blockBuilders.size()));
            }
            for (int columnNumber = 0; columnNumber < blockBuilders.size(); columnNumber++) {
                setValue(types.get(columnNumber), blockBuilders.get(columnNumber), result.get(columnNumber));
            }
        }
    }

    protected static void handleCommonResponse(String pinotQuery, JsonNode jsonBody)
    {
        JsonNode numServersResponded = jsonBody.get("numServersResponded");
        JsonNode numServersQueried = jsonBody.get("numServersQueried");

        if (numServersQueried == null || numServersResponded == null || numServersQueried.asInt() > numServersResponded.asInt()) {
            throw new PinotException(
                PINOT_INSUFFICIENT_SERVER_RESPONSE,
                Optional.of(pinotQuery),
                String.format("Only %s out of %s servers responded for query %s", numServersResponded.asInt(), numServersQueried.asInt(), pinotQuery));
        }

        JsonNode exceptions = jsonBody.get("exceptions");
        if (exceptions != null && exceptions.isArray() && exceptions.size() > 0) {
            if (exceptions.get(0).get("errorCode").asInt() == 180) {
                throw new PinotException(
                    PINOT_UNAUTHENTICATED_EXCEPTION,
                    Optional.empty(),
                    "Query authentication failed.");
            }
            // Pinot is known to return exceptions with benign errorcodes like 200
            // so we treat any exception as an error
            throw new PinotException(
                PINOT_EXCEPTION,
                Optional.of(pinotQuery),
                String.format("Query %s encountered exception %s", pinotQuery, exceptions.get(0)));
        }
    }

    protected static String asText(JsonNode node)
    {
        if (node.isArray()) {
            String[] results = new String[node.size()];
            for (int i = 0; i < node.size(); i++) {
                results[i] = asText(node.get(i));
            }
            return Arrays.toString(results);
        }
        checkState(node.isValueNode());
        return node.isNull() ? null : node.asText();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        finished = true;
    }

    protected int issueQueryAndPopulate(
            PinotQueryGenerator.GeneratedPinotQuery pinotQuery,
            List<BlockBuilder> blockBuilders,
            List<Type> types)
    {
        return doWithRetries(PinotSessionProperties.getPinotRetryCount(session), (retryNumber) -> {
            String queryHost;
            Optional<String> rpcService;
            if (pinotConfig.isUseProxy()) {
                queryHost = pinotConfig.getControllerUrl();
                rpcService = Optional.ofNullable(pinotConfig.getRestProxyServiceForQuery());
            }
            else {
                queryHost = clusterInfoFetcher.getBrokerHost(pinotQuery.getTable());
                rpcService = Optional.empty();
            }
            Request.Builder builder = Request.Builder
                    .preparePost()
                    .setUri(URI.create(String.format(QUERY_URL_TEMPLATE, pinotConfig.isUseSecureConnection() ? "https" : "http", queryHost)));
            brokerAuthenticationProvider.getAuthenticationToken(session).ifPresent(token -> builder.setHeader(AUTHORIZATION, token));
            String body = clusterInfoFetcher.doHttpActionWithHeaders(builder, Optional.of(getRequestPayload(pinotQuery)), rpcService);
            return populateFromQueryResults(pinotQuery, blockBuilders, types, body);
        });
    }

    public static String getRequestPayload(PinotQueryGenerator.GeneratedPinotQuery pinotQuery)
    {
        ImmutableMap<String, String> pinotRequest = ImmutableMap.of(REQUEST_PAYLOAD_KEY, pinotQuery.getQuery());
        try {
            return OBJECT_MAPPER.writeValueAsString(pinotRequest);
        }
        catch (JsonProcessingException e) {
            throw new PinotException(
                    PINOT_REQUEST_GENERATOR_FAILURE,
                    Optional.of(pinotQuery.getQuery()),
                    "Unable to Jsonify request: " + Arrays.toString(pinotRequest.entrySet().toArray()),
                    e);
        }
    }

    // Set Pinot Response from query response json string.
    @VisibleForTesting
    public int populateFromQueryResults(
            PinotQueryGenerator.GeneratedPinotQuery pinotQuery,
            List<BlockBuilder> blockBuilders,
            List<Type> types,
            String responseJsonString)
    {
        String sql = pinotQuery.getQuery();
        JsonNode jsonBody;
        try {
            jsonBody = objectMapper.readTree(responseJsonString);
        }
        catch (IOException e) {
            throw new PinotException(PINOT_UNEXPECTED_RESPONSE, Optional.of(sql), "Couldn't parse response", e);
        }
        handleCommonResponse(sql, jsonBody);
        JsonNode resultTable = jsonBody.get("resultTable");
        if (resultTable != null) {
            JsonNode dataSchema = resultTable.get("dataSchema");
            if (dataSchema == null) {
                throw new PinotException(
                    PINOT_UNEXPECTED_RESPONSE,
                    Optional.of(sql),
                    String.format("Expected data schema in the response"));
            }
            JsonNode columnDataTypes = dataSchema.get("columnDataTypes");
            JsonNode columnNames = dataSchema.get("columnNames");

            if (columnDataTypes == null
                    || !columnDataTypes.isArray()
                    || columnDataTypes.size() < blockBuilders.size()) {
                throw new PinotException(
                    PINOT_UNEXPECTED_RESPONSE,
                    Optional.of(sql),
                    String.format("ColumnDataTypes and results expected for %s, expected %d columnDataTypes but got %d", sql, blockBuilders.size(), columnDataTypes == null ? 0 : columnDataTypes.size()));
            }
            if (columnNames == null
                    || !columnNames.isArray()
                    || columnNames.size() < blockBuilders.size()) {
                throw new PinotException(
                    PINOT_UNEXPECTED_RESPONSE,
                    Optional.of(sql),
                    String.format("ColumnNames and results expected for %s, expected %d columnNames but got %d", sql, blockBuilders.size(), columnNames == null ? 0 : columnNames.size()));
            }

            JsonNode rows = resultTable.get("rows");
            setRows(sql, blockBuilders, types, rows);
            return rows.size();
        }
        return 0;
    }

    // Build BlockAndTypeBuilder from different query syntax.
    // E.g. SQL needs to handle the case that groupBy fields are always show up in front of selection list.
    @VisibleForTesting
    public BlockAndTypeBuilder buildBlockAndTypeBuilder(List<PinotColumnHandle> columnHandles,
            PinotQueryGenerator.GeneratedPinotQuery brokerSql)
    {
        // When we created the SQL, we came up with some column handles
        // however other optimizers post-pushdown can come in and prune/re-order the required column handles
        // so we need to map from the column handles the SQL corresponds to, to the actual column handles
        // needed in the scan.

        List<Type> expectedTypes = columnHandles.stream()
                .map(PinotColumnHandle::getDataType)
                .collect(Collectors.toList());
        PageBuilder pageBuilder = new PageBuilder(expectedTypes);

        // The expectedColumnHandles are the handles corresponding to the generated SQL
        // However, the engine could end up requesting only a permutation/subset of those handles
        // during the actual scan

        // Map the handles from planning time to the handles asked in the scan
        // so that we know which columns to discard.
        int[] handleMapping = new int[expectedHandles.size()];
        for (int i = 0; i < handleMapping.length; ++i) {
            handleMapping[i] = columnHandles.indexOf(expectedHandles.get(i));
        }

        ArrayList<BlockBuilder> columnBlockBuilders = new ArrayList<>();
        ArrayList<Type> columnTypes = new ArrayList<>();

        for (int expectedColumnIndex : brokerSql.getExpectedColumnIndices()) {
            // columnIndex is the index of this column in the current scan
            // It is obtained from the mapping and can be -ve, which means that the
            // expectedColumnIndex'th column returned by Pinot can be discarded.
            int columnIndex = -1;
            if (expectedColumnIndex >= 0) {
                columnIndex = handleMapping[expectedColumnIndex];
            }
            columnBlockBuilders.add(columnIndex >= 0 ? pageBuilder.getBlockBuilder(columnIndex) : null);
            columnTypes.add(columnIndex >= 0 ? expectedTypes.get(columnIndex) : null);
        }
        return new BlockAndTypeBuilder(pageBuilder, columnBlockBuilders, columnTypes);
    }

    public static class BlockAndTypeBuilder
    {
        private final PageBuilder pageBuilder;
        private final List<BlockBuilder> columnBlockBuilders;
        private final List<Type> columnTypes;

        public BlockAndTypeBuilder(PageBuilder pageBuilder, List<BlockBuilder> columnBlockBuilders, List<Type> columnTypes)
        {
            this.pageBuilder = pageBuilder;
            this.columnBlockBuilders = columnBlockBuilders;
            this.columnTypes = columnTypes;
        }

        public PageBuilder getPageBuilder()
        {
            return pageBuilder;
        }

        public List<BlockBuilder> getColumnBlockBuilders()
        {
            return columnBlockBuilders;
        }

        public List<Type> getColumnTypes()
        {
            return columnTypes;
        }
    }
}
