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
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.pinot.query.PinotQueryGenerator;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_DECODE_ERROR;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_EXCEPTION;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_INSUFFICIENT_SERVER_RESPONSE;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_REQUEST_GENERATOR_FAILURE;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNEXPECTED_RESPONSE;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static com.facebook.presto.pinot.PinotUtils.doWithRetries;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Long.parseLong;
import static java.util.Objects.requireNonNull;

public abstract class PinotBrokerPageSourceBase
        implements ConnectorPageSource
{
    private static final String PINOT_INFINITY = "∞";
    private static final String PINOT_POSITIVE_INFINITY = "+" + PINOT_INFINITY;
    private static final String PINOT_NEGATIVE_INFINITY = "-" + PINOT_INFINITY;
    private static final Double PRESTO_INFINITY = Double.POSITIVE_INFINITY;
    private static final Double PRESTO_NEGATIVE_INFINITY = Double.NEGATIVE_INFINITY;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected final PinotConfig pinotConfig;
    protected final List<PinotColumnHandle> columnHandles;
    protected final PinotClusterInfoFetcher clusterInfoFetcher;
    protected final ConnectorSession session;
    protected final ObjectMapper objectMapper;

    protected boolean finished;
    protected long readTimeNanos;
    protected long completedBytes;

    public PinotBrokerPageSourceBase(
            PinotConfig pinotConfig,
            ConnectorSession session,
            List<PinotColumnHandle> columnHandles,
            PinotClusterInfoFetcher clusterInfoFetcher,
            ObjectMapper objectMapper)
    {
        this.pinotConfig = requireNonNull(pinotConfig, "pinot config is null");
        this.clusterInfoFetcher = requireNonNull(clusterInfoFetcher, "cluster info fetcher is null");
        this.columnHandles = ImmutableList.copyOf(columnHandles);
        this.session = requireNonNull(session, "session is null");
        this.objectMapper = requireNonNull(objectMapper, "object mapper is null");
    }

    private static Double parseDouble(String value)
    {
        try {
            return Double.valueOf(value);
        }
        catch (NumberFormatException ne) {
            switch (value) {
                case PINOT_INFINITY:
                case PINOT_POSITIVE_INFINITY:
                    return PRESTO_INFINITY;
                case PINOT_NEGATIVE_INFINITY:
                    return PRESTO_NEGATIVE_INFINITY;
            }
            throw new PinotException(PINOT_DECODE_ERROR, Optional.empty(), "Cannot decode double value from pinot " + value, ne);
        }
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
        if (!(type instanceof FixedWidthType) && !(type instanceof VarcharType)) {
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
                type.writeLong(blockBuilder, parseLong(value));
            }
            else if (type instanceof DateType) {
                type.writeLong(blockBuilder, parseLong(value));
            }
            else {
                throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "type '" + type + "' not supported");
            }
        }
        else {
            Slice slice = Slices.utf8Slice(value);
            blockBuilder.writeBytes(slice, 0, slice.length()).closeEntry();
            completedBytes += slice.length();
        }
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
            BlockAndTypeBuilder blockAndTypeBuilder = buildBlockAndTypeBuilder(columnHandles, getBrokerQuery());
            int counter = issueQueryAndPopulate(
                    getBrokerQuery(),
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

    // Generated Pinot Query with different syntax, e.g. PQL vs SQL.
    protected abstract PinotQueryGenerator.GeneratedPinotQuery getBrokerQuery();

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
    protected static void handleCommonResponse(String pql, JsonNode jsonBody)
    {
        JsonNode numServersResponded = jsonBody.get("numServersResponded");
        JsonNode numServersQueried = jsonBody.get("numServersQueried");

        if (numServersQueried == null || numServersResponded == null || numServersQueried.asInt() > numServersResponded.asInt()) {
            throw new PinotException(
                PINOT_INSUFFICIENT_SERVER_RESPONSE,
                Optional.of(pql),
                String.format("Only %s out of %s servers responded for query %s", numServersResponded.asInt(), numServersQueried.asInt(), pql));
        }

        JsonNode exceptions = jsonBody.get("exceptions");
        if (exceptions != null && exceptions.isArray() && exceptions.size() > 0) {
            // Pinot is known to return exceptions with benign errorcodes like 200
            // so we treat any exception as an error
            throw new PinotException(
                PINOT_EXCEPTION,
                Optional.of(pql),
                String.format("Query %s encountered exception %s", pql, exceptions.get(0)));
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
            if (pinotConfig.getRestProxyUrl() != null) {
                queryHost = pinotConfig.getRestProxyUrl();
                rpcService = Optional.ofNullable(pinotConfig.getRestProxyServiceForQuery());
            }
            else {
                queryHost = clusterInfoFetcher.getBrokerHost(pinotQuery.getTable());
                rpcService = Optional.empty();
            }
            Request.Builder builder = Request.Builder
                    .preparePost()
                    .setUri(URI.create(String.format(getQueryUrlTemplate(), queryHost)));
            String body = clusterInfoFetcher.doHttpActionWithHeaders(builder, Optional.of(getRequestPayload(pinotQuery)), rpcService);
            return populateFromQueryResults(pinotQuery, blockBuilders, types, body);
        });
    }

    String getRequestPayload(PinotQueryGenerator.GeneratedPinotQuery pinotQuery)
    {
        ImmutableMap<String, String> pinotRequest = ImmutableMap.of(getRequestPayloadKey(), pinotQuery.getQuery());
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

    // Get the broker query endpoint url template.
    abstract String getQueryUrlTemplate();

    // Get the broker request payload key.
    abstract String getRequestPayloadKey();

    // Set Pinot Response from query response json string.
    abstract int populateFromQueryResults(PinotQueryGenerator.GeneratedPinotQuery pinotQuery, List<BlockBuilder> blockBuilders, List<Type> types, String responseJsonString);

    // Build BlockAndTypeBuilder from different query syntax.
    // E.g. PQL needs to handle the case that groupBy fields are always show up in front of selection list.
    abstract BlockAndTypeBuilder buildBlockAndTypeBuilder(List<PinotColumnHandle> columnHandles, PinotQueryGenerator.GeneratedPinotQuery brokerPinotQuery);

    public static class BlockAndTypeBuilder
    {
        private final PageBuilder pageBuilder;
        private final List<BlockBuilder> columnBlockBuilders;
        private final List<Type> columnTypes;

        public BlockAndTypeBuilder(List<Type> columnTypes)
        {
            this.columnTypes = columnTypes;
            this.pageBuilder = new PageBuilder(columnTypes);
            this.columnBlockBuilders = new ArrayList<>();
            for (int columnIndex = 0; columnIndex < columnTypes.size(); columnIndex++) {
                this.columnBlockBuilders.add(this.pageBuilder.getBlockBuilder(columnIndex));
            }
        }

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
