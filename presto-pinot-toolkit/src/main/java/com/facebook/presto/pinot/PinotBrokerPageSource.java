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
import com.facebook.presto.pinot.query.PinotQueryGenerator.GeneratedPql;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_DECODE_ERROR;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_EXCEPTION;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_INSUFFICIENT_SERVER_RESPONSE;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNEXPECTED_RESPONSE;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static com.facebook.presto.pinot.PinotUtils.doWithRetries;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Long.parseLong;
import static java.util.Objects.requireNonNull;

public class PinotBrokerPageSource
        implements ConnectorPageSource
{
    private static final String REQUEST_PAYLOAD_TEMPLATE = "{\"pql\" : \"%s\" }";
    private static final String QUERY_URL_TEMPLATE = "http://%s/query";

    private static final String PINOT_INFINITY = "∞";
    private static final String PINOT_POSITIVE_INFINITY = "+" + PINOT_INFINITY;
    private static final String PINOT_NEGATIVE_INFINITY = "-" + PINOT_INFINITY;

    private static final Double PRESTO_INFINITY = Double.POSITIVE_INFINITY;
    private static final Double PRESTO_NEGATIVE_INFINITY = Double.NEGATIVE_INFINITY;

    private final GeneratedPql brokerPql;
    private final PinotConfig pinotConfig;
    private final List<PinotColumnHandle> columnHandles;
    private final PinotClusterInfoFetcher clusterInfoFetcher;
    private final ConnectorSession session;
    private final ObjectMapper objectMapper;

    private boolean finished;
    private long readTimeNanos;
    private long completedBytes;

    public PinotBrokerPageSource(
            PinotConfig pinotConfig,
            ConnectorSession session,
            GeneratedPql brokerPql,
            List<PinotColumnHandle> columnHandles,
            PinotClusterInfoFetcher clusterInfoFetcher,
            ObjectMapper objectMapper)
    {
        this.pinotConfig = requireNonNull(pinotConfig, "pinot config is null");
        this.brokerPql = requireNonNull(brokerPql, "broker is null");
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

    private void setValue(Type type, BlockBuilder blockBuilder, String value)
    {
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

    private void setValuesForGroupby(
            List<BlockBuilder> blockBuilders,
            List<Type> types,
            int numGroupByClause,
            JsonNode group,
            String[] values)
    {
        for (int i = 0; i < group.size(); i++) {
            setValue(types.get(i), blockBuilders.get(i), group.get(i).asText());
        }
        for (int i = 0; i < values.length; i++) {
            int metricColumnIndex = i + numGroupByClause;
            if (metricColumnIndex < blockBuilders.size()) {
                setValue(types.get(metricColumnIndex), blockBuilders.get(metricColumnIndex), values[i]);
            }
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
            List<Type> expectedTypes = columnHandles.stream()
                    .map(PinotColumnHandle::getDataType)
                    .collect(Collectors.toList());
            PageBuilder pageBuilder = new PageBuilder(expectedTypes);
            ImmutableList.Builder<BlockBuilder> columnBlockBuilders = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            for (int i : brokerPql.getExpectedColumnIndices()) {
                if (i == -1) {
                    continue;
                }
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                columnBlockBuilders.add(blockBuilder);
                columnTypes.add(expectedTypes.get(i));
            }

            int counter = issuePqlAndPopulate(
                    brokerPql.getTable(),
                    brokerPql.getPql(),
                    brokerPql.getGroupByClauses(),
                    columnBlockBuilders.build(),
                    columnTypes.build());
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

    private int issuePqlAndPopulate(
            String table,
            String pql,
            int numGroupByClause,
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
                queryHost = clusterInfoFetcher.getBrokerHost(table);
                rpcService = Optional.empty();
            }
            Request.Builder builder = Request.Builder
                    .preparePost()
                    .setUri(URI.create(String.format(QUERY_URL_TEMPLATE, queryHost)));
            String body = clusterInfoFetcher.doHttpActionWithHeaders(builder, Optional.of(String.format(REQUEST_PAYLOAD_TEMPLATE, pql)), rpcService);

            return populateFromPqlResults(pql, numGroupByClause, blockBuilders, types, body);
        });
    }

    @VisibleForTesting
    public int populateFromPqlResults(
            String pql,
            int numGroupByClause,
            List<BlockBuilder> blockBuilders,
            List<Type> types,
            String body)
    {
        JsonNode jsonBody;

        try {
            jsonBody = objectMapper.readTree(body);
        }
        catch (IOException e) {
            throw new PinotException(PINOT_UNEXPECTED_RESPONSE, Optional.of(pql), "Couldn't parse response", e);
        }

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

        JsonNode aggregationResults = jsonBody.get("aggregationResults");
        JsonNode selectionResults = jsonBody.get("selectionResults");

        int rowCount;
        if (aggregationResults != null && aggregationResults.isArray()) {
            // This is map is populated only when we have multiple aggregates with a group by
            checkState(aggregationResults.size() >= 1, "Expected at least one metric to be present");
            Map<JsonNode, String[]> groupToValue = aggregationResults.size() == 1 || numGroupByClause == 0 ? null : new HashMap<>();
            rowCount = 0;
            String[] singleAggregation = new String[1];
            Boolean seenGroupByResult = null;
            for (int aggregationIndex = 0; aggregationIndex < aggregationResults.size(); aggregationIndex++) {
                JsonNode result = aggregationResults.get(aggregationIndex);

                JsonNode metricValuesForEachGroup = result.get("groupByResult");

                if (metricValuesForEachGroup != null) {
                    checkState(seenGroupByResult == null || seenGroupByResult);
                    seenGroupByResult = true;
                    checkState(numGroupByClause > 0, "Expected having non zero group by clauses");
                    JsonNode groupByColumns = checkNotNull(result.get("groupByColumns"), "groupByColumns missing in %s", pql);
                    if (groupByColumns.size() != numGroupByClause) {
                        throw new PinotException(
                                PINOT_UNEXPECTED_RESPONSE,
                                Optional.of(pql),
                                String.format("Expected %d gby columns but got %s instead from pinot", numGroupByClause, groupByColumns));
                    }

                    // group by aggregation
                    for (int groupByIndex = 0; groupByIndex < metricValuesForEachGroup.size(); groupByIndex++) {
                        JsonNode row = metricValuesForEachGroup.get(groupByIndex);
                        JsonNode group = row.get("group");
                        if (group == null || !group.isArray() || group.size() != numGroupByClause) {
                            throw new PinotException(
                                    PINOT_UNEXPECTED_RESPONSE,
                                    Optional.of(pql),
                                    String.format("Expected %d group by columns but got only a group of size %d (%s)", numGroupByClause, group.size(), group));
                        }
                        if (groupToValue == null) {
                            singleAggregation[0] = row.get("value").asText();
                            setValuesForGroupby(blockBuilders, types, numGroupByClause, group, singleAggregation);
                            rowCount++;
                        }
                        else {
                            groupToValue.computeIfAbsent(group, (ignored) -> new String[aggregationResults.size()])[aggregationIndex] = row.get("value").asText();
                        }
                    }
                }
                else {
                    checkState(seenGroupByResult == null || !seenGroupByResult);
                    seenGroupByResult = false;
                    // simple aggregation
                    // TODO: Validate that this is expected semantically
                    checkState(numGroupByClause == 0, "Expected no group by columns in pinot");
                    setValue(types.get(aggregationIndex), blockBuilders.get(aggregationIndex), result.get("value").asText());
                    rowCount = 1;
                }
            }

            if (groupToValue != null) {
                checkState(rowCount == 0, "Row count shouldn't have changed from zero");
                groupToValue.forEach((group, values) -> setValuesForGroupby(blockBuilders, types, numGroupByClause, group, values));
                rowCount = groupToValue.size();
            }
        }
        else if (selectionResults != null) {
            JsonNode columns = selectionResults.get("columns");
            JsonNode results = selectionResults.get("results");
            if (columns == null || results == null || !columns.isArray() || !results.isArray() || columns.size() != blockBuilders.size()) {
                throw new PinotException(
                        PINOT_UNEXPECTED_RESPONSE,
                        Optional.of(pql),
                        String.format("Columns and results expected for %s, expected %d columns but got %d", pql, blockBuilders.size(), columns == null ? 0 : columns.size()));
            }
            for (int rowNumber = 0; rowNumber < results.size(); ++rowNumber) {
                JsonNode result = results.get(rowNumber);
                if (result == null || result.size() != blockBuilders.size()) {
                    throw new PinotException(
                            PINOT_UNEXPECTED_RESPONSE,
                            Optional.of(pql),
                            String.format("Expected row of %d columns", blockBuilders.size()));
                }
                for (int columnNumber = 0; columnNumber < blockBuilders.size(); columnNumber++) {
                    setValue(types.get(columnNumber), blockBuilders.get(columnNumber), result.get(columnNumber).asText());
                }
            }
            rowCount = results.size();
        }
        else {
            throw new PinotException(
                    PINOT_UNEXPECTED_RESPONSE,
                    Optional.of(pql),
                    "Expected one of aggregationResults or selectionResults to be present");
        }

        checkState(rowCount >= 0, "Expected row count to be initialized");
        return rowCount;
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
}
