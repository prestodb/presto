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

import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.pinot.query.PinotQueryGenerator;
import com.facebook.presto.pinot.query.PinotQueryGenerator.GeneratedPinotQuery;
import com.facebook.presto.spi.ConnectorSession;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNEXPECTED_RESPONSE;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PinotBrokerPageSourcePql
        extends PinotBrokerPageSourceBase
{
    private static final String REQUEST_PAYLOAD_KEY = "pql";
    private static final String QUERY_URL_TEMPLATE = "http://%s/query";

    private final GeneratedPinotQuery brokerPql;
    private final List<PinotColumnHandle> expectedHandles;

    public PinotBrokerPageSourcePql(
            PinotConfig pinotConfig,
            ConnectorSession session,
            GeneratedPinotQuery brokerPql,
            List<PinotColumnHandle> columnHandles,
            List<PinotColumnHandle> expectedHandles,
            PinotClusterInfoFetcher clusterInfoFetcher,
            ObjectMapper objectMapper)
    {
        super(pinotConfig, session, columnHandles, clusterInfoFetcher, objectMapper);
        this.expectedHandles = requireNonNull(expectedHandles, "expected handles is null");
        this.brokerPql = requireNonNull(brokerPql, "broker is null");
    }

    private void setValuesForGroupby(
            List<BlockBuilder> blockBuilders,
            List<Type> types,
            int numGroupByClause,
            JsonNode group,
            String[] values)
    {
        requireNonNull(group, "Expected valid group");
        requireNonNull(values, "Expected valid values in group by");
        checkState(
                blockBuilders.size() == values.length + group.size(),
                String.format(
                        "Expected pinot to return total of %d values for group by, but got only %d group-by-keys and %d values",
                        blockBuilders.size(),
                        group.size(),
                        values.length));
        for (int i = 0; i < group.size(); i++) {
            setValue(types.get(i), blockBuilders.get(i), group.get(i));
        }
        for (int i = 0; i < values.length; i++) {
            int metricColumnIndex = i + numGroupByClause;
            setValue(types.get(metricColumnIndex), blockBuilders.get(metricColumnIndex), values[i]);
        }
    }

    @Override
    protected GeneratedPinotQuery getBrokerQuery()
    {
        return brokerPql;
    }

    @VisibleForTesting
    @Override
    public int populateFromQueryResults(
            GeneratedPinotQuery pinotQuery,
            List<BlockBuilder> blockBuilders,
            List<Type> types,
            String responseJsonString)
    {
        String pql = pinotQuery.getQuery();
        int numGroupByClause = pinotQuery.getGroupByClauses();

        JsonNode jsonBody;
        try {
            jsonBody = objectMapper.readTree(responseJsonString);
        }
        catch (IOException e) {
            throw new PinotException(PINOT_UNEXPECTED_RESPONSE, Optional.of(pql), "Couldn't parse response", e);
        }
        handleCommonResponse(pql, jsonBody);

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
                            singleAggregation[0] = asText(row.get("value"));
                            setValuesForGroupby(blockBuilders, types, numGroupByClause, group, singleAggregation);
                            rowCount++;
                        }
                        else {
                            groupToValue.computeIfAbsent(group, (ignored) -> new String[aggregationResults.size()])[aggregationIndex] = asText(row.get("value"));
                        }
                    }
                }
                else {
                    checkState(seenGroupByResult == null || !seenGroupByResult);
                    seenGroupByResult = false;
                    // simple aggregation
                    // TODO: Validate that this is expected semantically
                    checkState(numGroupByClause == 0, "Expected no group by columns in pinot");
                    setValue(types.get(aggregationIndex), blockBuilders.get(aggregationIndex), result.get("value"));
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
            setRows(pql, blockBuilders, types, results);
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
    String getQueryUrlTemplate()
    {
        return QUERY_URL_TEMPLATE;
    }

    @Override
    String getRequestPayloadKey()
    {
        return REQUEST_PAYLOAD_KEY;
    }

    @Override
    @VisibleForTesting
    public BlockAndTypeBuilder buildBlockAndTypeBuilder(List<PinotColumnHandle> columnHandles, PinotQueryGenerator.GeneratedPinotQuery brokerPql)
    {
        // When we created the PQL, we came up with some column handles
        // however other optimizers post-pushdown can come in and prune/re-order the required column handles
        // so we need to map from the column handles the PQL corresponds to, to the actual column handles
        // needed in the scan.

        List<Type> expectedTypes = columnHandles.stream()
                .map(PinotColumnHandle::getDataType)
                .collect(Collectors.toList());
        PageBuilder pageBuilder = new PageBuilder(expectedTypes);

        checkState(brokerPql.getExpectedColumnIndices().size() >= expectedHandles.size());
        checkState(expectedHandles.size() >= columnHandles.size());

        // The expectedColumnHandles are the handles corresponding to the generated PQL
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

        for (int expectedColumnIndex : brokerPql.getExpectedColumnIndices()) {
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
}
