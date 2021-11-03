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

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.pinot.query.PinotQueryGenerator.GeneratedPinotQuery;
import com.facebook.presto.spi.ConnectorSession;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNEXPECTED_RESPONSE;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PinotBrokerPageSourceSql
        extends PinotBrokerPageSourceBase
{
    private static final String REQUEST_PAYLOAD_KEY = "sql";
    private static final String QUERY_URL_TEMPLATE = "http://%s/query/sql";

    private final GeneratedPinotQuery brokerSql;

    public PinotBrokerPageSourceSql(
            PinotConfig pinotConfig,
            ConnectorSession session,
            GeneratedPinotQuery brokerSql,
            List<PinotColumnHandle> columnHandles,
            PinotClusterInfoFetcher clusterInfoFetcher,
            ObjectMapper objectMapper)
    {
        super(pinotConfig, session, columnHandles, clusterInfoFetcher, objectMapper);
        this.brokerSql = requireNonNull(brokerSql, "broker is null");
    }

    @Override
    protected GeneratedPinotQuery getBrokerQuery()
    {
        return brokerSql;
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

    @VisibleForTesting
    @Override
    public int populateFromQueryResults(
            GeneratedPinotQuery pinotQuery,
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
            checkState(rows.size() >= 1, "Expected at least one row to be present");
            setRows(sql, blockBuilders, types, rows);
            return rows.size();
        }
        return 0;
    }

    @VisibleForTesting
    @Override
    public BlockAndTypeBuilder buildBlockAndTypeBuilder(List<PinotColumnHandle> columnHandles,
            GeneratedPinotQuery brokerSql)
    {
        // SQL broker page source returns the columns in the order requested by Presto.
        List<Type> columnTypes = columnHandles.stream()
                .map(PinotColumnHandle::getDataType)
                .collect(Collectors.toList());
        return new BlockAndTypeBuilder(columnTypes);
    }
}
