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
package com.facebook.plugin.arrow;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.Result;
import org.apache.arrow.vector.types.pojo.Field;

import javax.inject.Inject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static java.util.Locale.ENGLISH;

public class TestingArrowMetadata
        extends AbstractArrowMetadata
{
    private static final Logger logger = Logger.get(TestingArrowMetadata.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final NodeManager nodeManager;
    private final TestingArrowFlightConfig testconfig;
    private final ArrowFlightClientHandler clientHandler;
    private final ArrowFlightConfig config;

    @Inject
    public TestingArrowMetadata(ArrowFlightConfig config, ArrowFlightClientHandler clientHandler, NodeManager nodeManager, TestingArrowFlightConfig testconfig)
    {
        super(config, clientHandler);
        this.nodeManager = nodeManager;
        this.testconfig = testconfig;
        this.clientHandler = clientHandler;
        this.config = config;
    }

    @Override
    protected ArrowFlightRequest getArrowFlightRequest(ArrowFlightConfig config, String schema)
    {
        return new TestingArrowFlightRequest(config, schema, nodeManager.getWorkerNodes().size(), testconfig);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        List<String> listSchemas = extractSchemaAndTableData(Optional.empty(), session);
        List<String> names = new ArrayList<>();
        for (String value : listSchemas) {
            names.add(value.toLowerCase(ENGLISH));
        }
        return ImmutableList.copyOf(names);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        String schemaValue = schemaName.orElse("");
        String dataSourceSpecificSchemaName = getDataSourceSpecificSchemaName(config, schemaValue);
        List<String> listTables = extractSchemaAndTableData(Optional.ofNullable(dataSourceSpecificSchemaName), session);
        List<SchemaTableName> tables = new ArrayList<>();
        for (String value : listTables) {
            tables.add(new SchemaTableName(dataSourceSpecificSchemaName.toLowerCase(ENGLISH), value.toLowerCase(ENGLISH)));
        }

        return tables;
    }

    public List<String> extractSchemaAndTableData(Optional<String> schema, ConnectorSession connectorSession)
    {
        try (ArrowFlightClient client = clientHandler.getClient(Optional.empty())) {
            List<String> names = new ArrayList<>();
            ArrowFlightRequest request = getArrowFlightRequest(config, schema.orElse(null));
            ObjectNode rootNode = (ObjectNode) objectMapper.readTree(request.getCommand());

            String modifiedQueryJson = objectMapper.writeValueAsString(rootNode);
            byte[] queryJsonBytes = modifiedQueryJson.getBytes(StandardCharsets.UTF_8);
            Iterator<Result> iterator = client.getFlightClient().doAction(new Action("discovery", queryJsonBytes), clientHandler.getCallOptions(connectorSession));
            while (iterator.hasNext()) {
                Result result = iterator.next();
                String jsonResult = new String(result.getBody(), StandardCharsets.UTF_8);
                List<String> tableNames = objectMapper.readValue(jsonResult, new TypeReference<List<String>>() {
                });
                names.addAll(tableNames);
            }
            return names;
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Type overrideFieldType(Field field, Type type)
    {
        String columnLength = field.getMetadata().get("columnLength");
        int length = columnLength != null ? Integer.parseInt(columnLength) : 0;

        String nativeType = field.getMetadata().get("columnNativeType");

        if ("CHAR".equals(nativeType) || "CHARACTER".equals(nativeType)) {
            return CharType.createCharType(length);
        }
        else if ("VARCHAR".equals(nativeType)) {
            return VarcharType.createVarcharType(length);
        }
        else if ("TIME".equals(nativeType)) {
            return TimeType.TIME;
        }
        else {
            return type;
        }
    }

    @Override
    protected String getDataSourceSpecificSchemaName(ArrowFlightConfig config, String schemaName)
    {
        return schemaName;
    }

    @Override
    protected String getDataSourceSpecificTableName(ArrowFlightConfig config, String tableName)
    {
        return tableName;
    }

    @Override
    protected ArrowFlightRequest getArrowFlightRequest(ArrowFlightConfig config, Optional<String> query, String schema, String table)
    {
        return new TestingArrowFlightRequest(config, testconfig, schema, table, query, nodeManager.getWorkerNodes().size());
    }
}
