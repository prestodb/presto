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
package com.facebook.presto.spi.plan;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.TestingTypeDeserializer;
import com.facebook.presto.common.type.TestingTypeManager;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.eventlistener.OutputColumnMetadata;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestTableWriterNode
{
    private JsonCodec<TableWriterNode.WriterTarget> writerTargetCodec;

    @BeforeClass
    public void setup()
    {
        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
        ObjectMapper objectMapper = provider.get();

        // Register custom serializers/deserializers for types that don't have them
        SimpleModule module = new SimpleModule();
        module.addAbstractTypeMapping(ConnectorTableHandle.class, MockConnectorTableHandle.class);
        module.addAbstractTypeMapping(ConnectorTransactionHandle.class, MockConnectorTransactionHandle.class);
        module.addAbstractTypeMapping(ColumnHandle.class, MockColumnHandle.class);
        // Register KeyDeserializer for VariableReferenceExpression used as Map key in StatisticAggregations
        module.addKeyDeserializer(VariableReferenceExpression.class, new VariableReferenceExpressionKeyDeserializer(new TestingTypeManager()));
        objectMapper.registerModule(module);

        JsonCodecFactory codecFactory = new JsonCodecFactory(() -> objectMapper);
        writerTargetCodec = codecFactory.jsonCodec(TableWriterNode.WriterTarget.class);
    }

    // Custom KeyDeserializer for VariableReferenceExpression
    // StatisticAggregations.java line 35 has: Map<VariableReferenceExpression, Aggregation> aggregations
    // TableWriterNode has Optional<StatisticAggregations>, so Jackson needs to deserialize
    // VariableReferenceExpression as a Map key even when the field isn't used in tests
    private static class VariableReferenceExpressionKeyDeserializer
            extends KeyDeserializer
    {
        private static final char VARIABLE_TYPE_OPEN_BRACKET = '<';
        private static final char VARIABLE_TYPE_CLOSE_BRACKET = '>';
        private final TypeManager typeManager;

        public VariableReferenceExpressionKeyDeserializer(TypeManager typeManager)
        {
            this.typeManager = typeManager;
        }

        @Override
        public Object deserializeKey(String key, DeserializationContext ctx) throws IOException
        {
            int p = key.indexOf(VARIABLE_TYPE_OPEN_BRACKET);
            if (p <= 0 || key.charAt(key.length() - 1) != VARIABLE_TYPE_CLOSE_BRACKET) {
                throw new IllegalArgumentException(format("Expect key to be of format 'name<type>', found %s", key));
            }
            String name = key.substring(0, p);
            String typeStr = key.substring(p + 1, key.length() - 1);
            Type type = typeManager.getType(parseTypeSignature(typeStr));
            return new VariableReferenceExpression(Optional.empty(), name, type);
        }
    }

    @Test
    public void testInsertReferenceTargetSerialization() throws JsonProcessingException
    {
        TableHandle tableHandle = new TableHandle(
                new ConnectorId("test_catalog"),
                new MockConnectorTableHandle("test_table"),
                new MockConnectorTransactionHandle("txn123"),
                Optional.empty());
        SchemaTableName schemaTableName = new SchemaTableName("test_schema", "test_table");

        TableWriterNode.InsertReference insertReference = new TableWriterNode.InsertReference(
                tableHandle,
                schemaTableName,
                Optional.empty());

        String json = writerTargetCodec.toJson(insertReference);
        assertNotNull(json);
        assertTrue(json.contains("\"@type\""));
        assertTrue(json.contains("insertReference"));
        assertTrue(json.contains("test_schema"));
        assertTrue(json.contains("test_table"));

        ObjectMapper objectMapper = new JsonObjectMapperProvider().get().disable(
                SerializationFeature.FAIL_ON_EMPTY_BEANS);
        Map<String, Object> writerTargetMap = objectMapper.readValue(json,
                new TypeReference<Map<String, Object>>() {
                });
        JsonNode rootJsonNode = objectMapper.valueToTree(writerTargetMap);
        assertTrue(rootJsonNode.has("handle"));
        assertTrue(rootJsonNode.get("handle").has("connectorHandle"));
        assertTrue(rootJsonNode.get("handle").get("connectorHandle").has("tableName"));
        assertTrue(rootJsonNode.get("handle").get("connectorHandle").get("tableName").asText()
                .equals("test_table"));

        TableWriterNode.WriterTarget deserialized = writerTargetCodec.fromJson(json);

        assertTrue(deserialized instanceof TableWriterNode.InsertReference);
        TableWriterNode.InsertReference deserializedTarget = (TableWriterNode.InsertReference) deserialized;
        assertEquals(deserializedTarget.getSchemaTableName(), schemaTableName);
        assertEquals(deserializedTarget.getHandle().getConnectorId(), tableHandle.getConnectorId());

        // Verify that columns field is not present in JSON (due to @JsonIgnore on getter)
        // and therefore deserializes as Optional.empty()
        assertFalse(json.contains("\"columns\""));
        assertFalse(deserializedTarget.getOutputColumns().isPresent());
    }

    @Test
    public void testDeleteHandleTargetSerialization()
    {
        TableHandle tableHandle = new TableHandle(
                new ConnectorId("test_catalog"),
                new MockConnectorTableHandle("test_table"),
                new MockConnectorTransactionHandle("txn123"),
                Optional.empty());
        SchemaTableName schemaTableName = new SchemaTableName("test_schema", "test_table");

        TableWriterNode.DeleteHandle deleteHandle = new TableWriterNode.DeleteHandle(
                tableHandle,
                schemaTableName);

        String json = writerTargetCodec.toJson(deleteHandle);
        assertNotNull(json);
        assertTrue(json.contains("\"@type\""));
        assertTrue(json.contains("deleteHandle"));
        assertTrue(json.contains("test_schema"));
        assertTrue(json.contains("test_table"));

        TableWriterNode.WriterTarget deserialized = writerTargetCodec.fromJson(json);

        assertTrue(deserialized instanceof TableWriterNode.DeleteHandle);
        TableWriterNode.DeleteHandle deserializedTarget = (TableWriterNode.DeleteHandle) deserialized;
        assertEquals(deserializedTarget.getSchemaTableName(), schemaTableName);
        assertEquals(deserializedTarget.getHandle().getConnectorId(), tableHandle.getConnectorId());
    }

    @Test
    public void testRefreshMaterializedViewTargetSerialization()
    {
        TableHandle tableHandle = new TableHandle(
                new ConnectorId("test_catalog"),
                new MockConnectorTableHandle("test_mv"),
                new MockConnectorTransactionHandle("txn123"),
                Optional.empty());
        SchemaTableName schemaTableName = new SchemaTableName("test_schema", "test_materialized_view");

        TableWriterNode.RefreshMaterializedViewReference refreshReference = new TableWriterNode.RefreshMaterializedViewReference(
                tableHandle,
                schemaTableName);

        String json = writerTargetCodec.toJson(refreshReference);
        assertNotNull(json);
        assertTrue(json.contains("\"@type\""));
        assertTrue(json.contains("refreshMaterializedView"));
        assertTrue(json.contains("test_schema"));
        assertTrue(json.contains("test_materialized_view"));

        TableWriterNode.WriterTarget deserialized = writerTargetCodec.fromJson(json);

        assertTrue(deserialized instanceof TableWriterNode.RefreshMaterializedViewReference);
        TableWriterNode.RefreshMaterializedViewReference deserializedTarget = (TableWriterNode.RefreshMaterializedViewReference) deserialized;
        assertEquals(deserializedTarget.getSchemaTableName(), schemaTableName);
        assertEquals(deserializedTarget.getHandle().getConnectorId(), tableHandle.getConnectorId());
    }

    @Test
    public void testUpdateTargetSerialization()
    {
        TableHandle tableHandle = new TableHandle(
                new ConnectorId("test_catalog"),
                new MockConnectorTableHandle("test_table"),
                new MockConnectorTransactionHandle("txn123"),
                Optional.empty());
        SchemaTableName schemaTableName = new SchemaTableName("test_schema", "test_table");

        ColumnHandle columnHandle1 = new MockColumnHandle("col1");
        ColumnHandle columnHandle2 = new MockColumnHandle("col2");

        TableWriterNode.UpdateTarget updateTarget = new TableWriterNode.UpdateTarget(
                tableHandle,
                schemaTableName,
                ImmutableList.of("col1", "col2"),
                ImmutableList.of(columnHandle1, columnHandle2));

        String json = writerTargetCodec.toJson(updateTarget);
        assertNotNull(json);
        assertTrue(json.contains("\"@type\""));
        assertTrue(json.contains("updateTarget"));
        assertTrue(json.contains("test_schema"));
        assertTrue(json.contains("test_table"));
        assertFalse(json.contains("updatedColumns"));
        assertFalse(json.contains("updatedColumnHandles"));

        // Test deserialization - UpdateTarget should deserialize with only handle and schemaTableName
        TableWriterNode.WriterTarget deserialized = writerTargetCodec.fromJson(json);
        assertNotNull(deserialized);
        assertTrue(deserialized instanceof TableWriterNode.UpdateTarget);
        TableWriterNode.UpdateTarget deserializedTarget = (TableWriterNode.UpdateTarget) deserialized;
        assertEquals(deserializedTarget.getSchemaTableName(), schemaTableName);
        assertEquals(deserializedTarget.getHandle().getConnectorId(), tableHandle.getConnectorId());
        // Verify that updatedColumns and updatedColumnHandles default to empty lists
        assertEquals(deserializedTarget.getUpdatedColumns().size(), 0);
        assertEquals(deserializedTarget.getUpdatedColumnHandles().size(), 0);
    }

    @Test
    public void testTableWriterNodeWithCreateNameSerialization()
            throws Exception
    {
        ConnectorId connectorId = new ConnectorId("test_catalog");
        SchemaTableName schemaTableName = new SchemaTableName("test_schema", "test_table");

        List<ColumnMetadata> columns = ImmutableList.of();
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(schemaTableName, columns);

        TableWriterNode.CreateName createName = new TableWriterNode.CreateName(
                connectorId,
                tableMetadata,
                Optional.empty(),
                Optional.empty());

        PlanNodeId sourcePlanNodeId = new PlanNodeId("source_1");
        VariableReferenceExpression sourceVariable = new VariableReferenceExpression(
                Optional.empty(),
                "source_col",
                VarcharType.VARCHAR);
        List<VariableReferenceExpression> sourceOutputVariables = ImmutableList.of(sourceVariable);
        ValuesNode sourceNode = new ValuesNode(
                Optional.of(new SourceLocation(1, 1)),
                sourcePlanNodeId,
                sourceOutputVariables,
                ImmutableList.of(),
                Optional.empty());

        VariableReferenceExpression rowCountVariable = new VariableReferenceExpression(
                Optional.empty(),
                "row_count",
                BigintType.BIGINT);
        VariableReferenceExpression fragmentVariable = new VariableReferenceExpression(
                Optional.empty(),
                "fragment",
                BigintType.BIGINT);
        VariableReferenceExpression tableCommitContextVariable = new VariableReferenceExpression(
                Optional.empty(),
                "table_commit_context",
                VarcharType.VARCHAR);

        PlanNodeId writerNodeId = new PlanNodeId("writer_1");
        TableWriterNode writerNode = new TableWriterNode(
                Optional.empty(),
                writerNodeId,
                sourceNode,
                Optional.of(createName),
                rowCountVariable,
                fragmentVariable,
                tableCommitContextVariable,
                ImmutableList.of(sourceVariable),
                ImmutableList.of("col1"),
                ImmutableSet.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
        ObjectMapper objectMapper = provider.get();

        TestingTypeManager typeManager = new TestingTypeManager();
        SimpleModule module = new SimpleModule();
        module.addAbstractTypeMapping(ConnectorTableHandle.class, MockConnectorTableHandle.class);
        module.addAbstractTypeMapping(ConnectorTransactionHandle.class, MockConnectorTransactionHandle.class);
        module.addAbstractTypeMapping(ConnectorTableLayoutHandle.class, MockConnectorTableLayoutHandle.class);
        module.addAbstractTypeMapping(ColumnHandle.class, MockColumnHandle.class);
        module.addDeserializer(Type.class, new TestingTypeDeserializer(typeManager));
        module.addKeyDeserializer(VariableReferenceExpression.class, new VariableReferenceExpressionKeyDeserializer(typeManager));
        objectMapper.registerModule(module);

        JsonCodecFactory codecFactory = new JsonCodecFactory(() -> objectMapper);
        JsonCodec<TableWriterNode> codec = codecFactory.jsonCodec(TableWriterNode.class);

        String json = codec.toJson(writerNode);
        assertNotNull(json);

        TableWriterNode deserializedNode = codec.fromJson(json);
        assertNotNull(deserializedNode);

        assertEquals(deserializedNode.getId(), writerNodeId);
        assertFalse(deserializedNode.getTarget().isPresent());
    }

    @Test
    public void testCompleteTableWriterNodeSerialization()
            throws Exception
    {
        // Create a complete TableWriterNode with all components
        ConnectorId connectorId = new ConnectorId("test_catalog");
        SchemaTableName schemaTableName = new SchemaTableName("test_schema", "test_table");

        // Create output columns for InsertReference
        List<OutputColumnMetadata> outputColumns = ImmutableList.of(
                new OutputColumnMetadata("col1", "varchar", ImmutableSet.of()),
                new OutputColumnMetadata("col2", "bigint", ImmutableSet.of()));

        // Create TableHandle for InsertReference
        TableHandle tableHandle = new TableHandle(
                connectorId,
                new MockConnectorTableHandle("test_table"),
                new MockConnectorTransactionHandle("txn123"),
                Optional.of(new MockConnectorTableLayoutHandle("test_tablePath")));

        // Create WriterTarget using InsertReference
        TableWriterNode.InsertReference insertReference = new TableWriterNode.InsertReference(
                tableHandle,
                schemaTableName,
                Optional.of(outputColumns));

        // Create source PlanNode (ValuesNode as a simple source)
        PlanNodeId sourcePlanNodeId = new PlanNodeId("source_1");
        VariableReferenceExpression sourceVariable = new VariableReferenceExpression(
                Optional.empty(),
                "source_col",
                VarcharType.VARCHAR);
        List<VariableReferenceExpression> sourceOutputVariables = ImmutableList.of(sourceVariable);
        ValuesNode sourceNode = new ValuesNode(
                Optional.of(new SourceLocation(1, 1)),
                sourcePlanNodeId,
                sourceOutputVariables,
                ImmutableList.of(),
                Optional.empty());

        // Create TableWriterNode variables
        VariableReferenceExpression rowCountVariable = new VariableReferenceExpression(
                Optional.empty(),
                "row_count",
                BigintType.BIGINT);
        VariableReferenceExpression fragmentVariable = new VariableReferenceExpression(
                Optional.empty(),
                "fragment",
                BigintType.BIGINT);
        VariableReferenceExpression tableCommitContextVariable = new VariableReferenceExpression(
                Optional.empty(),
                "table_commit_context",
                VarcharType.VARCHAR);

        // Create column mappings
        List<VariableReferenceExpression> writerColumns = ImmutableList.of(sourceVariable);
        List<String> columnNames = ImmutableList.of("col1");
        Set<VariableReferenceExpression> notNullColumns = ImmutableSet.of();

        // Create TableWriterNode
        PlanNodeId writerNodeId = new PlanNodeId("writer_1");
        TableWriterNode writerNode = new TableWriterNode(
                Optional.of(new SourceLocation(10, 5)),
                writerNodeId,
                sourceNode,
                Optional.of(insertReference),
                rowCountVariable,
                fragmentVariable,
                tableCommitContextVariable,
                writerColumns,
                columnNames,
                notNullColumns,
                Optional.empty(),
                Optional.empty(),
                Optional.of(100),
                Optional.of(false));

        // Serialize to JSON using Jackson codec
        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
        ObjectMapper objectMapper = provider.get();

        // Register custom mappings
        TestingTypeManager typeManager = new TestingTypeManager();
        SimpleModule module = new SimpleModule();
        module.addAbstractTypeMapping(ConnectorTableHandle.class, MockConnectorTableHandle.class);
        module.addAbstractTypeMapping(ConnectorTransactionHandle.class, MockConnectorTransactionHandle.class);
        module.addAbstractTypeMapping(ConnectorTableLayoutHandle.class, MockConnectorTableLayoutHandle.class);
        module.addAbstractTypeMapping(ColumnHandle.class, MockColumnHandle.class);
        // Register Type deserializer for VariableReferenceExpression fields
        module.addDeserializer(Type.class, new TestingTypeDeserializer(typeManager));
        // Register KeyDeserializer for VariableReferenceExpression used as Map key in StatisticAggregations
        module.addKeyDeserializer(VariableReferenceExpression.class, new VariableReferenceExpressionKeyDeserializer(typeManager));
        objectMapper.registerModule(module);

        JsonCodecFactory codecFactory = new JsonCodecFactory(() -> objectMapper);
        JsonCodec<TableWriterNode> codec = codecFactory.jsonCodec(TableWriterNode.class);

        String json = codec.toJson(writerNode);
        assertNotNull(json);

        // Verify using JsonNode navigation pattern
        JsonNode rootNode = objectMapper.readTree(json);

        // Verify PlanNode basic properties
        assertEquals(rootNode.path("@type").asText(), ".TableWriterNode");
        assertEquals(rootNode.path("id").asText(), "writer_1");
        assertTrue(rootNode.has("sourceLocation"));
        assertEquals(rootNode.path("sourceLocation").path("line").asInt(), 10);
        assertEquals(rootNode.path("sourceLocation").path("column").asInt(), 5);

        // Verify source node
        assertTrue(rootNode.has("source"));
        JsonNode sourceNodeJson = rootNode.path("source");
        assertEquals(sourceNodeJson.path("@type").asText(), ".ValuesNode");
        assertEquals(sourceNodeJson.path("id").asText(), "source_1");

        // Verify target
        assertTrue(rootNode.has("target"));
        JsonNode targetNode = rootNode.path("target");
        assertEquals(targetNode.path("@type").asText(), "insertReference");

        // Verify TableHandle within target
        assertTrue(targetNode.has("handle"));
        JsonNode handleNode = targetNode.path("handle");
        assertEquals(handleNode.path("connectorId").asText(), "test_catalog");
        assertTrue(handleNode.has("connectorHandle"));
        JsonNode connectorHandleNode = handleNode.path("connectorHandle");
        assertEquals(connectorHandleNode.path("tableName").asText(), "test_table");

        // Verify SchemaTableName within target
        assertTrue(targetNode.has("schemaTableName"));
        JsonNode schemaTableNameNode = targetNode.path("schemaTableName");
        assertEquals(schemaTableNameNode.path("schema").asText(), "test_schema");
        assertEquals(schemaTableNameNode.path("table").asText(), "test_table");

        // Verify columns in InsertReference (OutputColumnMetadata)
        assertFalse(targetNode.has("outputColumns"));

        // Verify writer variables
        JsonNode rowCountVarNode = rootNode.path("rowCountVariable");
        assertEquals(rowCountVarNode.path("name").asText(), "row_count");

        JsonNode fragmentVarNode = rootNode.path("fragmentVariable");
        assertEquals(fragmentVarNode.path("name").asText(), "fragment");

        JsonNode tableCommitContextVarNode = rootNode.path("tableCommitContextVariable");
        assertEquals(tableCommitContextVarNode.path("name").asText(), "table_commit_context");

        // Verify column mappings
        JsonNode columnsNode = rootNode.path("columns");
        assertTrue(columnsNode.isArray());
        assertEquals(columnsNode.size(), 1);
        assertEquals(columnsNode.get(0).path("name").asText(), "source_col");

        JsonNode columnNamesNode = rootNode.path("columnNames");
        assertTrue(columnNamesNode.isArray());
        assertEquals(columnNamesNode.size(), 1);
        assertEquals(columnNamesNode.get(0).asText(), "col1");

        // Verify notNullColumnVariables
        JsonNode notNullColumnsNode = rootNode.path("notNullColumnVariables");
        assertTrue(notNullColumnsNode.isArray());
        assertEquals(notNullColumnsNode.size(), 0);

        // Verify optional fields
        assertEquals(rootNode.path("taskCountIfScaledWriter").asInt(), 100);
        assertEquals(rootNode.path("isTemporaryTableWriter").asBoolean(), false);
        assertFalse(
                rootNode.has("tablePartitioningScheme") && !rootNode.path("tablePartitioningScheme")
                        .isNull());
        assertFalse(rootNode.has("statisticsAggregation") && !rootNode.path("statisticsAggregation")
                .isNull());

        assertEquals(rootNode.get("target").get("schemaTableName").get("schema").asText(),
                "test_schema");
        assertEquals(rootNode.get("target").get("schemaTableName").get("table").asText(),
                "test_table");
        assertEquals(
                rootNode.get("target").get("handle").get("connectorTableLayout").get("tablePath")
                        .asText(), "test_tablePath");

        // Verify the original object matches what we created (no need for deserialization round-trip)
        assertEquals(writerNode.getId(), writerNodeId);
        assertEquals(writerNode.getRowCountVariable().getName(), "row_count");
        assertEquals(writerNode.getFragmentVariable().getName(), "fragment");
        assertEquals(writerNode.getTableCommitContextVariable().getName(), "table_commit_context");
        assertEquals(writerNode.getColumns().size(), 1);
        assertEquals(writerNode.getColumnNames().size(), 1);
        assertEquals(writerNode.getColumnNames().get(0), "col1");
        assertTrue(writerNode.getTarget().isPresent());
        assertTrue(writerNode.getTarget().get() instanceof TableWriterNode.InsertReference);

        TableWriterNode.InsertReference targetRef = (TableWriterNode.InsertReference) writerNode.getTarget().get();
        assertEquals(targetRef.getHandle().getConnectorId(), connectorId);
        assertEquals(targetRef.getSchemaTableName(), schemaTableName);
        assertTrue(targetRef.getOutputColumns().isPresent());
        assertEquals(targetRef.getOutputColumns().get().size(), 2);

        // Test deserialization
        TableWriterNode deserializedNode = codec.fromJson(json);
        assertNotNull(deserializedNode);

        // Verify deserialized node properties
        assertEquals(deserializedNode.getId(), writerNodeId);
        assertEquals(deserializedNode.getRowCountVariable().getName(), "row_count");
        assertEquals(deserializedNode.getFragmentVariable().getName(), "fragment");
        assertEquals(deserializedNode.getTableCommitContextVariable().getName(), "table_commit_context");
        assertEquals(deserializedNode.getColumns().size(), 1);
        assertEquals(deserializedNode.getColumns().get(0).getName(), "source_col");
        assertEquals(deserializedNode.getColumnNames().size(), 1);
        assertEquals(deserializedNode.getColumnNames().get(0), "col1");
        assertEquals(deserializedNode.getNotNullColumnVariables().size(), 0);
        assertTrue(deserializedNode.getTaskCountIfScaledWriter().isPresent());
        assertEquals(deserializedNode.getTaskCountIfScaledWriter().get().intValue(), 100);
        assertTrue(deserializedNode.getIsTemporaryTableWriter().isPresent());
        assertEquals(deserializedNode.getIsTemporaryTableWriter().get(), false);
        assertFalse(deserializedNode.getTablePartitioningScheme().isPresent());
        assertFalse(deserializedNode.getStatisticsAggregation().isPresent());

        // Verify deserialized target
        assertTrue(deserializedNode.getTarget().isPresent());
        assertTrue(deserializedNode.getTarget().get() instanceof TableWriterNode.InsertReference);
        TableWriterNode.InsertReference deserializedTargetRef = (TableWriterNode.InsertReference) deserializedNode.getTarget().get();
        assertEquals(deserializedTargetRef.getSchemaTableName(), schemaTableName);
        assertEquals(deserializedTargetRef.getHandle().getConnectorId(), connectorId);
        // Verify columns field was not serialized (JsonIgnore) and deserializes as Optional.empty()
        assertFalse(deserializedTargetRef.getOutputColumns().isPresent());

        // Verify deserialized source node
        assertTrue(deserializedNode.getSource() instanceof ValuesNode);
        ValuesNode deserializedSourceNode = (ValuesNode) deserializedNode.getSource();
        assertEquals(deserializedSourceNode.getId(), sourcePlanNodeId);
        assertEquals(deserializedSourceNode.getOutputVariables().size(), 1);
        assertEquals(deserializedSourceNode.getOutputVariables().get(0).getName(), "source_col");
    }

    // Mock classes with JSON support
    public static class MockConnectorTableHandle
            implements ConnectorTableHandle
    {
        private final String tableName;

        @JsonCreator
        public MockConnectorTableHandle(@JsonProperty("tableName") String tableName)
        {
            this.tableName = tableName;
        }

        @JsonProperty
        public String getTableName()
        {
            return tableName;
        }
    }

    public static class MockConnectorTableLayoutHandle
            implements ConnectorTableLayoutHandle
    {
        private final String tablePath;

        @JsonCreator
        public MockConnectorTableLayoutHandle(@JsonProperty("tablePath") String tablePath)
        {
            this.tablePath = tablePath;
        }

        @JsonProperty
        public String getTablePath()
        {
            return tablePath;
        }
    }

    public static class MockConnectorTransactionHandle
            implements ConnectorTransactionHandle
    {
        private final String transactionId;

        @JsonCreator
        public MockConnectorTransactionHandle(@JsonProperty("transactionId") String transactionId)
        {
            this.transactionId = transactionId;
        }

        @JsonProperty
        public String getTransactionId()
        {
            return transactionId;
        }
    }

    public static class MockColumnHandle
            implements ColumnHandle
    {
        private final String columnName;

        @JsonCreator
        public MockColumnHandle(@JsonProperty("columnName") String columnName)
        {
            this.columnName = columnName;
        }

        @JsonProperty
        public String getColumnName()
        {
            return columnName;
        }
    }
}
