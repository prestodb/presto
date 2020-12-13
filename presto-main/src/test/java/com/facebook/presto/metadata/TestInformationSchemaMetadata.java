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
package com.facebook.presto.metadata;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.connector.MockConnectorFactory;
import com.facebook.presto.connector.informationSchema.InformationSchemaColumnHandle;
import com.facebook.presto.connector.informationSchema.InformationSchemaMetadata;
import com.facebook.presto.connector.informationSchema.InformationSchemaTableHandle;
import com.facebook.presto.connector.informationSchema.InformationSchemaTableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.testing.TestingConnectorContext;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.spi.ConnectorId.createSystemTablesConnectorId;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestInformationSchemaMetadata
{
    private static final JsonCodec<ViewDefinition> VIEW_DEFINITION_JSON_CODEC = JsonCodec.jsonCodec(ViewDefinition.class);

    private final TransactionManager transactionManager;
    private final Metadata metadata;

    public TestInformationSchemaMetadata()
    {
        MockConnectorFactory.Builder builder = MockConnectorFactory.builder();
        MockConnectorFactory mockConnectorFactory = builder.withListSchemaNames(connectorSession -> ImmutableList.of("test_schema"))
                .withListTables((connectorSession, schemaNameOrNull) ->
                        ImmutableList.of(
                                new SchemaTableName("test_schema", "test_view"),
                                new SchemaTableName("test_schema", "another_table")))
                .withGetViews((connectorSession, prefix) -> {
                    String viewJson = VIEW_DEFINITION_JSON_CODEC.toJson(new ViewDefinition("select 1", Optional.of("test_catalog"), Optional.of("test_schema"), ImmutableList.of(), Optional.empty(), false));
                    SchemaTableName viewName = new SchemaTableName("test_schema", "test_view");
                    return ImmutableMap.of(viewName, new ConnectorViewDefinition(viewName, Optional.empty(), viewJson));
                }).build();
        Connector testConnector = mockConnectorFactory.create("test", ImmutableMap.of(), new TestingConnectorContext());
        CatalogManager catalogManager = new CatalogManager();
        String catalogName = "test_catalog";
        ConnectorId connectorId = new ConnectorId(catalogName);
        catalogManager.registerCatalog(new Catalog(
                catalogName,
                connectorId,
                testConnector,
                createInformationSchemaConnectorId(connectorId),
                testConnector,
                createSystemTablesConnectorId(connectorId),
                testConnector));
        transactionManager = createTestTransactionManager(catalogManager);
        metadata = createTestMetadataManager(transactionManager, new FeaturesConfig());
    }

    /**
     * Tests information schema predicate pushdown when both schema and table name are specified.
     */
    @Test
    public void testInformationSchemaPredicatePushdown()
    {
        TransactionId transactionId = transactionManager.beginTransaction(false);

        ImmutableMap.Builder<ColumnHandle, Domain> domains = new ImmutableMap.Builder<>();
        domains.put(new InformationSchemaColumnHandle("table_schema"), Domain.singleValue(VARCHAR, Slices.utf8Slice("test_schema")));
        domains.put(new InformationSchemaColumnHandle("table_name"), Domain.singleValue(VARCHAR, Slices.utf8Slice("test_view")));
        Constraint<ColumnHandle> constraint = new Constraint<>(TupleDomain.withColumnDomains(domains.build()));

        InformationSchemaMetadata informationSchemaMetadata = new InformationSchemaMetadata("test_catalog", metadata);
        List<ConnectorTableLayoutResult> layoutResults = informationSchemaMetadata.getTableLayouts(
                createNewSession(transactionId),
                new InformationSchemaTableHandle("test_catalog", "information_schema", "views"),
                constraint,
                Optional.empty());

        assertEquals(layoutResults.size(), 1);
        ConnectorTableLayoutHandle handle = layoutResults.get(0).getTableLayout().getHandle();
        assertTrue(handle instanceof InformationSchemaTableLayoutHandle);
        InformationSchemaTableLayoutHandle tableHandle = (InformationSchemaTableLayoutHandle) handle;
        assertEquals(tableHandle.getPrefixes(), ImmutableSet.of(new QualifiedTablePrefix("test_catalog", "test_schema", "test_view")));
    }

    @Test
    public void testInformationSchemaPredicatePushdownWithConstraintPredicate()
    {
        TransactionId transactionId = transactionManager.beginTransaction(false);
        Constraint<ColumnHandle> constraint = new Constraint<>(
                TupleDomain.all(),
                // test_schema has a table named "another_table" and we filter that out in this predicate
                bindings -> {
                    NullableValue catalog = bindings.get(new InformationSchemaColumnHandle("table_catalog"));
                    NullableValue schema = bindings.get(new InformationSchemaColumnHandle("table_schema"));
                    NullableValue table = bindings.get(new InformationSchemaColumnHandle("table_name"));
                    boolean isValid = true;
                    if (catalog != null) {
                        isValid = ((Slice) catalog.getValue()).toStringUtf8().equals("test_catalog");
                    }
                    if (schema != null) {
                        isValid &= ((Slice) schema.getValue()).toStringUtf8().equals("test_schema");
                    }
                    if (table != null) {
                        isValid &= ((Slice) table.getValue()).toStringUtf8().equals("test_view");
                    }
                    return isValid;
                });

        InformationSchemaMetadata informationSchemaMetadata = new InformationSchemaMetadata("test_catalog", metadata);
        List<ConnectorTableLayoutResult> layoutResults = informationSchemaMetadata.getTableLayouts(
                createNewSession(transactionId),
                new InformationSchemaTableHandle("test_catalog", "information_schema", "views"),
                constraint,
                Optional.empty());

        assertEquals(layoutResults.size(), 1);
        ConnectorTableLayoutHandle handle = layoutResults.get(0).getTableLayout().getHandle();
        assertTrue(handle instanceof InformationSchemaTableLayoutHandle);
        InformationSchemaTableLayoutHandle tableHandle = (InformationSchemaTableLayoutHandle) handle;
        assertEquals(tableHandle.getPrefixes(), ImmutableSet.of(new QualifiedTablePrefix("test_catalog", "test_schema", "test_view")));
    }

    private ConnectorSession createNewSession(TransactionId transactionId)
    {
        return testSessionBuilder()
                .setCatalog("test_catalog")
                .setSchema("information_schema")
                .setTransactionId(transactionId)
                .build()
                .toConnectorSession();
    }
}
