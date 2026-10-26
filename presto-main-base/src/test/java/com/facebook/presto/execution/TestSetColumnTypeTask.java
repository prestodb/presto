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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.AbstractMockMetadata;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SetColumnType;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestSetColumnTypeTask
{
    private static final String CATALOG_NAME = "catalog";
    private static final String SCHEMA = "schema";

    private Session testSession;
    private TransactionManager transactionManager;
    private MockMetadata metadata;

    @BeforeMethod
    public void setUp()
    {
        CatalogManager catalogManager = new CatalogManager();
        catalogManager.registerCatalog(createBogusTestingCatalog(CATALOG_NAME));
        transactionManager = createTestTransactionManager(catalogManager);
        testSession = testSessionBuilder()
                .setTransactionId(transactionManager.beginTransaction(false))
                .build();
        ConnectorId connectorId = catalogManager.getCatalog(CATALOG_NAME).get().getConnectorId();
        metadata = new MockMetadata(createTestFunctionAndTypeManager(), connectorId);
    }

    @Test
    public void testSetDataTypeNotExistingTable()
    {
        assertThatThrownBy(() -> getFutureValue(executeSetColumnType(
                QualifiedName.of(CATALOG_NAME, SCHEMA, "not_existing_table"),
                QualifiedName.of("test"),
                "INTEGER",
                false)));
    }

    @Test
    public void testSetDataTypeNotExistingTableIfExists()
    {
        getFutureValue(executeSetColumnType(
                QualifiedName.of(CATALOG_NAME, SCHEMA, "not_existing_table"),
                QualifiedName.of("test"),
                "INTEGER",
                true));
        // no exception
    }

    @Test
    public void testSetDataTypeNestedColumn()
    {
        getFutureValue(executeSetColumnType(
                QualifiedName.of(CATALOG_NAME, SCHEMA, "existing_table"),
                QualifiedName.of("info", "age"),
                "BIGINT",
                false));
        assertThat(metadata.getFieldPath()).containsExactly("age");
        assertThat(metadata.getFieldColumn()).isEqualTo("info");
    }

    @Test
    public void testSetDataTypeTopLevelColumn()
    {
        getFutureValue(executeSetColumnType(
                QualifiedName.of(CATALOG_NAME, SCHEMA, "existing_table"),
                QualifiedName.of("info"),
                "BIGINT",
                false));
        assertThat(metadata.getFieldPath()).isNull();
        assertThat(metadata.isColumnTypeSet()).isTrue();
    }

    @Test
    public void testSetDataTypeNotExistingColumn()
    {
        assertThatThrownBy(() -> getFutureValue(executeSetColumnType(
                QualifiedName.of(CATALOG_NAME, SCHEMA, "existing_table"),
                QualifiedName.of("not_existing_column"),
                "INTEGER",
                false)));
    }

    private ListenableFuture<Void> executeSetColumnType(QualifiedName table, QualifiedName column, String type, boolean exists)
    {
        return new SetColumnTypeTask(metadata)
                .execute(new SetColumnType(new NodeLocation(1, 1), table, column, type, exists),
                        transactionManager, metadata, new AllowAllAccessControl(),
                        testSession, ImmutableList.of(), (WarningCollector) null, null);
    }

    private static final class MockMetadata
            extends AbstractMockMetadata
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private final TableHandle tableHandle;

        private List<String> fieldPath;
        private String fieldColumn;
        private boolean columnTypeSet;

        MockMetadata(FunctionAndTypeManager functionAndTypeManager, ConnectorId connectorId)
        {
            this.functionAndTypeManager = functionAndTypeManager;
            this.tableHandle = new TableHandle(connectorId, new TestingTableHandle(), TestingTransactionHandle.create(), Optional.empty());
        }

        @Override
        public MetadataResolver getMetadataResolver(Session session)
        {
            MetadataResolver base = super.getMetadataResolver(session);
            return new MetadataResolver()
            {
                @Override
                public boolean catalogExists(String catalogName)
                {
                    return base.catalogExists(catalogName);
                }

                @Override
                public boolean schemaExists(CatalogSchemaName schemaName)
                {
                    return base.schemaExists(schemaName);
                }

                @Override
                public Optional<TableHandle> getTableHandle(QualifiedObjectName tableName)
                {
                    return tableName.getObjectName().equals("existing_table") ? Optional.of(tableHandle) : Optional.empty();
                }

                @Override
                public List<ColumnMetadata> getColumns(TableHandle t)
                {
                    return base.getColumns(t);
                }

                @Override
                public Map<String, ColumnHandle> getColumnHandles(TableHandle t)
                {
                    return base.getColumnHandles(t);
                }

                @Override
                public Optional<ViewDefinition> getView(QualifiedObjectName v)
                {
                    return base.getView(v);
                }

                @Override
                public Optional<MaterializedViewDefinition> getMaterializedView(QualifiedObjectName v)
                {
                    return base.getMaterializedView(v);
                }
            };
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
        {
            return ImmutableMap.of("info", new TestingColumnHandle("info"));
        }

        @Override
        public Type getType(TypeSignature signature)
        {
            return functionAndTypeManager.getType(signature);
        }

        @Override
        public void setColumnType(Session session, TableHandle tableHandle, ColumnHandle columnHandle, Type type)
        {
            this.columnTypeSet = true;
        }

        @Override
        public void setFieldType(Session session, TableHandle tableHandle, ColumnHandle columnHandle, List<String> fieldPath, Type type)
        {
            this.fieldPath = fieldPath;
            this.fieldColumn = ((TestingColumnHandle) columnHandle).getName();
        }

        List<String> getFieldPath()
        {
            return fieldPath;
        }

        String getFieldColumn()
        {
            return fieldColumn;
        }

        boolean isColumnTypeSet()
        {
            return columnTypeSet;
        }
    }
}
