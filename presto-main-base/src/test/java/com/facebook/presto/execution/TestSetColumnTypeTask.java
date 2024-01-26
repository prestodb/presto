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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.connector.informationSchema.InformationSchemaTableHandle;
import com.facebook.presto.connector.informationSchema.InformationSchemaTransactionHandle;
import com.facebook.presto.metadata.AbstractMockMetadata;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.ColumnPropertyManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SetColumnType;
import com.facebook.presto.testing.TestingConnectorContext;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.facebook.presto.sql.QueryUtil.identifier;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.collect.Sets.immutableEnumSet;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestSetColumnTypeTask
{
    public static final String SCHEMA = "schema";
    private static final String CATALOG_NAME = "catalog";
    private Session testSession;

    protected TransactionManager transactionManager;
    protected MockMetadata metadata;
    protected AccessControl accessControl;
    protected ConnectorContext context;
    protected WarningCollector warningCollector;

    protected static QualifiedObjectName qualifiedObjectName(String objectName)
    {
        return new QualifiedObjectName(CATALOG_NAME, SCHEMA, objectName);
    }

    protected static QualifiedName asQualifiedName(QualifiedObjectName qualifiedObjectName)
    {
        return QualifiedName.of(qualifiedObjectName.getCatalogName(), qualifiedObjectName.getSchemaName(), qualifiedObjectName.getObjectName());
    }

    @BeforeMethod
    public void setUp()
    {
        CatalogManager catalogManager = new CatalogManager();
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        transactionManager = createTestTransactionManager(catalogManager);
        TablePropertyManager tablePropertyManager = new TablePropertyManager();
        ColumnPropertyManager columnPropertyManager = new ColumnPropertyManager();
        Catalog testCatalog = createBogusTestingCatalog(CATALOG_NAME);
        catalogManager.registerCatalog(testCatalog);
        tablePropertyManager.addProperties(testCatalog.getConnectorId(),
                ImmutableList.of(stringProperty("baz", "test property", null, false)));
        columnPropertyManager.addProperties(testCatalog.getConnectorId(), ImmutableList.of());
        testSession = testSessionBuilder()
                .setTransactionId(transactionManager.beginTransaction(false))
                .build();
        context = new TestingConnectorContext();
        metadata = new MockMetadata(
            functionAndTypeManager,
            tablePropertyManager,
            columnPropertyManager,
            testCatalog.getConnectorId(),
            emptySet(), testCatalog);
    }
    @Test
    public void testSetDataType()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        ConnectorId connectorId = new ConnectorId("test");
        ConnectorTableHandle connectorHandle = new InformationSchemaTableHandle("catalog", "schema", "existing_table");
        UUID uuid = UUID.fromString("ffe9ae3e-60de-4175-a0b5-d635767085fa");
        ConnectorTransactionHandle connectorTransactionHandle = new InformationSchemaTransactionHandle(new TransactionId(uuid));
        TableHandle tableHandle = new TableHandle(connectorId, connectorHandle, connectorTransactionHandle, Optional.empty());
        ColumnMetadata columnMetadata = ColumnMetadata.builder().setName("test").setType(BIGINT).build();
        ConnectorTableMetadata ctMetaData = new ConnectorTableMetadata(((InformationSchemaTableHandle) tableHandle.getConnectorHandle()).getSchemaTableName(), ImmutableList.of(columnMetadata));
        metadata.createTable(null, "catalog", ctMetaData, true);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(testSession, tableHandle).getMetadata();
        assertEquals(tableMetadata.getColumns(), ImmutableList.of(
                columnMetadata));

        columnMetadata = ColumnMetadata.builder().setName("test").setType(INTEGER).build();
        ctMetaData = new ConnectorTableMetadata(((InformationSchemaTableHandle) tableHandle.getConnectorHandle()).getSchemaTableName(), ImmutableList.of(columnMetadata));
        metadata.createTable(null, "catalog", ctMetaData, true);

        // Change the column type to integer from bigint
        getFutureValue(executeSetColumnType(asQualifiedName(tableName), identifier("test"), "INTEGER", true));
        assertThat(metadata.getTableMetadata(testSession, tableHandle).getColumns())
                .isEqualTo(ImmutableList.of(ColumnMetadata.builder().setName("test").setType(INTEGER).build()));

        // Specify the same column type
        getFutureValue(executeSetColumnType(asQualifiedName(tableName), identifier("test"), "INTEGER", true));
        assertThat(metadata.getTableMetadata(testSession, tableHandle).getColumns())
                .isEqualTo(ImmutableList.of(ColumnMetadata.builder().setName("test").setType(INTEGER).build()));
    }

    @Test
    public void testSetDataTypeNotExistingTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("not_existing_table");

        assertThatThrownBy(() -> getFutureValue(executeSetColumnType(asQualifiedName(tableName), identifier("test"), "INTEGER", false)));
    }

    @Test
    public void testSetDataTypeNotExistingTableIfExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("not_existing_table");

        getFutureValue(executeSetColumnType(asQualifiedName(tableName), identifier("test"), "INTEGER", true));
        // no exception
    }

    @Test
    public void testSetDataTypeNotExistingColumn()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        assertThatThrownBy(() -> getFutureValue(executeSetColumnType(asQualifiedName(tableName), identifier("not_existing_column"), "INTEGER", false)));
    }

    private ListenableFuture<Void> executeSetColumnType(QualifiedName table, Identifier column, String type, boolean exists)
    {
        return new SetColumnTypeTask(metadata)
                .execute(new SetColumnType(new NodeLocation(1, 1), table, column, type, exists), transactionManager, metadata, accessControl, testSession, ImmutableList.of(), warningCollector, null);
    }

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private final TablePropertyManager tablePropertyManager;
        private final ColumnPropertyManager columnPropertyManager;
        private final ConnectorId catalogHandle;
        private final Map<SchemaTableName, ConnectorTableMetadata> tables = new ConcurrentHashMap<>();
        private final Set<ConnectorCapabilities> connectorCapabilities;
        private final Catalog catalog;
        public MockMetadata(
                FunctionAndTypeManager functionAndTypeManager,
                TablePropertyManager tablePropertyManager,
                ColumnPropertyManager columnPropertyManager,
                ConnectorId catalogHandle,
                Set<ConnectorCapabilities> connectorCapabilities, Catalog testCatalog)
        {
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
            this.columnPropertyManager = requireNonNull(columnPropertyManager, "columnPropertyManager is null");
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
            this.catalog = testCatalog;
            this.connectorCapabilities = requireNonNull(immutableEnumSet(connectorCapabilities), "connectorCapabilities is null");
        }

        @Override
        public void setColumnType(Session session, TableHandle tableHandle, ColumnHandle columnHandle, Type type)
        {
            SchemaTableName tableName = getTableName(tableHandle);
            ConnectorTableMetadata metadata = tables.get(tableName);

            ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builderWithExpectedSize(metadata.getColumns().size());
            for (ColumnMetadata column : metadata.getColumns()) {
                if (column.getName().equals(((TestingColumnHandle) columnHandle).getName())) {
                    columns.add(ColumnMetadata.builder().setName(column.getName()).setType(type).build());
                }
                else {
                    columns.add(column);
                }
            }
            tables.put(tableName, new ConnectorTableMetadata(tableName, columns.build()));
        }

        @Override
        public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
        {
            tables.put(tableMetadata.getTable(), tableMetadata);
            if (!ignoreExisting) {
                throw new PrestoException(ALREADY_EXISTS, "Table already exists");
            }
        }

        @Override
        public TablePropertyManager getTablePropertyManager()
        {
            return tablePropertyManager;
        }

        @Override
        public ColumnPropertyManager getColumnPropertyManager()
        {
            return columnPropertyManager;
        }

        @Override
        public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
        {
            return new TableMetadata(catalog.getConnectorId(), getTableMetadata(tableHandle));
        }

        private ConnectorTableMetadata getTableMetadata(TableHandle tableHandle)
        {
            return tables.get(((InformationSchemaTableHandle) tableHandle.getConnectorHandle()).getSchemaTableName());
        }
        private SchemaTableName getTableName(TableHandle tableHandle)
        {
            return ((InformationSchemaTableHandle) tableHandle.getConnectorHandle()).getSchemaTableName();
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
        {
            return getTableMetadata(tableHandle).getColumns().stream()
                    .collect(toImmutableMap(
                        ColumnMetadata::getName,
                        column -> new TestingColumnHandle(column.getName())));
        }

        @Override
        public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            String columnName = ((TestingColumnHandle) columnHandle).getName();
            return getTableMetadata(tableHandle).getColumns().stream()
                    .filter(column -> column.getName().equals(columnName))
                    .collect(onlyElement());
        }

        @Override
        public Type getType(TypeSignature signature)
        {
            return functionAndTypeManager.getType(signature);
        }

        @Override
        public Optional<ConnectorId> getCatalogHandle(Session session, String catalogName)
        {
            if (catalogHandle.getCatalogName().equals(catalogName)) {
                return Optional.of(catalogHandle);
            }
            return Optional.empty();
        }

        @Override
        public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, ConnectorId catalogName)
        {
            return connectorCapabilities;
        }
    }
}
