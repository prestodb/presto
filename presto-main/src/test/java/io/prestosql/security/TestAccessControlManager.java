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
package io.prestosql.security;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.connector.ConnectorId;
import io.prestosql.connector.informationSchema.InformationSchemaConnector;
import io.prestosql.connector.system.SystemConnector;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.BasicPrincipal;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemAccessControlFactory;
import io.prestosql.testing.TestingConnectorContext;
import io.prestosql.transaction.TransactionManager;
import org.testng.annotations.Test;

import java.security.Principal;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.connector.ConnectorId.createInformationSchemaConnectorId;
import static io.prestosql.connector.ConnectorId.createSystemTablesConnectorId;
import static io.prestosql.spi.security.AccessDeniedException.denySelectColumns;
import static io.prestosql.spi.security.AccessDeniedException.denySelectTable;
import static io.prestosql.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.prestosql.transaction.TransactionBuilder.transaction;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestAccessControlManager
{
    private static final Principal PRINCIPAL = new BasicPrincipal("principal");
    private static final String USER_NAME = "user_name";

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Presto server is still initializing")
    public void testInitializing()
    {
        AccessControlManager accessControlManager = new AccessControlManager(createTestTransactionManager());
        accessControlManager.checkCanSetUser(Optional.empty(), "foo");
    }

    @Test
    public void testNoneSystemAccessControl()
    {
        AccessControlManager accessControlManager = new AccessControlManager(createTestTransactionManager());
        accessControlManager.setSystemAccessControl(AllowAllSystemAccessControl.NAME, ImmutableMap.of());
        accessControlManager.checkCanSetUser(Optional.empty(), USER_NAME);
    }

    @Test
    public void testReadOnlySystemAccessControl()
    {
        Identity identity = new Identity(USER_NAME, Optional.of(PRINCIPAL));
        QualifiedObjectName tableName = new QualifiedObjectName("catalog", "schema", "table");
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = new AccessControlManager(transactionManager);

        accessControlManager.setSystemAccessControl(ReadOnlySystemAccessControl.NAME, ImmutableMap.of());
        accessControlManager.checkCanSetUser(Optional.of(PRINCIPAL), USER_NAME);
        accessControlManager.checkCanSetSystemSessionProperty(identity, "property");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanSetCatalogSessionProperty(transactionId, identity, "catalog", "property");
                    accessControlManager.checkCanShowSchemas(transactionId, identity, "catalog");
                    accessControlManager.checkCanShowTablesMetadata(transactionId, identity, new CatalogSchemaName("catalog", "schema"));
                    accessControlManager.checkCanSelectFromColumns(transactionId, identity, tableName, ImmutableSet.of("column"));
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(transactionId, identity, tableName, ImmutableSet.of("column"));
                    Set<String> catalogs = ImmutableSet.of("catalog");
                    assertEquals(accessControlManager.filterCatalogs(identity, catalogs), catalogs);
                    Set<String> schemas = ImmutableSet.of("schema");
                    assertEquals(accessControlManager.filterSchemas(transactionId, identity, "catalog", schemas), schemas);
                    Set<SchemaTableName> tableNames = ImmutableSet.of(new SchemaTableName("schema", "table"));
                    assertEquals(accessControlManager.filterTables(transactionId, identity, "catalog", tableNames), tableNames);
                });

        try {
            transaction(transactionManager, accessControlManager)
                    .execute(transactionId -> {
                        accessControlManager.checkCanInsertIntoTable(transactionId, identity, tableName);
                    });
            fail();
        }
        catch (AccessDeniedException expected) {
        }
    }

    @Test
    public void testSetAccessControl()
    {
        AccessControlManager accessControlManager = new AccessControlManager(createTestTransactionManager());

        TestSystemAccessControlFactory accessControlFactory = new TestSystemAccessControlFactory("test");
        accessControlManager.addSystemAccessControlFactory(accessControlFactory);
        accessControlManager.setSystemAccessControl("test", ImmutableMap.of());

        accessControlManager.checkCanSetUser(Optional.of(PRINCIPAL), USER_NAME);
        assertEquals(accessControlFactory.getCheckedUserName(), USER_NAME);
        assertEquals(accessControlFactory.getCheckedPrincipal(), Optional.of(PRINCIPAL));
    }

    @Test
    public void testNoCatalogAccessControl()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = new AccessControlManager(transactionManager);

        TestSystemAccessControlFactory accessControlFactory = new TestSystemAccessControlFactory("test");
        accessControlManager.addSystemAccessControlFactory(accessControlFactory);
        accessControlManager.setSystemAccessControl("test", ImmutableMap.of());

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanSelectFromColumns(transactionId, new Identity(USER_NAME, Optional.of(PRINCIPAL)), new QualifiedObjectName("catalog", "schema", "table"), ImmutableSet.of("column"));
                });
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Access Denied: Cannot select from columns \\[column\\] in table or view schema.table")
    public void testDenyCatalogAccessControl()
    {
        CatalogManager catalogManager = new CatalogManager();
        TransactionManager transactionManager = createTestTransactionManager(catalogManager);
        AccessControlManager accessControlManager = new AccessControlManager(transactionManager);

        TestSystemAccessControlFactory accessControlFactory = new TestSystemAccessControlFactory("test");
        accessControlManager.addSystemAccessControlFactory(accessControlFactory);
        accessControlManager.setSystemAccessControl("test", ImmutableMap.of());

        ConnectorId connectorId = registerBogusConnector(catalogManager, transactionManager, accessControlManager, "catalog");
        accessControlManager.addCatalogAccessControl(connectorId, new DenyConnectorAccessControl());

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanSelectFromColumns(transactionId, new Identity(USER_NAME, Optional.of(PRINCIPAL)), new QualifiedObjectName("catalog", "schema", "table"), ImmutableSet.of("column"));
                });
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Access Denied: Cannot select from table secured_catalog.schema.table")
    public void testDenySystemAccessControl()
    {
        CatalogManager catalogManager = new CatalogManager();
        TransactionManager transactionManager = createTestTransactionManager(catalogManager);
        AccessControlManager accessControlManager = new AccessControlManager(transactionManager);

        TestSystemAccessControlFactory accessControlFactory = new TestSystemAccessControlFactory("test");
        accessControlManager.addSystemAccessControlFactory(accessControlFactory);
        accessControlManager.setSystemAccessControl("test", ImmutableMap.of());

        registerBogusConnector(catalogManager, transactionManager, accessControlManager, "connector");
        accessControlManager.addCatalogAccessControl(new ConnectorId("connector"), new DenyConnectorAccessControl());

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanSelectFromColumns(transactionId, new Identity(USER_NAME, Optional.of(PRINCIPAL)), new QualifiedObjectName("secured_catalog", "schema", "table"), ImmutableSet.of("column"));
                });
    }

    private static ConnectorId registerBogusConnector(CatalogManager catalogManager, TransactionManager transactionManager, AccessControl accessControl, String catalogName)
    {
        ConnectorId connectorId = new ConnectorId(catalogName);
        Connector connector = new TpchConnectorFactory().create(catalogName, ImmutableMap.of(), new TestingConnectorContext());

        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        MetadataManager metadata = MetadataManager.createTestMetadataManager(catalogManager);
        ConnectorId systemId = createSystemTablesConnectorId(connectorId);
        catalogManager.registerCatalog(new Catalog(
                catalogName,
                connectorId,
                connector,
                createInformationSchemaConnectorId(connectorId),
                new InformationSchemaConnector(catalogName, nodeManager, metadata, accessControl),
                systemId,
                new SystemConnector(
                        systemId,
                        nodeManager,
                        connector.getSystemTables(),
                        transactionId -> transactionManager.getConnectorTransaction(transactionId, connectorId))));

        return connectorId;
    }

    private static class TestSystemAccessControlFactory
            implements SystemAccessControlFactory
    {
        private final String name;
        private Map<String, String> config;

        private Optional<Principal> checkedPrincipal;
        private String checkedUserName;

        public TestSystemAccessControlFactory(String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        public Map<String, String> getConfig()
        {
            return config;
        }

        public Optional<Principal> getCheckedPrincipal()
        {
            return checkedPrincipal;
        }

        public String getCheckedUserName()
        {
            return checkedUserName;
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public SystemAccessControl create(Map<String, String> config)
        {
            this.config = config;
            return new SystemAccessControl()
            {
                @Override
                public void checkCanSetUser(Optional<Principal> principal, String userName)
                {
                    checkedPrincipal = principal;
                    checkedUserName = userName;
                }

                @Override
                public void checkCanAccessCatalog(Identity identity, String catalogName)
                {
                }

                @Override
                public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void checkCanSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
                {
                    if (table.getCatalogName().equals("secured_catalog")) {
                        denySelectTable(table.toString());
                    }
                }

                @Override
                public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
                {
                    return catalogs;
                }
            };
        }
    }

    private static class DenyConnectorAccessControl
            implements ConnectorAccessControl
    {
        @Override
        public void checkCanSelectFromColumns(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName, Set<String> columnNames)
        {
            denySelectColumns(tableName.toString(), columnNames);
        }

        @Override
        public void checkCanCreateSchema(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanDropSchema(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanRenameSchema(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName, String newSchemaName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanCreateTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanDropTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanRenameTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName, SchemaTableName newTableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanAddColumn(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanDropColumn(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanRenameColumn(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanInsertIntoTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanDeleteFromTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanCreateView(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName viewName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanDropView(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName viewName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanCreateViewWithSelectFromColumns(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName, Set<String> columnNames)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanSetCatalogSessionProperty(ConnectorTransactionHandle transactionHandle, Identity identity, String propertyName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transactionHandle, Identity identity, Privilege privilege, SchemaTableName tableName, String grantee, boolean withGrantOption)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transactionHandle, Identity identity, Privilege privilege, SchemaTableName tableName, String revokee, boolean grantOptionFor)
        {
            throw new UnsupportedOperationException();
        }
    }
}
