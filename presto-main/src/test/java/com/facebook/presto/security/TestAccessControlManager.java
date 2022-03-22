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
package com.facebook.presto.security;

import com.facebook.airlift.http.server.BasicPrincipal;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.connector.informationSchema.InformationSchemaConnector;
import com.facebook.presto.connector.system.SystemConnector;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.security.Principal;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.spi.ConnectorId.createSystemTablesConnectorId;
import static com.facebook.presto.spi.security.AccessDeniedException.denyQueryIntegrityCheck;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectColumns;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectTable;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.fail;

public class TestAccessControlManager
{
    private static final Principal PRINCIPAL = new BasicPrincipal("principal");
    private static final String USER_NAME = "user_name";
    private static final String QUERY_TOKEN_FIELD = "query_token";
    private static final String QUERY_ID = "query_id";

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Presto server is still initializing")
    public void testInitializing()
    {
        AccessControlManager accessControlManager = new AccessControlManager(createTestTransactionManager());
        accessControlManager.checkCanSetUser(
                new Identity(USER_NAME, Optional.of(PRINCIPAL)),
                new AccessControlContext(new QueryId(QUERY_ID), Optional.empty(), Optional.empty()),
                Optional.empty(),
                "foo");
    }

    @Test
    public void testNoneSystemAccessControl()
    {
        AccessControlManager accessControlManager = new AccessControlManager(createTestTransactionManager());
        accessControlManager.setSystemAccessControl(AllowAllSystemAccessControl.NAME, ImmutableMap.of());
        accessControlManager.checkCanSetUser(
                new Identity(USER_NAME, Optional.of(PRINCIPAL)),
                new AccessControlContext(new QueryId(QUERY_ID), Optional.empty(), Optional.empty()),
                Optional.empty(),
                USER_NAME);
    }

    @Test
    public void testReadOnlySystemAccessControl()
    {
        Identity identity = new Identity(USER_NAME, Optional.of(PRINCIPAL));
        QualifiedObjectName tableName = new QualifiedObjectName("catalog", "schema", "table");
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = new AccessControlManager(transactionManager);
        AccessControlContext context = new AccessControlContext(new QueryId(QUERY_ID), Optional.empty(), Optional.empty());

        accessControlManager.setSystemAccessControl(ReadOnlySystemAccessControl.NAME, ImmutableMap.of());
        accessControlManager.checkCanSetUser(identity, context, Optional.of(PRINCIPAL), USER_NAME);
        accessControlManager.checkCanSetSystemSessionProperty(identity, context, "property");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanSetCatalogSessionProperty(transactionId, identity, context, "catalog", "property");
                    accessControlManager.checkCanShowSchemas(transactionId, identity, context, "catalog");
                    accessControlManager.checkCanShowTablesMetadata(transactionId, identity, context, new CatalogSchemaName("catalog", "schema"));
                    accessControlManager.checkCanSelectFromColumns(transactionId, identity, context, tableName, ImmutableSet.of("column"));
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(transactionId, identity, context, tableName, ImmutableSet.of("column"));
                    Set<String> catalogs = ImmutableSet.of("catalog");
                    assertEquals(accessControlManager.filterCatalogs(identity, context, catalogs), catalogs);
                    Set<String> schemas = ImmutableSet.of("schema");
                    assertEquals(accessControlManager.filterSchemas(transactionId, identity, context, "catalog", schemas), schemas);
                    Set<SchemaTableName> tableNames = ImmutableSet.of(new SchemaTableName("schema", "table"));
                    assertEquals(accessControlManager.filterTables(transactionId, identity, context, "catalog", tableNames), tableNames);
                });

        try {
            transaction(transactionManager, accessControlManager)
                    .execute(transactionId -> {
                        accessControlManager.checkCanInsertIntoTable(transactionId, identity, context, tableName);
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

        accessControlManager.checkCanSetUser(
                new Identity(USER_NAME, Optional.of(PRINCIPAL)),
                new AccessControlContext(new QueryId(QUERY_ID), Optional.empty(), Optional.empty()),
                Optional.of(PRINCIPAL),
                USER_NAME);
        assertEquals(accessControlFactory.getCheckedUserName(), USER_NAME);
        assertEquals(accessControlFactory.getCheckedPrincipal(), Optional.of(PRINCIPAL));
    }

    @Test
    public void testCheckQueryIntegrity()
    {
        AccessControlManager accessControlManager = new AccessControlManager(createTestTransactionManager());
        AccessControlContext context = new AccessControlContext(new QueryId(QUERY_ID), Optional.empty(), Optional.empty());

        TestSystemAccessControlFactory accessControlFactory = new TestSystemAccessControlFactory("test");
        accessControlManager.addSystemAccessControlFactory(accessControlFactory);
        accessControlManager.setSystemAccessControl("test", ImmutableMap.of());
        String testQuery = "test_query";

        accessControlManager.checkQueryIntegrity(
                new Identity(
                        USER_NAME,
                        Optional.of(PRINCIPAL),
                        ImmutableMap.of(),
                        ImmutableMap.of(QUERY_TOKEN_FIELD, testQuery),
                        ImmutableMap.of()),
                context,
                testQuery);
        assertEquals(accessControlFactory.getCheckedUserName(), USER_NAME);
        assertEquals(accessControlFactory.getCheckedPrincipal(), Optional.of(PRINCIPAL));
        assertEquals(accessControlFactory.getCheckedQuery(), testQuery);

        assertThrows(
                AccessDeniedException.class,
                () -> accessControlManager.checkQueryIntegrity(
                        new Identity(
                                USER_NAME,
                                Optional.of(PRINCIPAL),
                                ImmutableMap.of(),
                                ImmutableMap.of(QUERY_TOKEN_FIELD, testQuery + " modified"),
                                ImmutableMap.of()),
                        context,
                        testQuery));
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
                    accessControlManager.checkCanSelectFromColumns(transactionId, new Identity(USER_NAME, Optional.of(PRINCIPAL)),
                            new AccessControlContext(new QueryId(QUERY_ID), Optional.empty(), Optional.empty()),
                            new QualifiedObjectName("catalog", "schema", "table"), ImmutableSet.of("column"));
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
                    accessControlManager.checkCanSelectFromColumns(transactionId, new Identity(USER_NAME, Optional.of(PRINCIPAL)),
                            new AccessControlContext(new QueryId(QUERY_ID), Optional.empty(), Optional.empty()),
                            new QualifiedObjectName("catalog", "schema", "table"), ImmutableSet.of("column"));
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
                    accessControlManager.checkCanSelectFromColumns(transactionId, new Identity(USER_NAME, Optional.of(PRINCIPAL)),
                            new AccessControlContext(new QueryId(QUERY_ID), Optional.empty(), Optional.empty()),
                            new QualifiedObjectName("secured_catalog", "schema", "table"), ImmutableSet.of("column"));
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
                new InformationSchemaConnector(catalogName, nodeManager, metadata, accessControl, ImmutableList.of()),
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
        private String checkedQuery;

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

        public String getCheckedQuery()
        {
            return checkedQuery;
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
                public void checkCanSetUser(Identity identity, AccessControlContext context, Optional<Principal> principal, String userName)
                {
                    checkedPrincipal = principal;
                    checkedUserName = userName;
                }

                @Override
                public void checkQueryIntegrity(Identity identity, AccessControlContext context, String query)
                {
                    if (!query.equals(identity.getExtraCredentials().get(QUERY_TOKEN_FIELD))) {
                        denyQueryIntegrityCheck();
                    }
                    checkedUserName = identity.getUser();
                    checkedPrincipal = identity.getPrincipal();
                    checkedQuery = query;
                }

                @Override
                public void checkCanAccessCatalog(Identity identity, AccessControlContext context, String catalogName)
                {
                }

                @Override
                public void checkCanSetSystemSessionProperty(Identity identity, AccessControlContext context, String propertyName)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void checkCanSelectFromColumns(Identity identity, AccessControlContext context, CatalogSchemaTableName table, Set<String> columns)
                {
                    if (table.getCatalogName().equals("secured_catalog")) {
                        denySelectTable(table.toString());
                    }
                }

                @Override
                public Set<String> filterCatalogs(Identity identity, AccessControlContext context, Set<String> catalogs)
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
        public void checkCanSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName, Set<String> columnNames)
        {
            denySelectColumns(tableName.toString(), columnNames);
        }

        @Override
        public void checkCanCreateSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanDropSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanRenameSchema(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String schemaName, String newSchemaName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanCreateTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanDropTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanRenameTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName, SchemaTableName newTableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanAddColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanDropColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanRenameColumn(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanInsertIntoTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanDeleteFromTable(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanCreateView(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName viewName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanDropView(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName viewName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanCreateViewWithSelectFromColumns(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, SchemaTableName tableName, Set<String> columnNames)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanSetCatalogSessionProperty(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, String propertyName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, Privilege privilege, SchemaTableName tableName, PrestoPrincipal grantee, boolean withGrantOption)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity, AccessControlContext context, Privilege privilege, SchemaTableName tableName, PrestoPrincipal revokee, boolean grantOptionFor)
        {
            throw new UnsupportedOperationException();
        }
    }
}
