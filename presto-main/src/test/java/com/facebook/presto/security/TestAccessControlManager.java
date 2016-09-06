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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.connector.informationSchema.InformationSchemaConnector;
import com.facebook.presto.connector.system.SystemConnector;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.security.Principal;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.connector.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.connector.ConnectorId.createSystemTablesConnectorId;
import static com.facebook.presto.security.AccessControlManager.ALLOW_ALL_ACCESS_CONTROL;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectTable;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.facebook.presto.transaction.TransactionManager.createTestTransactionManager;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestAccessControlManager
{
    private static final Principal PRINCIPAL = new TestingPrincipal("principal");
    private static final String USER_NAME = "user_name";

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Presto server is still initializing")
    public void testInitializing()
            throws Exception
    {
        AccessControlManager accessControlManager = new AccessControlManager(createTestTransactionManager());
        accessControlManager.checkCanSetUser(null, "foo");
    }

    @Test
    public void testNoneSystemAccessControl()
            throws Exception
    {
        AccessControlManager accessControlManager = new AccessControlManager(createTestTransactionManager());
        accessControlManager.setSystemAccessControl(ALLOW_ALL_ACCESS_CONTROL, ImmutableMap.<String, String>of());
        accessControlManager.checkCanSetUser(null, USER_NAME);
    }

    @Test
    public void testSetAccessControl()
            throws Exception
    {
        AccessControlManager accessControlManager = new AccessControlManager(createTestTransactionManager());

        TestSystemAccessControlFactory accessControlFactory = new TestSystemAccessControlFactory("test");
        accessControlManager.addSystemAccessControlFactory(accessControlFactory);
        accessControlManager.setSystemAccessControl("test", ImmutableMap.of());

        accessControlManager.checkCanSetUser(PRINCIPAL, USER_NAME);
        assertEquals(accessControlFactory.getCheckedUserName(), USER_NAME);
        assertEquals(accessControlFactory.getCheckedPrincipal(), PRINCIPAL);
    }

    @Test
    public void testNoCatalogAccessControl()
            throws Exception
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = new AccessControlManager(transactionManager);

        TestSystemAccessControlFactory accessControlFactory = new TestSystemAccessControlFactory("test");
        accessControlManager.addSystemAccessControlFactory(accessControlFactory);
        accessControlManager.setSystemAccessControl("test", ImmutableMap.of());

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanSelectFromTable(transactionId, new Identity(USER_NAME, Optional.of(PRINCIPAL)), new QualifiedObjectName("catalog", "schema", "table"));
                });
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Access Denied: Cannot select from table schema.table")
    public void testDenyCatalogAccessControl()
            throws Exception
    {
        CatalogManager catalogManager = new CatalogManager();
        TransactionManager transactionManager = createTestTransactionManager(catalogManager);
        AccessControlManager accessControlManager = new AccessControlManager(transactionManager);

        TestSystemAccessControlFactory accessControlFactory = new TestSystemAccessControlFactory("test");
        accessControlManager.addSystemAccessControlFactory(accessControlFactory);
        accessControlManager.setSystemAccessControl("test", ImmutableMap.of());

        ConnectorId connectorId = registerBogusConnector(catalogManager, transactionManager, "catalog");
        accessControlManager.addCatalogAccessControl(connectorId, new DenyConnectorAccessControl());

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanSelectFromTable(transactionId, new Identity(USER_NAME, Optional.of(PRINCIPAL)), new QualifiedObjectName("catalog", "schema", "table"));
                });
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Access Denied: Cannot select from table secured_catalog.schema.table")
    public void testDenySystemAccessControl()
            throws Exception
    {
        CatalogManager catalogManager = new CatalogManager();
        TransactionManager transactionManager = createTestTransactionManager(catalogManager);
        AccessControlManager accessControlManager = new AccessControlManager(transactionManager);

        TestSystemAccessControlFactory accessControlFactory = new TestSystemAccessControlFactory("test");
        accessControlManager.addSystemAccessControlFactory(accessControlFactory);
        accessControlManager.setSystemAccessControl("test", ImmutableMap.of());

        registerBogusConnector(catalogManager, transactionManager, "connector");
        accessControlManager.addCatalogAccessControl(new ConnectorId("connector"), new DenyConnectorAccessControl());

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanSelectFromTable(transactionId, new Identity(USER_NAME, Optional.of(PRINCIPAL)), new QualifiedObjectName("secured_catalog", "schema", "table"));
                });
    }

    private static ConnectorId registerBogusConnector(CatalogManager catalogManager, TransactionManager transactionManager, String catalogName)
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
                new InformationSchemaConnector(catalogName, nodeManager, metadata),
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

        private Principal checkedPrincipal;
        private String checkedUserName;

        public TestSystemAccessControlFactory(String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        public Map<String, String> getConfig()
        {
            return config;
        }

        public Principal getCheckedPrincipal()
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
                public void checkCanSetUser(Principal principal, String userName)
                {
                    checkedPrincipal = principal;
                    checkedUserName = userName;
                }

                @Override
                public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void checkCanSelectFromTable(Identity identity, CatalogSchemaTableName table)
                {
                    if (table.getCatalogName().equals("secured_catalog")) {
                        denySelectTable(table.toString());
                    }
                }
            };
        }
    }

    private static class DenyConnectorAccessControl
            implements ConnectorAccessControl
    {
        @Override
        public void checkCanSelectFromTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
        {
            denySelectTable(tableName.toString());
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
        public void checkCanSelectFromView(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName viewName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanCreateViewWithSelectFromTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanCreateViewWithSelectFromView(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName viewName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanSetCatalogSessionProperty(Identity identity, String propertyName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transactionHandle, Identity identity, Privilege privilege, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transactionHandle, Identity identity, Privilege privilege, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }
    }
}
