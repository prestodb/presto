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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.metadata.SchemaDaoUtil;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.security.FileBasedIdentityConfig;
import com.facebook.presto.raptor.security.FileBasedIdentityManager;
import com.facebook.presto.raptor.security.IdentityManager;
import com.facebook.presto.raptor.storage.StorageManagerConfig;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.TestingNodeManager;
import com.google.common.collect.ImmutableSet;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.createShardManager;
import static com.facebook.presto.spi.security.Privilege.DELETE;
import static com.facebook.presto.spi.security.Privilege.INSERT;
import static com.facebook.presto.spi.security.Privilege.SELECT;
import static com.facebook.presto.spi.security.Privilege.UPDATE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Ticker.systemTicker;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestRaptorAccessControl
{
    private static final ConnectorSession SESSION_USER1 = new TestingConnectorSession(
            "user1", new RaptorSessionProperties(new StorageManagerConfig()).getSessionProperties());
    private static final ConnectorSession SESSION_USER2 = new TestingConnectorSession(
            "user2", new RaptorSessionProperties(new StorageManagerConfig()).getSessionProperties());
    private static final SchemaTableName TABLE_TEST1_TABLE1 = new SchemaTableName("test1", "table1");
    private static final SchemaTableName TABLE_TEST2_TABLE2 = new SchemaTableName("test2", "table2");
    private static final ConnectorTransactionHandle TRANSACTION_HANDLE = new ConnectorTransactionHandle() {};
    private static final Identity USER1 = new Identity("user1", Optional.empty());
    private static final Identity USER2 = new Identity("user2", Optional.empty());

    private RaptorAccessControl accessControl;
    private DBI dbi;
    private Handle dummyHandle;
    private RaptorMetadata metadata;
    private IdentityManager identityManager;

    @BeforeMethod
    public void setup()
    {
        dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        SchemaDaoUtil.createTablesWithRetry(dbi);
        RaptorConnectorId connectorId = new RaptorConnectorId("raptor");

        NodeManager nodeManager = new TestingNodeManager();
        NodeSupplier nodeSupplier = nodeManager::getWorkerNodes;
        ShardManager shardManager = createShardManager(dbi, nodeSupplier, systemTicker());
        metadata = new RaptorMetadata(connectorId.toString(), dbi, shardManager);

        FileBasedIdentityConfig config = new FileBasedIdentityConfig();
        config.setGroupsFileName(getResourceFile("roles.json"));
        config.setSchemaOwnersFileName(getResourceFile("schema_owners.json"));
        identityManager = new FileBasedIdentityManager(config);

        accessControl = new RaptorAccessControl(dbi, identityManager, connectorId);
    }

    @AfterMethod
    public void tearDown()
    {
        dummyHandle.close();
    }

    @Test
    public void testCheckCanCreateSchema()
    {
        try {
            accessControl.checkCanCreateSchema(TRANSACTION_HANDLE, USER2, TABLE_TEST1_TABLE1.getSchemaName());
            fail();
        }
        catch (AccessDeniedException expected) {
        }

        accessControl.checkCanCreateSchema(TRANSACTION_HANDLE, USER1, TABLE_TEST1_TABLE1.getSchemaName());
    }

    @Test
    public void testCheckCanDropSchema()
    {
        try {
            accessControl.checkCanDropSchema(TRANSACTION_HANDLE, USER2, TABLE_TEST1_TABLE1.getSchemaName());
            fail();
        }
        catch (AccessDeniedException expected) {
        }

        accessControl.checkCanDropSchema(TRANSACTION_HANDLE, USER1, TABLE_TEST1_TABLE1.getSchemaName());
    }

    @Test
    public void checkCanRenameSchema()
    {
        try {
            accessControl.checkCanRenameSchema(TRANSACTION_HANDLE, USER2, TABLE_TEST1_TABLE1.getSchemaName(), TABLE_TEST2_TABLE2.getSchemaName());
            fail();
        }
        catch (AccessDeniedException expected) {
        }

        accessControl.checkCanRenameSchema(TRANSACTION_HANDLE, USER1, TABLE_TEST1_TABLE1.getSchemaName(), TABLE_TEST2_TABLE2.getSchemaName());
    }

    @Test
    public void testCheckCanShowSchemas()
    {
        accessControl.checkCanShowSchemas(TRANSACTION_HANDLE, USER1);
    }

    @Test
    public void testFilterSchemas()
    {
        Set<String> names = ImmutableSet.of("test1", "test2");
        Set<String> filtedNames = accessControl.filterSchemas(TRANSACTION_HANDLE, USER1, names);

        assertEquals(filtedNames, names);
    }

    @Test
    public void checkCanCreateTable()
    {
        try {
            accessControl.checkCanCreateTable(TRANSACTION_HANDLE, USER2, TABLE_TEST1_TABLE1);
            fail();
        }
        catch (AccessDeniedException expected) {
        }

        accessControl.checkCanCreateTable(TRANSACTION_HANDLE, USER1, TABLE_TEST1_TABLE1);
    }

    @Test
    public void checkCanDropTable()
    {
        metadata.createTable(SESSION_USER1, getTableMeta(TABLE_TEST1_TABLE1));

        try {
            accessControl.checkCanDropTable(TRANSACTION_HANDLE, USER2, TABLE_TEST1_TABLE1);
            fail();
        }
        catch (AccessDeniedException expected) {
        }

        accessControl.checkCanDropTable(TRANSACTION_HANDLE, USER1, TABLE_TEST1_TABLE1);
    }

    @Test
    public void checkCanRenameTable()
    {
        metadata.createTable(SESSION_USER1, getTableMeta(TABLE_TEST1_TABLE1));

        try {
            accessControl.checkCanRenameTable(TRANSACTION_HANDLE, USER2, TABLE_TEST1_TABLE1, TABLE_TEST2_TABLE2);
            fail();
        }
        catch (AccessDeniedException expected) {
        }

        accessControl.checkCanRenameTable(TRANSACTION_HANDLE, USER1, TABLE_TEST1_TABLE1, TABLE_TEST2_TABLE2);
    }

    @Test
    public void checkCanShowTablesMetadata()
    {
        accessControl.checkCanShowTablesMetadata(TRANSACTION_HANDLE, USER1, TABLE_TEST1_TABLE1.getSchemaName());
    }

    @Test
    public void filterTables()
    {
        Set<SchemaTableName> tables = ImmutableSet.of(TABLE_TEST1_TABLE1, TABLE_TEST2_TABLE2);
        Set<SchemaTableName> filteredTables = accessControl.filterTables(TRANSACTION_HANDLE, USER1, tables);

        assertEquals(filteredTables, tables);
    }

    @Test
    public void checkCanAddColumn()
    {
        metadata.createTable(SESSION_USER1, getTableMeta(TABLE_TEST1_TABLE1));

        try {
            accessControl.checkCanAddColumn(TRANSACTION_HANDLE, USER2, TABLE_TEST1_TABLE1);
            fail();
        }
        catch (AccessDeniedException expected) {
        }

        accessControl.checkCanAddColumn(TRANSACTION_HANDLE, USER1, TABLE_TEST1_TABLE1);
    }

    @Test
    public void checkCanRenameColumn()
    {
        metadata.createTable(SESSION_USER1, getTableMeta(TABLE_TEST1_TABLE1));

        try {
            accessControl.checkCanRenameColumn(TRANSACTION_HANDLE, USER2, TABLE_TEST1_TABLE1);
            fail();
        }
        catch (AccessDeniedException expected) {
        }

        accessControl.checkCanRenameColumn(TRANSACTION_HANDLE, USER1, TABLE_TEST1_TABLE1);
    }

    @Test
    public void checkCanSelectFromTable()
    {
        metadata.createTable(SESSION_USER1, getTableMeta(TABLE_TEST1_TABLE1));

        try {
            accessControl.checkCanSelectFromTable(TRANSACTION_HANDLE, USER2, TABLE_TEST1_TABLE1);
            fail();
        }
        catch (AccessDeniedException expected) {
        }

        accessControl.checkCanSelectFromTable(TRANSACTION_HANDLE, USER1, TABLE_TEST1_TABLE1);

        metadata.grantTablePrivileges(SESSION_USER1, TABLE_TEST1_TABLE1, ImmutableSet.of(SELECT), USER2.getUser(), false);
        accessControl.checkCanSelectFromTable(TRANSACTION_HANDLE, USER2, TABLE_TEST1_TABLE1);
    }

    @Test
    public void checkCanInsertIntoTable()
    {
        metadata.createTable(SESSION_USER1, getTableMeta(TABLE_TEST1_TABLE1));

        try {
            accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, USER2, TABLE_TEST1_TABLE1);
            fail();
        }
        catch (AccessDeniedException expected) {
        }

        accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, USER1, TABLE_TEST1_TABLE1);

        metadata.grantTablePrivileges(SESSION_USER1, TABLE_TEST1_TABLE1, ImmutableSet.of(INSERT), USER2.getUser(), false);
        accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, USER2, TABLE_TEST1_TABLE1);
    }

    @Test
    public void checkCanDeleteFromTable()
    {
        metadata.createTable(SESSION_USER1, getTableMeta(TABLE_TEST1_TABLE1));

        try {
            accessControl.checkCanDeleteFromTable(TRANSACTION_HANDLE, USER2, TABLE_TEST1_TABLE1);
            fail();
        }
        catch (AccessDeniedException expected) {
        }

        accessControl.checkCanDeleteFromTable(TRANSACTION_HANDLE, USER1, TABLE_TEST1_TABLE1);

        metadata.grantTablePrivileges(SESSION_USER1, TABLE_TEST1_TABLE1, ImmutableSet.of(DELETE), USER2.getUser(), false);
        accessControl.checkCanDeleteFromTable(TRANSACTION_HANDLE, USER2, TABLE_TEST1_TABLE1);
    }

    @Test
    public void checkCanSetCatalogSessionProperty()
    {
        try {
            accessControl.checkCanSetCatalogSessionProperty(USER2, "property");
            fail();
        }
        catch (AccessDeniedException expected) {
        }

        accessControl.checkCanSetCatalogSessionProperty(USER1, "property");
    }

    @Test
    public void checkCanGrantTablePrivilege()
    {
        metadata.createTable(SESSION_USER1, getTableMeta(TABLE_TEST1_TABLE1));
        accessControl.checkCanGrantTablePrivilege(TRANSACTION_HANDLE, USER1, SELECT, TABLE_TEST1_TABLE1);

        try {
            accessControl.checkCanGrantTablePrivilege(TRANSACTION_HANDLE, USER2, SELECT, TABLE_TEST1_TABLE1);
            fail();
        }
        catch (AccessDeniedException expected) {
        }

        metadata.grantTablePrivileges(SESSION_USER1, TABLE_TEST1_TABLE1, ImmutableSet.of(SELECT, INSERT), USER2.getUser(), true);

        accessControl.checkCanGrantTablePrivilege(TRANSACTION_HANDLE, USER2, SELECT, TABLE_TEST1_TABLE1);
        accessControl.checkCanGrantTablePrivilege(TRANSACTION_HANDLE, USER2, INSERT, TABLE_TEST1_TABLE1);
        try {
            accessControl.checkCanGrantTablePrivilege(TRANSACTION_HANDLE, USER2, UPDATE, TABLE_TEST1_TABLE1);
        }
        catch (AccessDeniedException expected) {
        }
    }

    @Test
    public void checkCanRevokeTablePrivilege()
    {
        metadata.createTable(SESSION_USER1, getTableMeta(TABLE_TEST1_TABLE1));
        accessControl.checkCanRevokeTablePrivilege(TRANSACTION_HANDLE, USER1, SELECT, TABLE_TEST1_TABLE1);

        try {
            accessControl.checkCanRevokeTablePrivilege(TRANSACTION_HANDLE, USER2, SELECT, TABLE_TEST1_TABLE1);
            fail();
        }
        catch (AccessDeniedException expected) {
        }

        metadata.grantTablePrivileges(SESSION_USER1, TABLE_TEST1_TABLE1, ImmutableSet.of(SELECT, INSERT), USER2.getUser(), true);
        accessControl.checkCanRevokeTablePrivilege(TRANSACTION_HANDLE, USER2, SELECT, TABLE_TEST1_TABLE1);
        accessControl.checkCanRevokeTablePrivilege(TRANSACTION_HANDLE, USER2, INSERT, TABLE_TEST1_TABLE1);
        try {
            accessControl.checkCanRevokeTablePrivilege(TRANSACTION_HANDLE, USER2, UPDATE, TABLE_TEST1_TABLE1);
            fail();
        }
        catch (AccessDeniedException expected) {
        }
    }

    private String getResourceFile(String name)
    {
        return this.getClass().getClassLoader().getResource(name).getFile();
    }

    private static ConnectorTableMetadata getTableMeta(SchemaTableName name)
    {
        return tableMetadataBuilder(name).column("col1", BIGINT).build();
    }
}
