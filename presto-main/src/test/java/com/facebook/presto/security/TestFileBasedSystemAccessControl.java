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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.security.Privilege.SELECT;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.facebook.presto.transaction.TransactionManager.createTestTransactionManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestFileBasedSystemAccessControl
{
    private static final Identity alice = new Identity("alice", Optional.empty());
    private static final Identity kerberosValidAlice = new Identity("alice", Optional.of(new KerberosPrincipal("alice/example.com@EXAMPLE.COM")));
    private static final Identity kerberosInvalidAlice = new Identity("alice", Optional.of(new KerberosPrincipal("mallory/example.com@EXAMPLE.COM")));
    private static final Identity bob = new Identity("bob", Optional.empty());
    private static final Identity admin = new Identity("admin", Optional.empty());
    private static final Identity nonAsciiUser = new Identity("\u0194\u0194\u0194", Optional.empty());
    private static final Set<String> allCatalogs = ImmutableSet.of("secret", "open-to-all", "all-allowed", "alice-catalog", "allowed-absent", "\u0200\u0200\u0200");
    private static final QualifiedObjectName aliceTable = new QualifiedObjectName("alice-catalog", "schema", "table");
    private static final QualifiedObjectName aliceView = new QualifiedObjectName("alice-catalog", "schema", "view");
    private static final CatalogSchemaName aliceSchema = new CatalogSchemaName("alice-catalog", "schema");

    @Test
    public void testCanSetUserOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_user_patterns.json");

        try {
            accessControlManager.checkCanSetUser(null, alice.getUser());
            throw new AssertionError("expected AccessDeniedExeption");
        }
        catch (AccessDeniedException expected) {
        }

        accessControlManager.checkCanSetUser(kerberosValidAlice.getPrincipal().get(), kerberosValidAlice.getUser());
        try {
            accessControlManager.checkCanSetUser(kerberosInvalidAlice.getPrincipal().get(), kerberosInvalidAlice.getUser());
            throw new AssertionError("expected AccessDeniedExeption");
        }
        catch (AccessDeniedException expected) {
        }

        TransactionManager transactionManagerNoPatterns = createTestTransactionManager();
        AccessControlManager accessControlManagerNoPatterns = newAccessControlManager(transactionManager, "catalog.json");
        accessControlManagerNoPatterns.checkCanSetUser(kerberosValidAlice.getPrincipal().get(), kerberosValidAlice.getUser());
    }

    @Test
    public void testCatalogOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    assertEquals(accessControlManager.filterCatalogs(newSession(transactionId, admin), allCatalogs), allCatalogs);
                    Set<String> aliceCatalogs = ImmutableSet.of("open-to-all", "alice-catalog", "all-allowed");
                    assertEquals(accessControlManager.filterCatalogs(newSession(transactionId, alice), allCatalogs), aliceCatalogs);
                    Set<String> bobCatalogs = ImmutableSet.of("open-to-all", "all-allowed");
                    assertEquals(accessControlManager.filterCatalogs(newSession(transactionId, bob), allCatalogs), bobCatalogs);
                    Set<String> nonAsciiUserCatalogs = ImmutableSet.of("open-to-all", "all-allowed", "\u0200\u0200\u0200");
                    assertEquals(accessControlManager.filterCatalogs(newSession(transactionId, nonAsciiUser), allCatalogs), nonAsciiUserCatalogs);
                });
    }

    @Test
    public void testSchemaOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    Set<String> aliceSchemas = ImmutableSet.of("schema");
                    assertEquals(accessControlManager.filterSchemas(newSession(transactionId, alice), "alice-catalog", aliceSchemas), aliceSchemas);
                    assertEquals(accessControlManager.filterSchemas(newSession(transactionId, bob), "alice-catalog", aliceSchemas), ImmutableSet.of());

                    accessControlManager.checkCanCreateSchema(newSession(transactionId, alice), aliceSchema);
                    accessControlManager.checkCanDropSchema(newSession(transactionId, alice), aliceSchema);
                    accessControlManager.checkCanRenameSchema(newSession(transactionId, alice), aliceSchema, "new-schema");
                    accessControlManager.checkCanShowSchemas(newSession(transactionId, alice), "alice-catalog");
                });
        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(newSession(transactionId, bob), aliceSchema);
        }));
    }

    @Test
    public void testTableOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    Set<SchemaTableName> aliceTables = ImmutableSet.of(new SchemaTableName("schema", "table"));
                    assertEquals(accessControlManager.filterTables(newSession(transactionId, alice), "alice-catalog", aliceTables), aliceTables);
                    assertEquals(accessControlManager.filterTables(newSession(transactionId, bob), "alice-catalog", aliceTables), ImmutableSet.of());

                    accessControlManager.checkCanCreateTable(newSession(transactionId, alice), aliceTable);
                    accessControlManager.checkCanDropTable(newSession(transactionId, alice), aliceTable);
                    accessControlManager.checkCanSelectFromTable(newSession(transactionId, alice), aliceTable);
                    accessControlManager.checkCanInsertIntoTable(newSession(transactionId, alice), aliceTable);
                    accessControlManager.checkCanDeleteFromTable(newSession(transactionId, alice), aliceTable);
                    accessControlManager.checkCanAddColumns(newSession(transactionId, alice), aliceTable);
                    accessControlManager.checkCanRenameColumn(newSession(transactionId, alice), aliceTable);
                });
        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateTable(newSession(transactionId, bob), aliceTable);
        }));
    }

    @Test
    public void testViewOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanCreateView(newSession(transactionId, alice), aliceView);
                    accessControlManager.checkCanDropView(newSession(transactionId, alice), aliceView);
                    accessControlManager.checkCanSelectFromView(newSession(transactionId, alice), aliceView);
                    accessControlManager.checkCanCreateViewWithSelectFromTable(newSession(transactionId, alice), aliceTable);
                    accessControlManager.checkCanCreateViewWithSelectFromView(newSession(transactionId, alice), aliceView);
                    accessControlManager.checkCanSetCatalogSessionProperty(transactionId, alice, "alice-catalog", "property");
                    accessControlManager.checkCanGrantTablePrivilege(newSession(transactionId, alice), SELECT, aliceTable, "grantee", true);
                    accessControlManager.checkCanRevokeTablePrivilege(newSession(transactionId, alice), SELECT, aliceTable, "revokee", true);
                });
        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateView(newSession(transactionId, bob), aliceView);
        }));
    }

    private AccessControlManager newAccessControlManager(TransactionManager transactionManager, String resourceName)
    {
        AccessControlManager accessControlManager = new AccessControlManager(transactionManager);

        String path = this.getClass().getClassLoader().getResource(resourceName).getPath();
        accessControlManager.setSystemAccessControl(FileBasedSystemAccessControl.NAME, ImmutableMap.of("security.config-file", path));

        return accessControlManager;
    }

    private static Session newSession(TransactionId transactionId, Identity identity)
    {
        return testSessionBuilder()
                .setTransactionId(transactionId)
                .setIdentity(identity)
                .build();
    }

    private static Session newSession(TransactionId transactionId, String catalogName, String schemaName, Identity identity)
    {
        return testSessionBuilder()
                .setTransactionId(transactionId)
                .setCatalog(catalogName)
                .setSchema(schemaName)
                .setIdentity(identity)
                .build();
    }
}
