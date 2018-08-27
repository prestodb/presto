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

import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.security.FileBasedSystemAccessControl.Factory.CONFIG_FILE_NAME;
import static com.facebook.presto.spi.security.Privilege.SELECT;
import static com.facebook.presto.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestFileBasedSystemAccessControl
{
    private static final Identity alice = new Identity("alice", Optional.empty());
    private static final Identity kerberosValidAlice = new Identity("alice", Optional.of(new KerberosPrincipal("alice/example.com@EXAMPLE.COM")));
    private static final Identity kerberosValidNonAsciiUser = new Identity("\u0194\u0194\u0194", Optional.of(new KerberosPrincipal("\u0194\u0194\u0194/example.com@EXAMPLE.COM")));
    private static final Identity kerberosInvalidAlice = new Identity("alice", Optional.of(new KerberosPrincipal("mallory/example.com@EXAMPLE.COM")));
    private static final Identity kerberosValidShare = new Identity("alice", Optional.of(new KerberosPrincipal("valid/example.com@EXAMPLE.COM")));
    private static final Identity kerberosInValidShare = new Identity("alice", Optional.of(new KerberosPrincipal("invalid/example.com@EXAMPLE.COM")));
    private static final Identity validSpecialRegexWildDot = new Identity(".*", Optional.of(new KerberosPrincipal("special/.*@EXAMPLE.COM")));
    private static final Identity validSpecialRegexEndQuote = new Identity("\\E", Optional.of(new KerberosPrincipal("special/\\E@EXAMPLE.COM")));
    private static final Identity invalidSpecialRegex = new Identity("alice", Optional.of(new KerberosPrincipal("special/.*@EXAMPLE.COM")));
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
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_principal.json");

        try {
            accessControlManager.checkCanSetUser(Optional.empty(), alice.getUser());
            throw new AssertionError("expected AccessDeniedExeption");
        }
        catch (AccessDeniedException expected) {
        }

        accessControlManager.checkCanSetUser(kerberosValidAlice.getPrincipal(), kerberosValidAlice.getUser());
        accessControlManager.checkCanSetUser(kerberosValidNonAsciiUser.getPrincipal(), kerberosValidNonAsciiUser.getUser());
        try {
            accessControlManager.checkCanSetUser(kerberosInvalidAlice.getPrincipal(), kerberosInvalidAlice.getUser());
            throw new AssertionError("expected AccessDeniedExeption");
        }
        catch (AccessDeniedException expected) {
        }

        accessControlManager.checkCanSetUser(kerberosValidShare.getPrincipal(), kerberosValidShare.getUser());
        try {
            accessControlManager.checkCanSetUser(kerberosInValidShare.getPrincipal(), kerberosInValidShare.getUser());
            throw new AssertionError("expected AccessDeniedExeption");
        }
        catch (AccessDeniedException expected) {
        }

        accessControlManager.checkCanSetUser(validSpecialRegexWildDot.getPrincipal(), validSpecialRegexWildDot.getUser());
        accessControlManager.checkCanSetUser(validSpecialRegexEndQuote.getPrincipal(), validSpecialRegexEndQuote.getUser());
        try {
            accessControlManager.checkCanSetUser(invalidSpecialRegex.getPrincipal(), invalidSpecialRegex.getUser());
            throw new AssertionError("expected AccessDeniedExeption");
        }
        catch (AccessDeniedException expected) {
        }

        TransactionManager transactionManagerNoPatterns = createTestTransactionManager();
        AccessControlManager accessControlManagerNoPatterns = newAccessControlManager(transactionManager, "catalog.json");
        accessControlManagerNoPatterns.checkCanSetUser(kerberosValidAlice.getPrincipal(), kerberosValidAlice.getUser());
    }

    @Test
    public void testCatalogOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    assertEquals(accessControlManager.filterCatalogs(admin, allCatalogs), allCatalogs);
                    Set<String> aliceCatalogs = ImmutableSet.of("open-to-all", "alice-catalog", "all-allowed");
                    assertEquals(accessControlManager.filterCatalogs(alice, allCatalogs), aliceCatalogs);
                    Set<String> bobCatalogs = ImmutableSet.of("open-to-all", "all-allowed");
                    assertEquals(accessControlManager.filterCatalogs(bob, allCatalogs), bobCatalogs);
                    Set<String> nonAsciiUserCatalogs = ImmutableSet.of("open-to-all", "all-allowed", "\u0200\u0200\u0200");
                    assertEquals(accessControlManager.filterCatalogs(nonAsciiUser, allCatalogs), nonAsciiUserCatalogs);
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
                    assertEquals(accessControlManager.filterSchemas(transactionId, alice, "alice-catalog", aliceSchemas), aliceSchemas);
                    assertEquals(accessControlManager.filterSchemas(transactionId, bob, "alice-catalog", aliceSchemas), ImmutableSet.of());

                    accessControlManager.checkCanCreateSchema(transactionId, alice, aliceSchema);
                    accessControlManager.checkCanDropSchema(transactionId, alice, aliceSchema);
                    accessControlManager.checkCanRenameSchema(transactionId, alice, aliceSchema, "new-schema");
                    accessControlManager.checkCanShowSchemas(transactionId, alice, "alice-catalog");
                });
        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(transactionId, bob, aliceSchema);
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
                    assertEquals(accessControlManager.filterTables(transactionId, alice, "alice-catalog", aliceTables), aliceTables);
                    assertEquals(accessControlManager.filterTables(transactionId, bob, "alice-catalog", aliceTables), ImmutableSet.of());

                    accessControlManager.checkCanCreateTable(transactionId, alice, aliceTable);
                    accessControlManager.checkCanDropTable(transactionId, alice, aliceTable);
                    accessControlManager.checkCanSelectFromColumns(transactionId, alice, aliceTable, ImmutableSet.of());
                    accessControlManager.checkCanInsertIntoTable(transactionId, alice, aliceTable);
                    accessControlManager.checkCanDeleteFromTable(transactionId, alice, aliceTable);
                    accessControlManager.checkCanAddColumns(transactionId, alice, aliceTable);
                    accessControlManager.checkCanRenameColumn(transactionId, alice, aliceTable);
                });
        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateTable(transactionId, bob, aliceTable);
        }));
    }

    @Test
    public void testViewOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanCreateView(transactionId, alice, aliceView);
                    accessControlManager.checkCanDropView(transactionId, alice, aliceView);
                    accessControlManager.checkCanSelectFromColumns(transactionId, alice, aliceView, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(transactionId, alice, aliceTable, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(transactionId, alice, aliceView, ImmutableSet.of());
                    accessControlManager.checkCanSetCatalogSessionProperty(transactionId, alice, "alice-catalog", "property");
                    accessControlManager.checkCanGrantTablePrivilege(transactionId, alice, SELECT, aliceTable, "grantee", true);
                    accessControlManager.checkCanRevokeTablePrivilege(transactionId, alice, SELECT, aliceTable, "revokee", true);
                });
        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateView(transactionId, bob, aliceView);
        }));
    }

    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(SystemAccessControl.class, FileBasedSystemAccessControl.class);
    }

    private AccessControlManager newAccessControlManager(TransactionManager transactionManager, String resourceName)
    {
        AccessControlManager accessControlManager = new AccessControlManager(transactionManager);

        String path = this.getClass().getClassLoader().getResource(resourceName).getPath();
        accessControlManager.setSystemAccessControl(FileBasedSystemAccessControl.NAME, ImmutableMap.of("security.config-file", path));

        return accessControlManager;
    }

    @Test
    public void parseUnknownRules()
    {
        assertThatThrownBy(() -> parse("src/test/resources/security-config-file-with-unknown-rules.json"))
                .hasMessageContaining("Invalid JSON");
    }

    private SystemAccessControl parse(String path)
    {
        return new FileBasedSystemAccessControl.Factory().create(ImmutableMap.of(CONFIG_FILE_NAME, path));
    }
}
