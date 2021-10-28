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

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.io.File;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
import static com.facebook.presto.plugin.base.security.FileBasedAccessControlConfig.SECURITY_REFRESH_PERIOD;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.facebook.presto.spi.security.Privilege.SELECT;
import static com.facebook.presto.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.io.Files.copy;
import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Files.newTemporaryFile;
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
    private static final Set<String> allCatalogs = ImmutableSet.of("secret", "open-to-all", "all-allowed", "alice-catalog", "\u0200\u0200\u0200");
    private static final QualifiedObjectName aliceTable = new QualifiedObjectName("alice-catalog", "schema", "table");
    private static final QualifiedObjectName aliceView = new QualifiedObjectName("alice-catalog", "schema", "view");
    private static final CatalogSchemaName aliceSchema = new CatalogSchemaName("alice-catalog", "schema");
    private static final AccessControlContext context = new AccessControlContext(new QueryId("query_id"), Optional.empty(), Optional.empty());

    @Test
    public void testCanSetUserOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_principal.json");

        try {
            accessControlManager.checkCanSetUser(alice, context, Optional.empty(), alice.getUser());
            throw new AssertionError("expected AccessDeniedExeption");
        }
        catch (AccessDeniedException expected) {
        }

        accessControlManager.checkCanSetUser(kerberosValidAlice, context, kerberosValidAlice.getPrincipal(), kerberosValidAlice.getUser());
        accessControlManager.checkCanSetUser(kerberosValidNonAsciiUser, context, kerberosValidNonAsciiUser.getPrincipal(), kerberosValidNonAsciiUser.getUser());
        try {
            accessControlManager.checkCanSetUser(kerberosInvalidAlice, context, kerberosInvalidAlice.getPrincipal(), kerberosInvalidAlice.getUser());
            throw new AssertionError("expected AccessDeniedExeption");
        }
        catch (AccessDeniedException expected) {
        }

        accessControlManager.checkCanSetUser(kerberosValidShare, context, kerberosValidShare.getPrincipal(), kerberosValidShare.getUser());
        try {
            accessControlManager.checkCanSetUser(kerberosInValidShare, context, kerberosInValidShare.getPrincipal(), kerberosInValidShare.getUser());
            throw new AssertionError("expected AccessDeniedExeption");
        }
        catch (AccessDeniedException expected) {
        }

        accessControlManager.checkCanSetUser(validSpecialRegexWildDot, context, validSpecialRegexWildDot.getPrincipal(), validSpecialRegexWildDot.getUser());
        accessControlManager.checkCanSetUser(validSpecialRegexEndQuote, context, validSpecialRegexEndQuote.getPrincipal(), validSpecialRegexEndQuote.getUser());
        try {
            accessControlManager.checkCanSetUser(invalidSpecialRegex, context, invalidSpecialRegex.getPrincipal(), invalidSpecialRegex.getUser());
            throw new AssertionError("expected AccessDeniedExeption");
        }
        catch (AccessDeniedException expected) {
        }

        TransactionManager transactionManagerNoPatterns = createTestTransactionManager();
        AccessControlManager accessControlManagerNoPatterns = newAccessControlManager(transactionManager, "catalog.json");
        accessControlManagerNoPatterns.checkCanSetUser(kerberosValidAlice, context, kerberosValidAlice.getPrincipal(), kerberosValidAlice.getUser());
    }

    @Test
    public void testCatalogOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    assertEquals(accessControlManager.filterCatalogs(admin, context, allCatalogs), allCatalogs);
                    Set<String> aliceCatalogs = ImmutableSet.of("open-to-all", "alice-catalog", "all-allowed");
                    assertEquals(accessControlManager.filterCatalogs(alice, context, allCatalogs), aliceCatalogs);
                    Set<String> bobCatalogs = ImmutableSet.of("open-to-all", "all-allowed");
                    assertEquals(accessControlManager.filterCatalogs(bob, context, allCatalogs), bobCatalogs);
                    Set<String> nonAsciiUserCatalogs = ImmutableSet.of("open-to-all", "all-allowed", "\u0200\u0200\u0200");
                    assertEquals(accessControlManager.filterCatalogs(nonAsciiUser, context, allCatalogs), nonAsciiUserCatalogs);
                });
    }

    @Test
    public void testCatalogOperationsReadOnly()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_read_only.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    assertEquals(accessControlManager.filterCatalogs(admin, context, allCatalogs), allCatalogs);
                    Set<String> aliceCatalogs = ImmutableSet.of("open-to-all", "alice-catalog", "all-allowed");
                    assertEquals(accessControlManager.filterCatalogs(alice, context, allCatalogs), aliceCatalogs);
                    Set<String> bobCatalogs = ImmutableSet.of("open-to-all", "all-allowed");
                    assertEquals(accessControlManager.filterCatalogs(bob, context, allCatalogs), bobCatalogs);
                    Set<String> nonAsciiUserCatalogs = ImmutableSet.of("open-to-all", "all-allowed", "\u0200\u0200\u0200");
                    assertEquals(accessControlManager.filterCatalogs(nonAsciiUser, context, allCatalogs), nonAsciiUserCatalogs);
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
                    assertEquals(accessControlManager.filterSchemas(transactionId, alice, context, "alice-catalog", aliceSchemas), aliceSchemas);
                    assertEquals(accessControlManager.filterSchemas(transactionId, bob, context, "alice-catalog", aliceSchemas), ImmutableSet.of());

                    accessControlManager.checkCanCreateSchema(transactionId, alice, context, aliceSchema);
                    accessControlManager.checkCanDropSchema(transactionId, alice, context, aliceSchema);
                    accessControlManager.checkCanRenameSchema(transactionId, alice, context, aliceSchema, "new-schema");
                    accessControlManager.checkCanShowSchemas(transactionId, alice, context, "alice-catalog");
                });
        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(transactionId, bob, context, aliceSchema);
        }));
    }

    @Test
    public void testSchemaRulesForCheckCanCreateSchema()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "file-based-system-access-schema.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanCreateSchema(transactionId, bob, context, new CatalogSchemaName("alice-catalog", "bob"));
                    accessControlManager.checkCanCreateSchema(transactionId, bob, context, new CatalogSchemaName("bob-catalog", "bob"));
                    accessControlManager.checkCanCreateSchema(transactionId, admin, context, new CatalogSchemaName("some-catalog", "some-schema"));
                    accessControlManager.checkCanCreateSchema(transactionId, admin, context, new CatalogSchemaName("some-catalog", "bob"));
                    accessControlManager.checkCanCreateSchema(transactionId, admin, context, new CatalogSchemaName("some-catalog", "alice"));
                });

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(transactionId, bob, context, new CatalogSchemaName("alice-catalog", "alice"));
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(transactionId, bob, context, new CatalogSchemaName("bob-catalog", "alice"));
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(transactionId, bob, context, new CatalogSchemaName("secret-catalog", "secret"));
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(transactionId, alice, context, new CatalogSchemaName("secret-catalog", "secret"));
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(transactionId, admin, context, new CatalogSchemaName("secret-catalog", "secret"));
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(transactionId, alice, context, new CatalogSchemaName("alice-catalog", "alice"));
        }));
    }

    @Test
    public void testSchemaRulesForCheckCanDropSchema()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "file-based-system-access-schema.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanDropSchema(transactionId, bob, context, new CatalogSchemaName("alice-catalog", "bob"));
                    accessControlManager.checkCanDropSchema(transactionId, bob, context, new CatalogSchemaName("bob-catalog", "bob"));
                    accessControlManager.checkCanDropSchema(transactionId, admin, context, new CatalogSchemaName("some-catalog", "bob"));
                    accessControlManager.checkCanDropSchema(transactionId, admin, context, new CatalogSchemaName("some-catalog", "alice"));
                    accessControlManager.checkCanDropSchema(transactionId, admin, context, new CatalogSchemaName("some-catalog", "some-schema"));
                });

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropSchema(transactionId, bob, context, new CatalogSchemaName("alice-catalog", "alice"));
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropSchema(transactionId, bob, context, new CatalogSchemaName("bob-catalog", "alice"));
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropSchema(transactionId, bob, context, new CatalogSchemaName("secret-catalog", "secret"));
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropSchema(transactionId, alice, context, new CatalogSchemaName("secret-catalog", "secret"));
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropSchema(transactionId, admin, context, new CatalogSchemaName("secret-catalog", "secret"));
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropSchema(transactionId, alice, context, new CatalogSchemaName("alice-catalog", "alice"));
        }));
    }

    @Test
    public void testSchemaRulesForCheckCanRenameSchema()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "file-based-system-access-schema.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanRenameSchema(transactionId, bob, context, new CatalogSchemaName("alice-catalog", "bob"), "some-schema");
                    accessControlManager.checkCanRenameSchema(transactionId, bob, context, new CatalogSchemaName("bob-catalog", "bob"), "some-schema");
                    accessControlManager.checkCanRenameSchema(transactionId, admin, context, new CatalogSchemaName("some-catalog", "bob"), "new-schema-name");
                    accessControlManager.checkCanRenameSchema(transactionId, admin, context, new CatalogSchemaName("some-catalog", "alice"), "new-schema-name");
                });

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRenameSchema(transactionId, bob, context, new CatalogSchemaName("alice-catalog", "alice"), "new-schema-name");
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRenameSchema(transactionId, bob, context, new CatalogSchemaName("bob-catalog", "alice"), "new-schema-name");
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRenameSchema(transactionId, bob, context, new CatalogSchemaName("secret-catalog", "secret"), "new-schema-name");
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRenameSchema(transactionId, alice, context, new CatalogSchemaName("secret-catalog", "secret"), "new-schema-name");
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRenameSchema(transactionId, admin, context, new CatalogSchemaName("secret-catalog", "secret"), "new-schema-name");
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRenameSchema(transactionId, alice, context, new CatalogSchemaName("alice-catalog", "alice"), "new-schema-name");
        }));
    }

    @Test
    public void testSchemaOperationsReadOnly()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_read_only.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    Set<String> aliceSchemas = ImmutableSet.of("schema");
                    assertEquals(accessControlManager.filterSchemas(transactionId, alice, context, "alice-catalog", aliceSchemas), aliceSchemas);
                    assertEquals(accessControlManager.filterSchemas(transactionId, bob, context, "alice-catalog", aliceSchemas), ImmutableSet.of());

                    accessControlManager.checkCanShowSchemas(transactionId, alice, context, "alice-catalog");
                });

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(transactionId, alice, context, aliceSchema);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropSchema(transactionId, alice, context, aliceSchema);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRenameSchema(transactionId, alice, context, aliceSchema, "new-schema");
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateSchema(transactionId, bob, context, aliceSchema);
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
                    assertEquals(accessControlManager.filterTables(transactionId, alice, context, "alice-catalog", aliceTables), aliceTables);
                    assertEquals(accessControlManager.filterTables(transactionId, bob, context, "alice-catalog", aliceTables), ImmutableSet.of());

                    accessControlManager.checkCanCreateTable(transactionId, alice, context, aliceTable);
                    accessControlManager.checkCanDropTable(transactionId, alice, context, aliceTable);
                    accessControlManager.checkCanSelectFromColumns(transactionId, alice, context, aliceTable, ImmutableSet.of());
                    accessControlManager.checkCanInsertIntoTable(transactionId, alice, context, aliceTable);
                    accessControlManager.checkCanDeleteFromTable(transactionId, alice, context, aliceTable);
                    accessControlManager.checkCanAddColumns(transactionId, alice, context, aliceTable);
                    accessControlManager.checkCanRenameColumn(transactionId, alice, context, aliceTable);
                });
        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateTable(transactionId, bob, context, aliceTable);
        }));
    }

    @Test
    public void testTableOperationsReadOnly()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_read_only.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    Set<SchemaTableName> aliceTables = ImmutableSet.of(new SchemaTableName("schema", "table"));
                    assertEquals(accessControlManager.filterTables(transactionId, alice, context, "alice-catalog", aliceTables), aliceTables);
                    assertEquals(accessControlManager.filterTables(transactionId, bob, context, "alice-catalog", aliceTables), ImmutableSet.of());

                    accessControlManager.checkCanSelectFromColumns(transactionId, alice, context, aliceTable, ImmutableSet.of());
                });

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateTable(transactionId, alice, context, aliceTable);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropTable(transactionId, alice, context, aliceTable);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanInsertIntoTable(transactionId, alice, context, aliceTable);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDeleteFromTable(transactionId, alice, context, aliceTable);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanAddColumns(transactionId, alice, context, aliceTable);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRenameColumn(transactionId, alice, context, aliceTable);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateTable(transactionId, bob, context, aliceTable);
        }));
    }

    @Test
    public void testViewOperations()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanCreateView(transactionId, alice, context, aliceView);
                    accessControlManager.checkCanDropView(transactionId, alice, context, aliceView);
                    accessControlManager.checkCanSelectFromColumns(transactionId, alice, context, aliceView, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(transactionId, alice, context, aliceTable, ImmutableSet.of());
                    accessControlManager.checkCanCreateViewWithSelectFromColumns(transactionId, alice, context, aliceView, ImmutableSet.of());
                    accessControlManager.checkCanSetCatalogSessionProperty(transactionId, alice, context, "alice-catalog", "property");
                    accessControlManager.checkCanGrantTablePrivilege(transactionId, alice, context, SELECT, aliceTable, new PrestoPrincipal(USER, "grantee"), true);
                    accessControlManager.checkCanRevokeTablePrivilege(transactionId, alice, context, SELECT, aliceTable, new PrestoPrincipal(USER, "revokee"), true);
                });
        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateView(transactionId, bob, context, aliceView);
        }));
    }

    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(SystemAccessControl.class, FileBasedSystemAccessControl.class);
    }

    @Test
    public void testViewOperationsReadOnly()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = newAccessControlManager(transactionManager, "catalog_read_only.json");

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanSelectFromColumns(transactionId, alice, context, aliceView, ImmutableSet.of());
                    accessControlManager.checkCanSetCatalogSessionProperty(transactionId, alice, context, "alice-catalog", "property");
                });

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateView(transactionId, alice, context, aliceView);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanDropView(transactionId, alice, context, aliceView);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateViewWithSelectFromColumns(transactionId, alice, context, aliceTable, ImmutableSet.of());
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateViewWithSelectFromColumns(transactionId, alice, context, aliceView, ImmutableSet.of());
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanGrantTablePrivilege(transactionId, alice, context, SELECT, aliceTable, new PrestoPrincipal(USER, "grantee"), true);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanRevokeTablePrivilege(transactionId, alice, context, SELECT, aliceTable, new PrestoPrincipal(USER, "revokee"), true);
        }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateView(transactionId, bob, context, aliceView);
        }));
    }

    @Test
    public void testRefreshing()
            throws Exception
    {
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControlManager = new AccessControlManager(transactionManager);
        File configFile = newTemporaryFile();
        configFile.deleteOnExit();
        copy(new File(getResourcePath("catalog.json")), configFile);

        accessControlManager.setSystemAccessControl(FileBasedSystemAccessControl.NAME, ImmutableMap.of(
                SECURITY_CONFIG_FILE, configFile.getAbsolutePath(),
                SECURITY_REFRESH_PERIOD, "1ms"));

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanCreateView(transactionId, alice, context, aliceView);
                    accessControlManager.checkCanCreateView(transactionId, alice, context, aliceView);
                    accessControlManager.checkCanCreateView(transactionId, alice, context, aliceView);
                });

        copy(new File(getResourcePath("security-config-file-with-unknown-rules.json")), configFile);
        sleep(2);

        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanCreateView(transactionId, alice, context, aliceView);
                }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Invalid JSON file");
        // test if file based cached control was not cached somewhere
        assertThatThrownBy(() -> transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanCreateView(transactionId, alice, context, aliceView);
                }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Invalid JSON file");

        copy(new File(getResourcePath("catalog.json")), configFile);
        sleep(2);

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanCreateView(transactionId, alice, context, aliceView);
                });
    }

    @Test
    public void testAllowModeIsRequired()
    {
        assertThrows(IllegalArgumentException.class, () -> newAccessControlManager(createTestTransactionManager(), "catalog_allow_unset.json"));
    }

    @Test
    public void testAllowModeInvalidValue()
    {
        assertThrows(IllegalArgumentException.class, () -> newAccessControlManager(createTestTransactionManager(), "catalog_invalid_allow_value.json"));
    }

    private AccessControlManager newAccessControlManager(TransactionManager transactionManager, String resourceName)
    {
        AccessControlManager accessControlManager = new AccessControlManager(transactionManager);

        accessControlManager.setSystemAccessControl(FileBasedSystemAccessControl.NAME, ImmutableMap.of("security.config-file", getResourcePath(resourceName)));

        return accessControlManager;
    }

    private String getResourcePath(String resourceName)
    {
        return this.getClass().getClassLoader().getResource(resourceName).getPath();
    }

    @Test
    public void parseUnknownRules()
    {
        assertThatThrownBy(() -> parse("src/test/resources/security-config-file-with-unknown-rules.json"))
                .hasMessageContaining("Invalid JSON");
    }

    private SystemAccessControl parse(String path)
    {
        return new FileBasedSystemAccessControl.Factory().create(ImmutableMap.of(SECURITY_CONFIG_FILE, path));
    }
}
