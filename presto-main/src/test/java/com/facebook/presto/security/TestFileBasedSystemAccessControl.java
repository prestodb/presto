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
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.sun.security.auth.LdapPrincipal;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.security.Principal;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.facebook.presto.spi.security.Privilege.SELECT;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.facebook.presto.transaction.TransactionManager.createTestTransactionManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestFileBasedSystemAccessControl
{
    private static final String JAVA_KDC_PROPERTY = "java.security.krb5.kdc";
    private static final String JAVA_REALM_PROPERTY = "java.security.krb5.realm";
    private static final String DEFAULT_CONFIG = "catalog.json";
    private static final Principal TESTING_PRINCIPAL = new TestingPrincipal("principal");
    private static final Identity alice = new Identity("alice", Optional.of(TESTING_PRINCIPAL));
    private static final Identity bob = new Identity("bob", Optional.of(TESTING_PRINCIPAL));
    private static final Identity admin = new Identity("admin", Optional.of(TESTING_PRINCIPAL));
    private static final Identity nonAsciiUser = new Identity("\u0194\u0194\u0194", Optional.of(TESTING_PRINCIPAL));
    private static final Set<String> allCatalogs = ImmutableSet.of("secret", "open-to-all", "all-allowed", "alice-catalog", "allowed-absent", "\u0200\u0200\u0200");
    private static final QualifiedObjectName aliceTable = new QualifiedObjectName("alice-catalog", "schema", "table");
    private static final QualifiedObjectName aliceView = new QualifiedObjectName("alice-catalog", "schema", "view");
    private static final CatalogSchemaName aliceSchema = new CatalogSchemaName("alice-catalog", "schema");
    private TransactionManager transactionManager;
    private Principal ldapPrincipal;
    private Principal kerberosHostRealm;
    private Principal kerberosIpRealm;
    private Principal kerberosRealm;
    private Principal kerberosHost;
    private Principal kerberosSimple;
    private Properties oldSessionProperties;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        oldSessionProperties = setKerberosSessionProperties();
        ldapPrincipal = new LdapPrincipal("CN=principal");
        kerberosHostRealm = new KerberosPrincipal("principal/hostname@REALM");
        kerberosIpRealm = new KerberosPrincipal("principal/192.1.1.1@REALM");
        kerberosRealm = new KerberosPrincipal("principal@REALM");
        kerberosHost = new KerberosPrincipal("principal/hostname");
        kerberosSimple = new KerberosPrincipal("principal");

        transactionManager = createTestTransactionManager();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (oldSessionProperties != null) {
            for (Map.Entry<Object, Object> property : oldSessionProperties.entrySet()) {
                System.setProperty((String) property.getKey(), (String) property.getValue());
            }
        }
    }

    private Properties setKerberosSessionProperties()
    {
        Properties properties = new Properties();
        String oldKdc = System.getProperty(JAVA_KDC_PROPERTY);
        System.setProperty(JAVA_KDC_PROPERTY, "localhost");
        if (oldKdc != null) {
            properties.setProperty(JAVA_KDC_PROPERTY, oldKdc);
        }

        String oldRealm = System.getProperty(JAVA_REALM_PROPERTY);
        System.setProperty(JAVA_REALM_PROPERTY, "REALM");
        if (oldRealm != null) {
            properties.setProperty(JAVA_REALM_PROPERTY, oldRealm);
        }

        return properties;
    }

    @Test
    public void testCatalogOperations()
    {
       AccessControlManager accessControlManager = newAccessControlManager();

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
        AccessControlManager accessControlManager = newAccessControlManager();

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
        AccessControlManager accessControlManager = newAccessControlManager();

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    Set<SchemaTableName> aliceTables = ImmutableSet.of(new SchemaTableName("schema", "table"));
                    assertEquals(accessControlManager.filterTables(transactionId, alice, "alice-catalog", aliceTables), aliceTables);
                    assertEquals(accessControlManager.filterTables(transactionId, bob, "alice-catalog", aliceTables), ImmutableSet.of());

                    accessControlManager.checkCanCreateTable(transactionId, alice, aliceTable);
                    accessControlManager.checkCanDropTable(transactionId, alice, aliceTable);
                    accessControlManager.checkCanSelectFromTable(transactionId, alice, aliceTable);
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
            throws Exception
    {
        AccessControlManager accessControlManager = newAccessControlManager();

        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanCreateView(transactionId, alice, aliceView);
                    accessControlManager.checkCanDropView(transactionId, alice, aliceView);
                    accessControlManager.checkCanSelectFromView(transactionId, alice, aliceView);
                    accessControlManager.checkCanCreateViewWithSelectFromTable(transactionId, alice, aliceTable);
                    accessControlManager.checkCanCreateViewWithSelectFromView(transactionId, alice, aliceView);
                    accessControlManager.checkCanSetCatalogSessionProperty(transactionId, alice, "alice-catalog", "property");
                    accessControlManager.checkCanGrantTablePrivilege(transactionId, alice, SELECT, aliceTable, new PrestoPrincipal(USER, "grantee"), true);
                    accessControlManager.checkCanRevokeTablePrivilege(transactionId, alice, SELECT, aliceTable, new PrestoPrincipal(USER, "revokee"), true);
                });
        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
            accessControlManager.checkCanCreateView(transactionId, bob, aliceView);
        }));
    }

    @Test
    public void testKerberosPrincipalNoExactMatchSpecified()
    {
        AccessControlManager accessControlManager = newAccessControlManager();
        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    checkCanSetWithAllPrincipals(accessControlManager, "alice");
                    checkCanSetWithAllPrincipals(accessControlManager, "principal");
                });
    }

    @Test
    public void testKerberosPrincipalExactMatchFalse()
    {
        AccessControlManager accessControlManager = newAccessControlManager("kerbExactMatchFalse.json");
        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    checkCanSetWithAllPrincipals(accessControlManager, "alice");
                    checkCanSetWithAllPrincipals(accessControlManager, "principal");
                });
    }

    @Test
    public void testKerberosPrincipalExactMatchTrue()
    {
        AccessControlManager accessControlManager = newAccessControlManager("kerbExactMatchTrue.json");
        transaction(transactionManager, accessControlManager)
                .execute(transactionId -> {
                    accessControlManager.checkCanSetUser(kerberosRealm, "principal");
                    accessControlManager.checkCanSetUser(kerberosSimple, "principal");
                    accessControlManager.checkCanSetUser(null, "principal");
                    accessControlManager.checkCanSetUser(ldapPrincipal, "principal");
                    accessControlManager.checkCanSetUser(null, "alice");
                    accessControlManager.checkCanSetUser(ldapPrincipal, "alice");
                });

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
                    accessControlManager.checkCanSetUser(kerberosHostRealm, "principal");
                }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
                    accessControlManager.checkCanSetUser(kerberosIpRealm, "principal");
                }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
                    accessControlManager.checkCanSetUser(kerberosHost, "principal");
                }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
                    accessControlManager.checkCanSetUser(kerberosHostRealm, "alice");
                }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
                    accessControlManager.checkCanSetUser(kerberosIpRealm, "alice");
                }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
                    accessControlManager.checkCanSetUser(kerberosRealm, "alice");
                }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
                    accessControlManager.checkCanSetUser(kerberosHost, "alice");
                }));

        assertThrows(AccessDeniedException.class, () -> transaction(transactionManager, accessControlManager).execute(transactionId -> {
                    accessControlManager.checkCanSetUser(kerberosSimple, "alice");
                }));
    }

    private void checkCanSetWithAllPrincipals(AccessControlManager accessControlManager, String user)
    {
        accessControlManager.checkCanSetUser(kerberosHostRealm, user);
        accessControlManager.checkCanSetUser(kerberosIpRealm, user);
        accessControlManager.checkCanSetUser(kerberosRealm, user);
        accessControlManager.checkCanSetUser(kerberosHost, user);
        accessControlManager.checkCanSetUser(kerberosSimple, user);
        accessControlManager.checkCanSetUser(null, user);
        accessControlManager.checkCanSetUser(ldapPrincipal, user);
    }

    private AccessControlManager newAccessControlManager()
    {
        return newAccessControlManager(DEFAULT_CONFIG);
    }

    private AccessControlManager newAccessControlManager(String configFile)
    {
        AccessControlManager accessControlManager =  new AccessControlManager(transactionManager);

        String path = this.getClass().getClassLoader().getResource(configFile).getPath();
        accessControlManager.setSystemAccessControl(FileBasedSystemAccessControl.NAME, ImmutableMap.of(
                "security.config-file", path));

        return accessControlManager;
    }
}
