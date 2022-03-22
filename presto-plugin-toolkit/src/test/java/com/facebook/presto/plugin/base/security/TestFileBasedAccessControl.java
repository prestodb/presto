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
package com.facebook.presto.plugin.base.security;

import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.google.common.collect.ImmutableSet;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertThrows;

public class TestFileBasedAccessControl
{
    public static final ConnectorTransactionHandle TRANSACTION_HANDLE = new ConnectorTransactionHandle() {};
    public static final AccessControlContext CONTEXT = new AccessControlContext(new QueryId("query_id"), Optional.empty(), Optional.empty());

    @Test
    public void testSchemaRules()
            throws IOException
    {
        ConnectorAccessControl accessControl = createAccessControl("schema.json");
        accessControl.checkCanCreateTable(TRANSACTION_HANDLE, user("admin"), CONTEXT, new SchemaTableName("test", "test"));
        accessControl.checkCanCreateTable(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("bob", "test"));
        assertDenied(() -> accessControl.checkCanCreateTable(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("test", "test")));
        assertDenied(() -> accessControl.checkCanCreateTable(TRANSACTION_HANDLE, user("admin"), CONTEXT, new SchemaTableName("secret", "test")));
    }

    @Test
    public void testTableRules()
            throws IOException
    {
        ConnectorAccessControl accessControl = createAccessControl("table.json");
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("alice"), CONTEXT, new SchemaTableName("test", "test"), ImmutableSet.of());
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("alice"), CONTEXT, new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of());
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("alice"), CONTEXT, new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of("bobcolumn"));
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of());
        accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("bobschema", "bobtable"));
        accessControl.checkCanDeleteFromTable(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("bobschema", "bobtable"));
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("joe"), CONTEXT, new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of());
        accessControl.checkCanCreateViewWithSelectFromColumns(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of());
        accessControl.checkCanDropTable(TRANSACTION_HANDLE, user("admin"), CONTEXT, new SchemaTableName("bobschema", "bobtable"));
        assertDenied(() -> accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("alice"), CONTEXT, new SchemaTableName("bobschema", "bobtable")));
        assertDenied(() -> accessControl.checkCanDropTable(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("bobschema", "bobtable")));
        assertDenied(() -> accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("bob"), CONTEXT, new SchemaTableName("test", "test")));
        assertDenied(() -> accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("admin"), CONTEXT, new SchemaTableName("secret", "secret"), ImmutableSet.of()));
        assertDenied(() -> accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, user("joe"), CONTEXT, new SchemaTableName("secret", "secret"), ImmutableSet.of()));
        assertDenied(() -> accessControl.checkCanCreateViewWithSelectFromColumns(TRANSACTION_HANDLE, user("joe"), CONTEXT, new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of()));
    }

    @Test
    public void testSessionPropertyRules()
            throws IOException
    {
        ConnectorAccessControl accessControl = createAccessControl("session_property.json");
        accessControl.checkCanSetCatalogSessionProperty(TRANSACTION_HANDLE, user("admin"), CONTEXT, "dangerous");
        accessControl.checkCanSetCatalogSessionProperty(TRANSACTION_HANDLE, user("alice"), CONTEXT, "safe");
        accessControl.checkCanSetCatalogSessionProperty(TRANSACTION_HANDLE, user("alice"), CONTEXT, "unsafe");
        accessControl.checkCanSetCatalogSessionProperty(TRANSACTION_HANDLE, user("bob"), CONTEXT, "safe");
        assertDenied(() -> accessControl.checkCanSetCatalogSessionProperty(TRANSACTION_HANDLE, user("bob"), CONTEXT, "unsafe"));
        assertDenied(() -> accessControl.checkCanSetCatalogSessionProperty(TRANSACTION_HANDLE, user("alice"), CONTEXT, "dangerous"));
        assertDenied(() -> accessControl.checkCanSetCatalogSessionProperty(TRANSACTION_HANDLE, user("charlie"), CONTEXT, "safe"));
    }

    @Test
    public void testInvalidRules()
    {
        assertThatThrownBy(() -> createAccessControl("invalid.json"))
                .hasMessageContaining("Invalid JSON");
    }

    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(ConnectorAccessControl.class, FileBasedAccessControl.class);
    }

    private static ConnectorIdentity user(String name)
    {
        return new ConnectorIdentity(name, Optional.empty(), Optional.empty());
    }

    private ConnectorAccessControl createAccessControl(String fileName)
            throws IOException
    {
        String path = this.getClass().getClassLoader().getResource(fileName).getPath();
        FileBasedAccessControlConfig config = new FileBasedAccessControlConfig();
        config.setConfigFile(path);
        return new FileBasedAccessControl(config);
    }

    private static void assertDenied(ThrowingRunnable runnable)
    {
        assertThrows(AccessDeniedException.class, runnable);
    }
}
