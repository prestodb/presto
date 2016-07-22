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

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.fail;

public class TestFileBasedAccessControl
{
    public static final ConnectorTransactionHandle TRANSACTION_HANDLE = new ConnectorTransactionHandle() { };

    @Test
    public void testSchemaRules()
            throws IOException
    {
        ConnectorAccessControl accessControl = createAccessControl("schema.json");
        accessControl.checkCanCreateTable(TRANSACTION_HANDLE, user("admin"), SchemaTableName.valueOf("test.test"));
        accessControl.checkCanCreateTable(TRANSACTION_HANDLE, user("bob"), SchemaTableName.valueOf("bob.test"));
        try {
            accessControl.checkCanCreateTable(TRANSACTION_HANDLE, user("bob"), SchemaTableName.valueOf("test.test"));
            fail();
        }
        catch (AccessDeniedException e) {
            // expected
        }
        try {
            accessControl.checkCanCreateTable(TRANSACTION_HANDLE, user("admin"), SchemaTableName.valueOf("secret.test"));
            fail();
        }
        catch (AccessDeniedException e) {
            // expected
        }
    }

    @Test
    public void testTableRules()
            throws IOException
    {
        ConnectorAccessControl accessControl = createAccessControl("table.json");
        accessControl.checkCanSelectFromTable(TRANSACTION_HANDLE, user("alice"), SchemaTableName.valueOf("test.test"));
        accessControl.checkCanSelectFromTable(TRANSACTION_HANDLE, user("alice"), SchemaTableName.valueOf("bobschema.bobtable"));
        accessControl.checkCanSelectFromTable(TRANSACTION_HANDLE, user("bob"), SchemaTableName.valueOf("bobschema.bobtable"));
        accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("bob"), SchemaTableName.valueOf("bobschema.bobtable"));
        accessControl.checkCanDeleteFromTable(TRANSACTION_HANDLE, user("bob"), SchemaTableName.valueOf("bobschema.bobtable"));
        accessControl.checkCanCreateViewWithSelectFromTable(TRANSACTION_HANDLE, user("bob"), SchemaTableName.valueOf("bobschema.bobtable"));
        accessControl.checkCanDropTable(TRANSACTION_HANDLE, user("admin"), SchemaTableName.valueOf("bobschema.bobtable"));
        try {
            accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("alice"), SchemaTableName.valueOf("bobschema.bobtable"));
            fail();
        }
        catch (AccessDeniedException e) {
            // expected
        }
        try {
            accessControl.checkCanDropTable(TRANSACTION_HANDLE, user("bob"), SchemaTableName.valueOf("bobschema.bobtable"));
            fail();
        }
        catch (AccessDeniedException e) {
            // expected
        }
        try {
            accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, user("bob"), SchemaTableName.valueOf("test.test"));
            fail();
        }
        catch (AccessDeniedException e) {
            // expected
        }
        try {
            accessControl.checkCanSelectFromTable(TRANSACTION_HANDLE, user("admin"), SchemaTableName.valueOf("secret.secret"));
            fail();
        }
        catch (AccessDeniedException e) {
            // expected
        }
    }

    @Test
    public void testSessionPropertyRules()
            throws IOException
    {
        ConnectorAccessControl accessControl = createAccessControl("session_property.json");
        accessControl.checkCanSetCatalogSessionProperty(user("admin"), "dangerous");
        accessControl.checkCanSetCatalogSessionProperty(user("alice"), "safe");
        accessControl.checkCanSetCatalogSessionProperty(user("alice"), "unsafe");
        accessControl.checkCanSetCatalogSessionProperty(user("bob"), "safe");
        try {
            accessControl.checkCanSetCatalogSessionProperty(user("bob"), "unsafe");
            fail();
        }
        catch (AccessDeniedException e) {
            // expected
        }
        try {
            accessControl.checkCanSetCatalogSessionProperty(user("alice"), "dangerous");
            fail();
        }
        catch (AccessDeniedException e) {
            // expected
        }
        try {
            accessControl.checkCanSetCatalogSessionProperty(user("charlie"), "safe");
            fail();
        }
        catch (AccessDeniedException e) {
            // expected
        }
    }

    private static Identity user(String name)
    {
        return new Identity(name, Optional.empty());
    }

    private ConnectorAccessControl createAccessControl(String fileName)
            throws IOException
    {
        String path = this.getClass().getClassLoader().getResource(fileName).getPath();
        FileBasedAccessControlConfig config = new FileBasedAccessControlConfig();
        config.setConfigFile(path);
        return new FileBasedAccessControl(config, jsonCodec(AccessControlRules.class));
    }
}
