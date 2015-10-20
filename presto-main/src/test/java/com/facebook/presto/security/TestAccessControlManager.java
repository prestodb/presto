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

import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.ConnectorAccessControl;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.security.Principal;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.security.AccessControlManager.ALLOW_ALL_ACCESS_CONTROL;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectTable;
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
        AccessControlManager accessControlManager = new AccessControlManager();
        accessControlManager.checkCanSetUser(null, "foo");
    }

    @Test
    public void testNoneSystemAccessControl()
            throws Exception
    {
        AccessControlManager accessControlManager = new AccessControlManager();
        accessControlManager.setSystemAccessControl(ALLOW_ALL_ACCESS_CONTROL, ImmutableMap.<String, String>of());
        accessControlManager.checkCanSetUser(null, USER_NAME);
    }

    @Test
    public void testSetAccessControl()
            throws Exception
    {
        AccessControlManager accessControlManager = new AccessControlManager();

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
        AccessControlManager accessControlManager = new AccessControlManager();
        accessControlManager.setSystemAccessControl(ALLOW_ALL_ACCESS_CONTROL, ImmutableMap.<String, String>of());

        accessControlManager.checkCanSelectFromTable(new Identity(USER_NAME, Optional.of(PRINCIPAL)), new QualifiedTableName("catalog", "schema", "table"));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Access Denied: Cannot select from table schema.table")
    public void testDenyCatalogAccessControl()
            throws Exception
    {
        AccessControlManager accessControlManager = new AccessControlManager();
        accessControlManager.setSystemAccessControl(ALLOW_ALL_ACCESS_CONTROL, ImmutableMap.<String, String>of());

        accessControlManager.addCatalogAccessControl("catalog", new DenyConnectorAccessControl());
        accessControlManager.checkCanSelectFromTable(new Identity(USER_NAME, Optional.of(PRINCIPAL)), new QualifiedTableName("catalog", "schema", "table"));
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
            };
        }
    }

    private static class DenyConnectorAccessControl
            implements ConnectorAccessControl
    {
        @Override
        public void checkCanSelectFromTable(Identity identity, SchemaTableName tableName)
        {
            denySelectTable(tableName.toString());
        }

        @Override
        public void checkCanCreateTable(Identity identity, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanDropTable(Identity identity, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanRenameTable(Identity identity, SchemaTableName tableName, SchemaTableName newTableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanAddColumn(Identity identity, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanRenameColumn(Identity identity, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanInsertIntoTable(Identity identity, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanDeleteFromTable(Identity identity, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanCreateView(Identity identity, SchemaTableName viewName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanDropView(Identity identity, SchemaTableName viewName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanSelectFromView(Identity identity, SchemaTableName viewName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanCreateViewWithSelectFromTable(Identity identity, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanCreateViewWithSelectFromView(Identity identity, SchemaTableName viewName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkCanSetCatalogSessionProperty(Identity identity, String propertyName)
        {
            throw new UnsupportedOperationException();
        }
    }
}
