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
package com.facebook.presto.testing;

import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.spi.security.Identity;
import com.google.common.base.MoreObjects;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDeleteTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyInsertTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectView;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetSystemSessionProperty;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetUser;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_VIEW;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_VIEW;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SET_SESSION;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SET_USER;
import static java.util.Objects.requireNonNull;

public class TestingAccessControlManager
        extends AccessControlManager
{
    private final Set<TestingPrivilege> denyPrivileges = new HashSet<>();

    public static TestingPrivilege privilege(String name, TestingPrivilegeType type)
    {
        return new TestingPrivilege(name, type);
    }

    public void deny(TestingPrivilege... deniedPrivileges)
    {
        Collections.addAll(this.denyPrivileges, deniedPrivileges);
    }

    public void reset()
    {
        denyPrivileges.clear();
    }

    @Override
    public void checkCanSetUser(Principal principal, String userName)
    {
        if (shouldDenyPrivilege(userName, SET_USER)) {
            denySetUser(principal, userName);
        }
        super.checkCanSetUser(principal, userName);
    }

    @Override
    public void checkCanCreateTable(Identity identity, QualifiedTableName tableName)
    {
        if (shouldDenyPrivilege(tableName.getTableName(), CREATE_TABLE)) {
            denyCreateTable(tableName.toString());
        }
        super.checkCanCreateTable(identity, tableName);
    }

    @Override
    public void checkCanDropTable(Identity identity, QualifiedTableName tableName)
    {
        if (shouldDenyPrivilege(tableName.getTableName(), DROP_TABLE)) {
            denyDropTable(tableName.toString());
        }
        super.checkCanDropTable(identity, tableName);
    }

    @Override
    public void checkCanRenameTable(Identity identity, QualifiedTableName tableName, QualifiedTableName newTableName)
    {
        if (shouldDenyPrivilege(tableName.getTableName(), RENAME_TABLE)) {
            denyRenameTable(tableName.toString(), newTableName.toString());
        }
        super.checkCanRenameTable(identity, tableName, newTableName);
    }

    @Override
    public void checkCanRenameColumn(Identity identity, QualifiedTableName tableName)
    {
        if (shouldDenyPrivilege(tableName.getTableName(), RENAME_COLUMN)) {
            denyRenameColumn(tableName.toString());
        }
        super.checkCanRenameColumn(identity, tableName);
    }

    @Override
    public void checkCanSelectFromTable(Identity identity, QualifiedTableName tableName)
    {
        if (shouldDenyPrivilege(tableName.getTableName(), SELECT_TABLE)) {
            denySelectTable(tableName.toString());
        }
        super.checkCanSelectFromTable(identity, tableName);
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, QualifiedTableName tableName)
    {
        if (shouldDenyPrivilege(tableName.getTableName(), INSERT_TABLE)) {
            denyInsertTable(tableName.toString());
        }
        super.checkCanInsertIntoTable(identity, tableName);
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, QualifiedTableName tableName)
    {
        if (shouldDenyPrivilege(tableName.getTableName(), DELETE_TABLE)) {
            denyDeleteTable(tableName.toString());
        }
        super.checkCanDeleteFromTable(identity, tableName);
    }

    @Override
    public void checkCanCreateView(Identity identity, QualifiedTableName viewName)
    {
        if (shouldDenyPrivilege(viewName.getTableName(), CREATE_VIEW)) {
            denyCreateView(viewName.toString());
        }
        super.checkCanCreateView(identity, viewName);
    }

    @Override
    public void checkCanDropView(Identity identity, QualifiedTableName viewName)
    {
        if (shouldDenyPrivilege(viewName.getTableName(), DROP_VIEW)) {
            denyDropView(viewName.toString());
        }
        super.checkCanDropView(identity, viewName);
    }

    @Override
    public void checkCanSelectFromView(Identity identity, QualifiedTableName viewName)
    {
        if (shouldDenyPrivilege(viewName.getTableName(), SELECT_VIEW)) {
            denySelectView(viewName.toString());
        }
        super.checkCanSelectFromView(identity, viewName);
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        if (shouldDenyPrivilege(propertyName, SET_SESSION)) {
            denySetSystemSessionProperty(propertyName);
        }
        super.checkCanSetSystemSessionProperty(identity, propertyName);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
        if (shouldDenyPrivilege(catalogName + "." + propertyName, SET_SESSION)) {
            denySetCatalogSessionProperty(catalogName, propertyName);
        }
        super.checkCanSetCatalogSessionProperty(identity, catalogName, propertyName);
    }

    private boolean shouldDenyPrivilege(String entityName, TestingPrivilegeType type)
    {
        return denyPrivileges.contains(privilege(entityName, type));
    }

    public enum TestingPrivilegeType
    {
        SET_USER,
        CREATE_TABLE, DROP_TABLE, RENAME_TABLE, RENAME_COLUMN, SELECT_TABLE, INSERT_TABLE, DELETE_TABLE,
        CREATE_VIEW, DROP_VIEW, SELECT_VIEW,
        SET_SESSION
    }

    public static class TestingPrivilege
    {
        private final String name;
        private final TestingPrivilegeType type;

        private TestingPrivilege(String name, TestingPrivilegeType type)
        {
            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestingPrivilege that = (TestingPrivilege) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(type, that.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, type);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("name", name)
                    .add("type", type)
                    .toString();
        }
    }
}
