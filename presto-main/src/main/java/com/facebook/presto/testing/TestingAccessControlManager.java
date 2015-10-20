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
import com.google.common.collect.ImmutableMap;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
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
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.ADD_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW_WITH_SELECT_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW_WITH_SELECT_VIEW;
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

    public TestingAccessControlManager()
    {
        setSystemAccessControl(ALLOW_ALL_ACCESS_CONTROL, ImmutableMap.of());
    }

    public static TestingPrivilege privilege(String entityName, TestingPrivilegeType type)
    {
        return new TestingPrivilege(Optional.empty(), entityName, type);
    }

    public static TestingPrivilege privilege(String userName, String entityName, TestingPrivilegeType type)
    {
        return new TestingPrivilege(Optional.of(userName), entityName, type);
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
        if (shouldDenyPrivilege(userName, userName, SET_USER)) {
            denySetUser(principal, userName);
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanSetUser(principal, userName);
        }
    }

    @Override
    public void checkCanCreateTable(Identity identity, QualifiedTableName tableName)
    {
        if (shouldDenyPrivilege(identity.getUser(), tableName.getTableName(), CREATE_TABLE)) {
            denyCreateTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanCreateTable(identity, tableName);
        }
    }

    @Override
    public void checkCanDropTable(Identity identity, QualifiedTableName tableName)
    {
        if (shouldDenyPrivilege(identity.getUser(), tableName.getTableName(), DROP_TABLE)) {
            denyDropTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanDropTable(identity, tableName);
        }
    }

    @Override
    public void checkCanRenameTable(Identity identity, QualifiedTableName tableName, QualifiedTableName newTableName)
    {
        if (shouldDenyPrivilege(identity.getUser(), tableName.getTableName(), RENAME_TABLE)) {
            denyRenameTable(tableName.toString(), newTableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanRenameTable(identity, tableName, newTableName);
        }
    }

    @Override
    public void checkCanAddColumns(Identity identity, QualifiedTableName tableName)
    {
        if (shouldDenyPrivilege(identity.getUser(), tableName.getTableName(), ADD_COLUMN)) {
            denyAddColumn(tableName.toString());
        }
        super.checkCanAddColumns(identity, tableName);
    }

    @Override
    public void checkCanRenameColumn(Identity identity, QualifiedTableName tableName)
    {
        if (shouldDenyPrivilege(identity.getUser(), tableName.getTableName(), RENAME_COLUMN)) {
            denyRenameColumn(tableName.toString());
        }
        super.checkCanRenameColumn(identity, tableName);
    }

    @Override
    public void checkCanSelectFromTable(Identity identity, QualifiedTableName tableName)
    {
        if (shouldDenyPrivilege(identity.getUser(), tableName.getTableName(), SELECT_TABLE)) {
            denySelectTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanSelectFromTable(identity, tableName);
        }
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, QualifiedTableName tableName)
    {
        if (shouldDenyPrivilege(identity.getUser(), tableName.getTableName(), INSERT_TABLE)) {
            denyInsertTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanInsertIntoTable(identity, tableName);
        }
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, QualifiedTableName tableName)
    {
        if (shouldDenyPrivilege(identity.getUser(), tableName.getTableName(), DELETE_TABLE)) {
            denyDeleteTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanDeleteFromTable(identity, tableName);
        }
    }

    @Override
    public void checkCanCreateView(Identity identity, QualifiedTableName viewName)
    {
        if (shouldDenyPrivilege(identity.getUser(), viewName.getTableName(), CREATE_VIEW)) {
            denyCreateView(viewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanCreateView(identity, viewName);
        }
    }

    @Override
    public void checkCanDropView(Identity identity, QualifiedTableName viewName)
    {
        if (shouldDenyPrivilege(identity.getUser(), viewName.getTableName(), DROP_VIEW)) {
            denyDropView(viewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanDropView(identity, viewName);
        }
    }

    @Override
    public void checkCanSelectFromView(Identity identity, QualifiedTableName viewName)
    {
        if (shouldDenyPrivilege(identity.getUser(), viewName.getTableName(), SELECT_VIEW)) {
            denySelectView(viewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanSelectFromView(identity, viewName);
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        if (shouldDenyPrivilege(identity.getUser(), propertyName, SET_SESSION)) {
            denySetSystemSessionProperty(propertyName);
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanSetSystemSessionProperty(identity, propertyName);
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(Identity identity, QualifiedTableName tableName)
    {
        if (shouldDenyPrivilege(identity.getUser(), tableName.getTableName(), CREATE_VIEW_WITH_SELECT_TABLE)) {
            denySelectTable(tableName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanCreateViewWithSelectFromTable(identity, tableName);
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(Identity identity, QualifiedTableName viewName)
    {
        if (shouldDenyPrivilege(identity.getUser(), viewName.getTableName(), CREATE_VIEW_WITH_SELECT_VIEW)) {
            denySelectView(viewName.toString());
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanCreateViewWithSelectFromView(identity, viewName);
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
        if (shouldDenyPrivilege(identity.getUser(), catalogName + "." + propertyName, SET_SESSION)) {
            denySetCatalogSessionProperty(catalogName, propertyName);
        }
        if (denyPrivileges.isEmpty()) {
            super.checkCanSetCatalogSessionProperty(identity, catalogName, propertyName);
        }
    }

    private boolean shouldDenyPrivilege(String userName, String entityName, TestingPrivilegeType type)
    {
        TestingPrivilege testPrivilege = privilege(userName, entityName, type);
        for (TestingPrivilege denyPrivilege : denyPrivileges) {
            if (denyPrivilege.matches(testPrivilege)) {
                return true;
            }
        }
        return false;
    }

    public enum TestingPrivilegeType
    {
        SET_USER,
        CREATE_TABLE, DROP_TABLE, RENAME_TABLE, SELECT_TABLE, INSERT_TABLE, DELETE_TABLE,
        ADD_COLUMN, RENAME_COLUMN,
        CREATE_VIEW, DROP_VIEW, SELECT_VIEW,
        CREATE_VIEW_WITH_SELECT_TABLE, CREATE_VIEW_WITH_SELECT_VIEW,
        SET_SESSION
    }

    public static class TestingPrivilege
    {
        private final Optional<String> userName;
        private final String entityName;
        private final TestingPrivilegeType type;

        private TestingPrivilege(Optional<String> userName, String entityName, TestingPrivilegeType type)
        {
            this.userName = requireNonNull(userName, "userName is null");
            this.entityName = requireNonNull(entityName, "entityName is null");
            this.type = requireNonNull(type, "type is null");
        }

        public boolean matches(TestingPrivilege testPrivilege)
        {
            return userName.map(name -> testPrivilege.userName.get().equals(name)).orElse(true) &&
                    entityName.equals(testPrivilege.entityName) &&
                    type == testPrivilege.type;
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
            return Objects.equals(entityName, that.entityName) &&
                    Objects.equals(type, that.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(entityName, type);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("userName", userName)
                    .add("entityName", entityName)
                    .add("type", type)
                    .toString();
        }
    }
}
