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
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.hive.metastore.HivePrivilege;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.ConnectorAccessControl;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.Set;

import static com.facebook.presto.hive.metastore.HivePrivilege.DELETE;
import static com.facebook.presto.hive.metastore.HivePrivilege.GRANT;
import static com.facebook.presto.hive.metastore.HivePrivilege.INSERT;
import static com.facebook.presto.hive.metastore.HivePrivilege.OWNERSHIP;
import static com.facebook.presto.hive.metastore.HivePrivilege.SELECT;
import static com.facebook.presto.hive.metastore.HivePrivilege.toHivePrivilege;
import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDeleteTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static com.facebook.presto.spi.security.AccessDeniedException.denyInsertTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectView;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static java.util.Objects.requireNonNull;

public class SqlStandardAccessControl
        implements ConnectorAccessControl
{
    private static final String ADMIN_ROLE_NAME = "admin";
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";

    private final String connectorId;
    private final HiveMetastore metastore;
    private final boolean allowDropTable;
    private final boolean allowRenameTable;

    @Inject
    public SqlStandardAccessControl(HiveConnectorId connectorId, HiveMetastore metastore, HiveClientConfig hiveClientConfig)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.metastore = requireNonNull(metastore, "metastore is null");

        requireNonNull(hiveClientConfig, "hiveClientConfig is null");
        allowDropTable = hiveClientConfig.getAllowDropTable();
        allowRenameTable = hiveClientConfig.getAllowRenameTable();
    }

    @Override
    public void checkCanCreateTable(Identity identity, SchemaTableName tableName)
    {
        if (!checkDatabasePermission(identity, tableName.getSchemaName(), OWNERSHIP)) {
            denyCreateTable(tableName.toString());
        }
    }

    @Override
    public void checkCanDropTable(Identity identity, SchemaTableName tableName)
    {
        if (!allowDropTable || !checkTablePermission(identity, tableName, OWNERSHIP)) {
            denyDropTable(tableName.toString());
        }
    }

    @Override
    public void checkCanRenameTable(Identity identity, SchemaTableName tableName, SchemaTableName newTableName)
    {
        if (!allowRenameTable || !checkTablePermission(identity, tableName, OWNERSHIP)) {
            denyRenameTable(tableName.toString(), newTableName.toString());
        }
    }

    @Override
    public void checkCanAddColumn(Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(identity, tableName, OWNERSHIP)) {
            denyAddColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanRenameColumn(Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(identity, tableName, OWNERSHIP)) {
            denyRenameColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanSelectFromTable(Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(identity, tableName, SELECT)) {
            denySelectTable(tableName.toString());
        }
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(identity, tableName, INSERT)) {
            denyInsertTable(tableName.toString());
        }
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(identity, tableName, DELETE)) {
            denyDeleteTable(tableName.toString());
        }
    }

    @Override
    public void checkCanCreateView(Identity identity, SchemaTableName viewName)
    {
        if (!checkDatabasePermission(identity, viewName.getSchemaName(), OWNERSHIP)) {
            denyCreateView(viewName.toString());
        }
    }

    @Override
    public void checkCanDropView(Identity identity, SchemaTableName viewName)
    {
        if (!checkTablePermission(identity, viewName, OWNERSHIP)) {
            denyDropView(viewName.toString());
        }
    }

    @Override
    public void checkCanSelectFromView(Identity identity, SchemaTableName viewName)
    {
        if (!checkTablePermission(identity, viewName, SELECT)) {
            denySelectView(viewName.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(identity, tableName, SELECT, GRANT)) {
            denySelectTable(tableName.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(Identity identity, SchemaTableName viewName)
    {
        if (!checkTablePermission(identity, viewName, SELECT, GRANT)) {
            denySelectView(viewName.toString());
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String propertyName)
    {
        if (!metastore.getRoles(identity.getUser()).contains(ADMIN_ROLE_NAME)) {
            denySetCatalogSessionProperty(connectorId, propertyName);
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(Identity identity, Privilege privilege, SchemaTableName tableName)
    {
        if (checkTablePermission(identity, tableName, OWNERSHIP)) {
            return;
        }

        HivePrivilege hivePrivilege = toHivePrivilege(privilege);
        if (hivePrivilege == null || !metastore.hasPrivilegeWithGrantOptionOnTable(identity.getUser(), tableName.getSchemaName(), tableName.getTableName(), hivePrivilege)) {
            denyGrantTablePrivilege(privilege.name(), tableName.toString());
        }
    }

    private boolean checkDatabasePermission(Identity identity, String schemaName, HivePrivilege... requiredPrivileges)
    {
        Set<HivePrivilege> privilegeSet = metastore.getDatabasePrivileges(identity.getUser(), schemaName);
        return privilegeSet.containsAll(ImmutableSet.copyOf(requiredPrivileges));
    }

    private boolean checkTablePermission(Identity identity, SchemaTableName tableName, HivePrivilege... requiredPrivileges)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return true;
        }

        Set<HivePrivilege> privilegeSet = metastore.getTablePrivileges(identity.getUser(), tableName.getSchemaName(), tableName.getTableName());
        return privilegeSet.containsAll(ImmutableSet.copyOf(requiredPrivileges));
    }
}
