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
package com.facebook.presto.hive.security;

import com.facebook.presto.hive.HiveConnectorId;
import com.facebook.presto.hive.HiveTransactionHandle;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.GrantInfo;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.DELETE;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.INSERT;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.SELECT;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.toHivePrivilege;
import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDeleteTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static com.facebook.presto.spi.security.AccessDeniedException.denyInsertTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectView;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static java.util.Objects.requireNonNull;

public class SqlStandardAccessControl
        implements ConnectorAccessControl
{
    public static final String ADMIN_ROLE_NAME = "admin";
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";

    private final String connectorId;
    // Two metastores (one transaction-aware, the other not) are available in this class
    // so that an appropriate one can be chosen based on whether transaction handle is available.
    // Transaction handle is not available for checkCanSetCatalogSessionProperty.
    private final Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider;
    private final ExtendedHiveMetastore metastore;

    @Inject
    public SqlStandardAccessControl(
            HiveConnectorId connectorId,
            Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider,
            ExtendedHiveMetastore metastore)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.metastoreProvider = requireNonNull(metastoreProvider, "metastoreProvider is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    public void checkCanCreateSchema(ConnectorTransactionHandle transaction, Identity identity, String schemaName)
    {
        if (!isAdmin(transaction, identity)) {
            denyCreateSchema(schemaName);
        }
    }

    @Override
    public void checkCanDropSchema(ConnectorTransactionHandle transaction, Identity identity, String schemaName)
    {
        if (!isDatabaseOwner(transaction, identity, schemaName)) {
            denyDropSchema(schemaName);
        }
    }

    @Override
    public void checkCanRenameSchema(ConnectorTransactionHandle transaction, Identity identity, String schemaName, String newSchemaName)
    {
        if (!isAdmin(transaction, identity) || !isDatabaseOwner(transaction, identity, schemaName)) {
            denyRenameSchema(schemaName, newSchemaName);
        }
    }

    @Override
    public void checkCanShowSchemas(ConnectorTransactionHandle transactionHandle, Identity identity)
    {
    }

    @Override
    public Set<String> filterSchemas(ConnectorTransactionHandle transactionHandle, Identity identity, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!isDatabaseOwner(transaction, identity, tableName.getSchemaName())) {
            denyCreateTable(tableName.toString());
        }
    }

    @Override
    public void checkCanDropTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(transaction, identity, tableName, OWNERSHIP)) {
            denyDropTable(tableName.toString());
        }
    }

    @Override
    public void checkCanRenameTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName, SchemaTableName newTableName)
    {
        if (!checkTablePermission(transaction, identity, tableName, OWNERSHIP)) {
            denyRenameTable(tableName.toString(), newTableName.toString());
        }
    }

    @Override
    public void checkCanShowTables(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorTransactionHandle transactionHandle, Identity identity, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanAddColumn(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(transaction, identity, tableName, OWNERSHIP)) {
            denyAddColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanRenameColumn(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(transaction, identity, tableName, OWNERSHIP)) {
            denyRenameColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanSelectFromTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(transaction, identity, tableName, SELECT)) {
            denySelectTable(tableName.toString());
        }
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(transaction, identity, tableName, INSERT)) {
            denyInsertTable(tableName.toString());
        }
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(transaction, identity, tableName, DELETE)) {
            denyDeleteTable(tableName.toString());
        }
    }

    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
        if (!isDatabaseOwner(transaction, identity, viewName.getSchemaName())) {
            denyCreateView(viewName.toString());
        }
    }

    @Override
    public void checkCanDropView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
        if (!checkTablePermission(transaction, identity, viewName, OWNERSHIP)) {
            denyDropView(viewName.toString());
        }
    }

    @Override
    public void checkCanSelectFromView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
        if (!checkTablePermission(transaction, identity, viewName, SELECT)) {
            denySelectView(viewName.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(transaction, identity, tableName, SELECT)) {
            denySelectTable(tableName.toString());
        }
        else if (!getGrantOptionForPrivilege(transaction, identity, Privilege.SELECT, tableName)) {
            denyCreateViewWithSelect(tableName.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
        if (!checkTablePermission(transaction, identity, viewName, SELECT)) {
            denySelectView(viewName.toString());
        }
        if (!getGrantOptionForPrivilege(transaction, identity, Privilege.SELECT, viewName)) {
            denyCreateViewWithSelect(viewName.toString());
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String propertyName)
    {
        // TODO: when this is updated to have a transaction, use isAdmin()
        if (!metastore.getRoles(identity.getUser()).contains(ADMIN_ROLE_NAME)) {
            denySetCatalogSessionProperty(connectorId, propertyName);
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transaction, Identity identity, Privilege privilege, SchemaTableName tableName)
    {
        if (checkTablePermission(transaction, identity, tableName, OWNERSHIP)) {
            return;
        }

        HivePrivilege hivePrivilege = toHivePrivilege(privilege);
        if (hivePrivilege == null || !getGrantOptionForPrivilege(transaction, identity, privilege, tableName)) {
            denyGrantTablePrivilege(privilege.name(), tableName.toString());
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transaction, Identity identity, Privilege privilege, SchemaTableName tableName)
    {
        if (checkTablePermission(transaction, identity, tableName, OWNERSHIP)) {
            return;
        }

        HivePrivilege hivePrivilege = toHivePrivilege(privilege);
        if (hivePrivilege == null || !getGrantOptionForPrivilege(transaction, identity, privilege, tableName)) {
            denyRevokeTablePrivilege(privilege.name(), tableName.toString());
        }
    }

    @Override
    public void checkCanShowGrants(ConnectorTransactionHandle transaction, Identity identity, String catalogName, SchemaTablePrefix schemaTablePrefix)
    {
    }

    @Override
    public Set<GrantInfo> filterGrants(ConnectorTransactionHandle transaction, Identity identity, String catalogName, SchemaTablePrefix schemaTablePrefix, Set<GrantInfo> grantInfos)
    {
        return grantInfos;
    }

    private boolean checkDatabasePermission(ConnectorTransactionHandle transaction, Identity identity, String schemaName, HivePrivilege... requiredPrivileges)
    {
        SemiTransactionalHiveMetastore metastore = metastoreProvider.apply(((HiveTransactionHandle) transaction));
        Set<HivePrivilege> privilegeSet = metastore.getDatabasePrivileges(identity.getUser(), schemaName).stream()
                .map(HivePrivilegeInfo::getHivePrivilege)
                .collect(Collectors.toSet());

        return privilegeSet.containsAll(ImmutableSet.copyOf(requiredPrivileges));
    }

    private boolean isDatabaseOwner(ConnectorTransactionHandle transaction, Identity identity, String schemaName)
    {
        return checkDatabasePermission(transaction, identity, schemaName, OWNERSHIP);
    }

    private boolean getGrantOptionForPrivilege(ConnectorTransactionHandle transaction, Identity identity, Privilege privilege, SchemaTableName tableName)
    {
        SemiTransactionalHiveMetastore metastore = metastoreProvider.apply(((HiveTransactionHandle) transaction));
        return metastore.getTablePrivileges(identity.getUser(), tableName.getSchemaName(), tableName.getTableName())
                .contains(new HivePrivilegeInfo(toHivePrivilege(privilege), true));
    }

    private boolean checkTablePermission(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName, HivePrivilege... requiredPrivileges)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return true;
        }

        SemiTransactionalHiveMetastore metastore = metastoreProvider.apply(((HiveTransactionHandle) transaction));
        Set<HivePrivilege> privilegeSet = metastore.getTablePrivileges(identity.getUser(), tableName.getSchemaName(), tableName.getTableName()).stream()
                .map(HivePrivilegeInfo::getHivePrivilege)
                .collect(Collectors.toSet());
        return privilegeSet.containsAll(ImmutableSet.copyOf(requiredPrivileges));
    }

    private boolean isAdmin(ConnectorTransactionHandle transaction, Identity identity)
    {
        SemiTransactionalHiveMetastore metastore = metastoreProvider.apply(((HiveTransactionHandle) transaction));
        return metastore.getRoles(identity.getUser()).contains(ADMIN_ROLE_NAME);
    }
}
