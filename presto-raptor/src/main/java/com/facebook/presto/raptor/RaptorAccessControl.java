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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.RaptorGrantInfo;
import com.facebook.presto.raptor.metadata.RaptorPrivilegeInfo;
import com.facebook.presto.raptor.metadata.RaptorPrivilegeInfo.RaptorPrivilege;
import com.facebook.presto.raptor.security.IdentityManager;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.PrivilegeInfo;
import com.google.inject.Inject;
import org.skife.jdbi.v2.IDBI;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.raptor.metadata.RaptorPrivilegeInfo.RaptorPrivilege.DELETE;
import static com.facebook.presto.raptor.metadata.RaptorPrivilegeInfo.RaptorPrivilege.INSERT;
import static com.facebook.presto.raptor.metadata.RaptorPrivilegeInfo.RaptorPrivilege.OWNERSHIP;
import static com.facebook.presto.raptor.metadata.RaptorPrivilegeInfo.RaptorPrivilege.SELECT;
import static com.facebook.presto.raptor.metadata.RaptorPrivilegeInfo.toRaptorPrivilege;
import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDeleteTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static com.facebook.presto.spi.security.AccessDeniedException.denyInsertTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static java.util.Objects.requireNonNull;

public class RaptorAccessControl
        implements ConnectorAccessControl
{
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";

    private final MetadataDao dao;
    private final IdentityManager identityManager;
    private final String connectorId;

    @Inject
    public RaptorAccessControl(@ForMetadata IDBI dbi, IdentityManager identityManager, RaptorConnectorId connectorId)
    {
        this.dao = onDemandDao(dbi, MetadataDao.class);
        this.identityManager = requireNonNull(identityManager, "identityManager is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    }

    public void checkCanCreateSchema(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName)
    {
        if (!isAdmin(transactionHandle, identity)) {
            denyCreateSchema(schemaName);
        }
    }

    public void checkCanDropSchema(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName)
    {
        if (!isSchemaOwner(transactionHandle, identity, schemaName)) {
            denyDropSchema(schemaName);
        }
    }

    public void checkCanRenameSchema(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName, String newSchemaName)
    {
        if (!isAdmin(transactionHandle, identity) || !isSchemaOwner(transactionHandle, identity, schemaName)) {
            denyRenameSchema(schemaName, newSchemaName);
        }
    }

    public void checkCanShowSchemas(ConnectorTransactionHandle transactionHandle, Identity identity)
    {
    }

    public Set<String> filterSchemas(ConnectorTransactionHandle transactionHandle, Identity identity, Set<String> schemaNames)
    {
        return schemaNames;
    }

    public void checkCanCreateTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
        if (!isSchemaOwner(transactionHandle, identity, tableName.getSchemaName())) {
            denyCreateTable(tableName.toString());
        }
    }

    public void checkCanDropTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(transactionHandle, identity, tableName, OWNERSHIP)) {
            denyDropTable(tableName.toString());
        }
    }

    public void checkCanRenameTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName, SchemaTableName newTableName)
    {
        if (!checkTablePermission(transactionHandle, identity, tableName, OWNERSHIP)) {
            denyRenameTable(tableName.toString(), newTableName.toString());
        }
    }

    public void checkCanShowTablesMetadata(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName)
    {
    }

    public Set<SchemaTableName> filterTables(ConnectorTransactionHandle transactionHandle, Identity identity, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    public void checkCanAddColumn(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(transactionHandle, identity, tableName, OWNERSHIP)) {
            denyAddColumn(tableName.toString());
        }
    }

    public void checkCanRenameColumn(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(transactionHandle, identity, tableName, OWNERSHIP)) {
            denyRenameColumn(tableName.toString());
        }
    }

    public void checkCanSelectFromTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(transactionHandle, identity, tableName, SELECT)) {
            denySelectTable(tableName.toString());
        }
    }

    public void checkCanInsertIntoTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(transactionHandle, identity, tableName, INSERT)) {
            denyInsertTable(tableName.toString());
        }
    }

    public void checkCanDeleteFromTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(transactionHandle, identity, tableName, DELETE)) {
            denyDeleteTable(tableName.toString());
        }
    }

    public void checkCanCreateView(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName viewName)
    {
        if (!isSchemaOwner(transactionHandle, identity, viewName.getSchemaName())) {
            denyCreateView(viewName.toString());
        }
    }

    public void checkCanDropView(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName viewName)
    {
        // todo: add check for this
    }

    public void checkCanSelectFromView(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName viewName)
    {
        // todo: add check for this
    }

    public void checkCanCreateViewWithSelectFromTable(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName tableName)
    {
        if (!checkTablePermission(transactionHandle, identity, tableName, SELECT)) {
            denySelectTable(tableName.toString());
        }
        else if (!getGrantOptionForPrivilege(transactionHandle, identity, Privilege.SELECT, tableName)) {
            denyCreateViewWithSelect(tableName.toString());
        }
    }

    public void checkCanCreateViewWithSelectFromView(ConnectorTransactionHandle transactionHandle, Identity identity, SchemaTableName viewName)
    {
        // todo: add check for this
    }

    public void checkCanSetCatalogSessionProperty(Identity identity, String propertyName)
    {
        if (!isAdmin(null, identity)) {
            denySetCatalogSessionProperty(connectorId, propertyName);
        }
    }

    public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transactionHandle, Identity identity, Privilege privilege, SchemaTableName tableName)
    {
        if (checkTablePermission(transactionHandle, identity, tableName, OWNERSHIP)) {
            return;
        }

        RaptorPrivilegeInfo.RaptorPrivilege raptorPrivilege = toRaptorPrivilege(privilege);
        if (raptorPrivilege == null || !getGrantOptionForPrivilege(transactionHandle, identity, privilege, tableName)) {
            denyGrantTablePrivilege(privilege.name(), tableName.toString());
        }
    }

    public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transactionHandle, Identity identity, Privilege privilege, SchemaTableName tableName)
    {
        if (checkTablePermission(transactionHandle, identity, tableName, OWNERSHIP)) {
            return;
        }

        RaptorPrivilegeInfo.RaptorPrivilege raptorPrivilege = toRaptorPrivilege(privilege);
        if (raptorPrivilege == null || !getGrantOptionForPrivilege(transactionHandle, identity, privilege, tableName)) {
            denyRevokeTablePrivilege(privilege.name(), tableName.toString());
        }
    }

    private boolean checkTablePermission(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName, RaptorPrivilegeInfo.RaptorPrivilege... requiredPrivileges)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return true;
        }

        List<RaptorGrantInfo> raptorGrantInfos = dao.getGrantInfos(tableName.getSchemaName(), tableName.getTableName(), identity.getUser());

        long maskOnFile = raptorGrantInfos.stream()
                .map(RaptorGrantInfo::getPrivilegeInfo)
                .flatMap(Set::stream)
                .map(RaptorPrivilegeInfo::getRaptorPrivilege)
                .mapToLong(RaptorPrivilege::getMaskValue)
                .reduce(0, (a, b) -> a | b);

        long maskRequired = Arrays.stream(requiredPrivileges)
                .mapToLong(RaptorPrivilege::getMaskValue)
                .reduce(0, (a, b) -> a | b);

        return maskOnFile != 0 &&
                (maskOnFile & maskRequired) == maskRequired;
    }

    private boolean getGrantOptionForPrivilege(ConnectorTransactionHandle transaction, Identity identity, Privilege privilege, SchemaTableName tableName)
    {
        List<RaptorGrantInfo> grantInfos = dao.getGrantInfos(tableName.getSchemaName(), tableName.getTableName(), identity.getUser());

        if (grantInfos == null) {
            return false;
        }

        Set<PrivilegeInfo> privileges = grantInfos.stream()
                .map(RaptorGrantInfo::getPrivilegeInfo)
                .flatMap(Set::stream)
                .map(RaptorPrivilegeInfo::toPrivilegeInfo)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        return privileges.contains(new PrivilegeInfo(privilege, true));
    }

    private boolean isAdmin(ConnectorTransactionHandle transaction, Identity identity)
    {
        return identityManager.isAdmin(transaction, identity);
    }

    private boolean isSchemaOwner(ConnectorTransactionHandle transaction, Identity identity, String schemaName)
    {
        return identityManager.isSchemaOwner(transaction, identity, schemaName);
    }
}
