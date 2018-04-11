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

import com.facebook.presto.plugin.base.security.TableAccessControlRule.TablePrivilege;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.plugin.base.security.TableAccessControlRule.TablePrivilege.DELETE;
import static com.facebook.presto.plugin.base.security.TableAccessControlRule.TablePrivilege.GRANT_SELECT;
import static com.facebook.presto.plugin.base.security.TableAccessControlRule.TablePrivilege.INSERT;
import static com.facebook.presto.plugin.base.security.TableAccessControlRule.TablePrivilege.OWNERSHIP;
import static com.facebook.presto.plugin.base.security.TableAccessControlRule.TablePrivilege.SELECT;
import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDeleteTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static com.facebook.presto.spi.security.AccessDeniedException.denyInsertTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectView;

public class FileBasedAccessControl
        implements ConnectorAccessControl
{
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";

    private final List<SchemaAccessControlRule> schemaRules;
    private final List<TableAccessControlRule> tableRules;
    private final List<SessionPropertyAccessControlRule> sessionPropertyRules;

    @Inject
    public FileBasedAccessControl(FileBasedAccessControlConfig config, JsonCodec<AccessControlRules> codec)
            throws IOException
    {
        AccessControlRules rules = codec.fromJson(Files.readAllBytes(Paths.get(config.getConfigFile())));

        this.schemaRules = rules.getSchemaRules();
        this.tableRules = rules.getTableRules();
        this.sessionPropertyRules = rules.getSessionPropertyRules();
    }

    @Override
    public void checkCanShowSchemas(ConnectorTransactionHandle transactionHandle, ConnectorSession session)
    {
    }

    @Override
    public Set<String> filterSchemas(ConnectorTransactionHandle transactionHandle, ConnectorSession session, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(ConnectorTransactionHandle transaction, ConnectorSession session, SchemaTableName tableName)
    {
        if (!isDatabaseOwner(session.getIdentity(), tableName.getSchemaName())) {
            denyCreateTable(tableName.toString());
        }
    }

    @Override
    public void checkCanDropTable(ConnectorTransactionHandle transaction, ConnectorSession session, SchemaTableName tableName)
    {
        if (!checkTablePermission(session.getIdentity(), tableName, OWNERSHIP)) {
            denyDropTable(tableName.toString());
        }
    }

    @Override
    public void checkCanShowTablesMetadata(ConnectorTransactionHandle transactionHandle, ConnectorSession session, String schemaName)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorTransactionHandle transactionHandle, ConnectorSession session, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanRenameTable(ConnectorTransactionHandle transaction, ConnectorSession session, SchemaTableName tableName, SchemaTableName newTableName)
    {
        if (!checkTablePermission(session.getIdentity(), tableName, OWNERSHIP)) {
            denyRenameTable(tableName.toString(), newTableName.toString());
        }
    }

    @Override
    public void checkCanAddColumn(ConnectorTransactionHandle transaction, ConnectorSession session, SchemaTableName tableName)
    {
        if (!checkTablePermission(session.getIdentity(), tableName, OWNERSHIP)) {
            denyAddColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanDropColumn(ConnectorTransactionHandle transactionHandle, ConnectorSession session, SchemaTableName tableName)
    {
        if (!checkTablePermission(session.getIdentity(), tableName, OWNERSHIP)) {
            denyDropColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanRenameColumn(ConnectorTransactionHandle transaction, ConnectorSession session, SchemaTableName tableName)
    {
        if (!checkTablePermission(session.getIdentity(), tableName, OWNERSHIP)) {
            denyRenameColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanSelectFromTable(ConnectorTransactionHandle transaction, ConnectorSession session, SchemaTableName tableName)
    {
        if (!checkTablePermission(session.getIdentity(), tableName, SELECT)) {
            denySelectTable(tableName.toString());
        }
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transaction, ConnectorSession session, SchemaTableName tableName)
    {
        if (!checkTablePermission(session.getIdentity(), tableName, INSERT)) {
            denyInsertTable(tableName.toString());
        }
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transaction, ConnectorSession session, SchemaTableName tableName)
    {
        if (!checkTablePermission(session.getIdentity(), tableName, DELETE)) {
            denyDeleteTable(tableName.toString());
        }
    }

    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transaction, ConnectorSession session, SchemaTableName viewName)
    {
        if (!isDatabaseOwner(session.getIdentity(), viewName.getSchemaName())) {
            denyCreateView(viewName.toString());
        }
    }

    @Override
    public void checkCanDropView(ConnectorTransactionHandle transaction, ConnectorSession session, SchemaTableName viewName)
    {
        if (!checkTablePermission(session.getIdentity(), viewName, OWNERSHIP)) {
            denyDropView(viewName.toString());
        }
    }

    @Override
    public void checkCanSelectFromView(ConnectorTransactionHandle transaction, ConnectorSession session, SchemaTableName viewName)
    {
        if (!checkTablePermission(session.getIdentity(), viewName, SELECT)) {
            denySelectView(viewName.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(ConnectorTransactionHandle transaction, ConnectorSession session, SchemaTableName tableName)
    {
        if (!checkTablePermission(session.getIdentity(), tableName, SELECT)) {
            denySelectTable(tableName.toString());
        }
        if (!checkTablePermission(session.getIdentity(), tableName, GRANT_SELECT)) {
            denyCreateViewWithSelect(tableName.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(ConnectorTransactionHandle transaction, ConnectorSession session, SchemaTableName viewName)
    {
        if (!checkTablePermission(session.getIdentity(), viewName, SELECT)) {
            denySelectView(viewName.toString());
        }
        if (!checkTablePermission(session.getIdentity(), viewName, GRANT_SELECT)) {
            denyCreateView(viewName.toString());
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String propertyName)
    {
        if (!canSetSessionProperty(identity, propertyName)) {
            denySetSessionProperty(propertyName);
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transaction, ConnectorSession session, Privilege privilege, SchemaTableName tableName, String grantee, boolean withGrantOption)
    {
        if (!checkTablePermission(session.getIdentity(), tableName, OWNERSHIP)) {
            denyGrantTablePrivilege(privilege.name(), tableName.toString());
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transaction, ConnectorSession session, Privilege privilege, SchemaTableName tableName, String revokee, boolean grantOptionFor)
    {
        if (!checkTablePermission(session.getIdentity(), tableName, OWNERSHIP)) {
            denyRevokeTablePrivilege(privilege.name(), tableName.toString());
        }
    }

    private boolean canSetSessionProperty(Identity identity, String property)
    {
        for (SessionPropertyAccessControlRule rule : sessionPropertyRules) {
            Optional<Boolean> allowed = rule.match(identity.getUser(), property);
            if (allowed.isPresent() && allowed.get()) {
                return true;
            }
            if (allowed.isPresent() && !allowed.get()) {
                return false;
            }
        }
        return false;
    }

    private boolean checkTablePermission(Identity identity, SchemaTableName tableName, TablePrivilege... requiredPrivileges)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return true;
        }

        for (TableAccessControlRule rule : tableRules) {
            Optional<Set<TablePrivilege>> tablePrivileges = rule.match(identity.getUser(), tableName);
            if (tablePrivileges.isPresent()) {
                return tablePrivileges.get().containsAll(ImmutableSet.copyOf(requiredPrivileges));
            }
        }
        return false;
    }

    private boolean isDatabaseOwner(Identity identity, String schemaName)
    {
        for (SchemaAccessControlRule rule : schemaRules) {
            Optional<Boolean> owner = rule.match(identity.getUser(), schemaName);
            if (owner.isPresent()) {
                return owner.get();
            }
        }
        return false;
    }

    private static void denySetSessionProperty(String propertyName)
    {
        throw new AccessDeniedException("Cannot set catalog session property: " + propertyName);
    }
}
