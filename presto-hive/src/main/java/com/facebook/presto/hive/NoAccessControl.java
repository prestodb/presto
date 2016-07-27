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
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.inject.Inject;

import java.util.Optional;

import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameTable;
import static java.util.Objects.requireNonNull;

public class NoAccessControl
        implements ConnectorAccessControl
{
    private final HiveMetastore metastore;
    private final boolean allowDropTable;
    private final boolean allowRenameTable;
    private final boolean allowAddColumn;
    private final boolean allowRenameColumn;

    @Inject
    public NoAccessControl(HiveMetastore metastore, HiveClientConfig hiveClientConfig)
    {
        requireNonNull(hiveClientConfig, "hiveClientConfig is null");
        allowDropTable = hiveClientConfig.getAllowDropTable();
        allowRenameTable = hiveClientConfig.getAllowRenameTable();
        allowAddColumn = hiveClientConfig.getAllowAddColumn();
        allowRenameColumn = hiveClientConfig.getAllowRenameColumn();
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    public void checkCanCreateTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanDropTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!allowDropTable) {
            denyDropTable(tableName.toString());
        }

        Optional<Table> target = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());

        if (!target.isPresent()) {
            denyDropTable(tableName.toString(), "Table not found");
        }

        if (!identity.getUser().equals(target.get().getOwner())) {
            denyDropTable(tableName.toString(), "Owner of the table is different from session user");
        }
    }

    @Override
    public void checkCanRenameTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName, SchemaTableName newTableName)
    {
        if (!allowRenameTable) {
            denyRenameTable(tableName.toString(), newTableName.toString());
        }
    }

    @Override
    public void checkCanAddColumn(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!allowAddColumn) {
            denyAddColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanRenameColumn(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!allowRenameColumn) {
            denyRenameColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanSelectFromTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
    }

    @Override
    public void checkCanDropView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
    }

    @Override
    public void checkCanSelectFromView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String propertyName)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(Identity identity, Privilege privilege, SchemaTableName tableName)
    {
    }

    @Override
    public void checkCanRevokeTablePrivilege(Identity identity, Privilege privilege, SchemaTableName tableName)
    {
    }
}
