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

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveConnectorId;
import com.facebook.presto.hive.HiveTransactionHandle;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import com.google.inject.Inject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.security.AccessControlException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateView;
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
import static com.facebook.presto.spi.security.AccessDeniedException.denyShowTablesMetadata;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class StorageBasedAccessControl
        implements ConnectorAccessControl
{
    public static final String ADMIN_ROLE_NAME = "admin";
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";

    private final String connectorId;
    private final Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider;
    private final ExtendedHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public StorageBasedAccessControl(
            HiveConnectorId connectorId,
            Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider,
            ExtendedHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.metastoreProvider = requireNonNull(metastoreProvider, "metastoreProvider is null");
        this.metastore = metastore;
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    @Override
    public void checkCanCreateSchema(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName)
    {
        // check if is admin or write at location ?
    }

    @Override
    public void checkCanDropSchema(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName)
    {
        if (!checkDatabaseFSAction(transactionHandle, identity, schemaName, FsAction.WRITE)) {
            denyDropSchema(schemaName);
        }
    }

    @Override
    public void checkCanRenameSchema(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName, String newSchemaName)
    {
        if (!checkDatabaseFSAction(transactionHandle, identity, schemaName, FsAction.WRITE)) {
            denyRenameSchema(schemaName, newSchemaName);
        }
    }

    @Override
    public void checkCanShowSchemas(ConnectorTransactionHandle transactionHandle, Identity identity)
    {
        System.out.println("here was me Zwierzonka");
    }

    @Override
    public Set<String> filterSchemas(ConnectorTransactionHandle transactionHandle, Identity identity, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!checkDatabaseFSAction(transaction, identity, tableName.getSchemaName(), FsAction.WRITE)) {
            denyCreateTable(tableName.toString());
        }
    }

    @Override
    public void checkCanDropTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!checkTableFSAction(transaction, identity, tableName, FsAction.WRITE)) {
            denyDropTable(tableName.toString());
        }
    }

    @Override
    public void checkCanRenameTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName, SchemaTableName newTableName)
    {
        if (!checkTableFSAction(transaction, identity, tableName, FsAction.WRITE)) {
            denyRenameTable(tableName.toString(), newTableName.toString());
        }
    }

    @Override
    public void checkCanShowTablesMetadata(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName)
    {
        if (!checkDatabaseFSAction(transactionHandle, identity, schemaName, FsAction.READ)) {
            denyShowTablesMetadata(schemaName);
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorTransactionHandle transactionHandle, Identity identity, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanAddColumn(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!checkTableFSAction(transaction, identity, tableName, FsAction.WRITE)) {
            denyAddColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanRenameColumn(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!checkTableFSAction(transaction, identity, tableName, FsAction.WRITE)) {
            denyRenameColumn(tableName.toString());
        }
    }

    @Override
    public void checkCanSelectFromTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!checkTableFSAction(transaction, identity, tableName, FsAction.READ)) {
            denySelectTable(tableName.toString());
        }
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!checkTableFSAction(transaction, identity, tableName, FsAction.WRITE)) {
            denyInsertTable(tableName.toString());
        }
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!checkTableFSAction(transaction, identity, tableName, FsAction.WRITE)) {
            denyDeleteTable(tableName.toString());
        }
    }

    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
        if (!checkDatabaseFSAction(transaction, identity, viewName.getSchemaName(), FsAction.WRITE)) {
            denyCreateView(viewName.toString());
        }
    }

    @Override
    public void checkCanDropView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
        // because view is not materialized checking for database credentials
        if (!checkDatabaseFSAction(transaction, identity, viewName.getSchemaName(), FsAction.WRITE)) {
            denyDropView(viewName.toString());
        }
    }

    @Override
    public void checkCanSelectFromView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
        // because view is not materialized checking for database credentials
        if (!checkDatabaseFSAction(transaction, identity, viewName.getSchemaName(), FsAction.READ)) {
            denySelectView(viewName.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        if (!checkTableFSAction(transaction, identity, tableName, FsAction.READ)) {
            denySelectTable(tableName.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
        if (!checkDatabaseFSAction(transaction, identity, viewName.getSchemaName(), FsAction.READ)) {
            denySelectView(viewName.toString());
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
    public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transaction, Identity identity, Privilege privilege, SchemaTableName tableName, String grantee, boolean withGrantOption)
    {
        denyGrantTablePrivilege(tableName.toString(), "In storage based security mode grants are not allowed.");
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transaction, Identity identity, Privilege privilege, SchemaTableName tableName, String revokee, boolean grantOptionFor)
    {
        denyRevokeTablePrivilege(tableName.toString(), "In storage based security mode grants are not allowed.");
    }

    private boolean checkDatabaseFSAction(ConnectorTransactionHandle transaction, Identity identity, String schemaName, FsAction action)
    {
        Optional<Database> target = metastoreProvider.apply(((HiveTransactionHandle) transaction)).getDatabase(schemaName);

        if (!target.isPresent()) {
            throw new AccessDeniedException(format("Database %s not found", schemaName));
        }

        String targetLocation = target.get().getLocation().get();
        String user = identity.getUser();

        return checkFSActionClassloader(user, targetLocation, action);
    }

    private boolean checkTableFSAction(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName, FsAction action)
    {
        if (INFORMATION_SCHEMA_NAME.equals(tableName.getSchemaName())) {
            return true;
        }

        Optional<Table> target = metastoreProvider.apply(((HiveTransactionHandle) transaction)).getTable(tableName.getSchemaName(), tableName.getTableName());

        if (!target.isPresent()) {
            throw new AccessDeniedException(format("Table %s not found", tableName.getTableName()));
        }

        String targetLocation = target.get().getStorage().getLocation();
        String user = identity.getUser();

        return checkFSActionClassloader(user, targetLocation, action);
    }

    private boolean checkFSActionClassloader(String user, String targetLocation, FsAction action)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(this.getClass().getClassLoader())) {
            return checkFSAction(user, targetLocation, action);
        }
    }

    private boolean checkFSAction(String user, String targetLocation, FsAction action)
    {
        try {
            Path targetPath = new Path(targetLocation);
            FileSystem tableFS = this.hdfsEnvironment.getFileSystem(user, targetPath);
            tableFS.access(targetPath, action);
        }
        catch (AccessControlException e) {
            return false;
        }
        catch (FileNotFoundException e) {
            throw new AccessDeniedException(
                    format("Location %s does not exist. Details: %s", targetLocation, e.getMessage()));
        }
        catch (IOException e) {
            throw new AccessDeniedException(
                    format("IO error: %s", targetLocation, e.getMessage()));
        }
        return true;
    }
}
