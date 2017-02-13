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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.StandardErrorCode.SERVER_STARTING_UP;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Maps.fromProperties;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AccessControlManager
        implements AccessControl
{
    private static final Logger log = Logger.get(AccessControlManager.class);
    private static final File ACCESS_CONTROL_CONFIGURATION = new File("etc/access-control.properties");
    private static final String ACCESS_CONTROL_PROPERTY_NAME = "access-control.name";

    private final TransactionManager transactionManager;
    private final Map<String, SystemAccessControlFactory> systemAccessControlFactories = new ConcurrentHashMap<>();
    private final Map<ConnectorId, CatalogAccessControlEntry> connectorAccessControl = new ConcurrentHashMap<>();

    private final AtomicReference<SystemAccessControl> systemAccessControl = new AtomicReference<>(new InitializingSystemAccessControl());
    private final AtomicBoolean systemAccessControlLoading = new AtomicBoolean();

    private final CounterStat authenticationSuccess = new CounterStat();
    private final CounterStat authenticationFail = new CounterStat();
    private final CounterStat authorizationSuccess = new CounterStat();
    private final CounterStat authorizationFail = new CounterStat();

    @Inject
    public AccessControlManager(TransactionManager transactionManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        addSystemAccessControlFactory(new AllowAllSystemAccessControl.Factory());
        addSystemAccessControlFactory(new ReadOnlySystemAccessControl.Factory());
    }

    public void addSystemAccessControlFactory(SystemAccessControlFactory accessControlFactory)
    {
        requireNonNull(accessControlFactory, "accessControlFactory is null");

        if (systemAccessControlFactories.putIfAbsent(accessControlFactory.getName(), accessControlFactory) != null) {
            throw new IllegalArgumentException(format("Access control '%s' is already registered", accessControlFactory.getName()));
        }
    }

    public void addCatalogAccessControl(ConnectorId connectorId, ConnectorAccessControl accessControl)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(accessControl, "accessControl is null");
        checkState(connectorAccessControl.putIfAbsent(connectorId, new CatalogAccessControlEntry(connectorId, accessControl)) == null,
                "Access control for connector '%s' is already registered", connectorId);
    }

    public void removeCatalogAccessControl(ConnectorId connectorId)
    {
        connectorAccessControl.remove(connectorId);
    }

    public void loadSystemAccessControl()
            throws Exception
    {
        if (ACCESS_CONTROL_CONFIGURATION.exists()) {
            Map<String, String> properties = new HashMap<>(loadProperties(ACCESS_CONTROL_CONFIGURATION));

            String accessControlName = properties.remove(ACCESS_CONTROL_PROPERTY_NAME);
            checkArgument(!isNullOrEmpty(accessControlName),
                    "Access control configuration %s does not contain %s", ACCESS_CONTROL_CONFIGURATION.getAbsoluteFile(), ACCESS_CONTROL_PROPERTY_NAME);

            setSystemAccessControl(accessControlName, properties);
        }
        else {
            setSystemAccessControl(AllowAllSystemAccessControl.NAME, ImmutableMap.of());
        }
    }

    @VisibleForTesting
    protected void setSystemAccessControl(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        checkState(systemAccessControlLoading.compareAndSet(false, true), "System access control already initialized");

        log.info("-- Loading system access control --");

        SystemAccessControlFactory systemAccessControlFactory = systemAccessControlFactories.get(name);
        checkState(systemAccessControlFactory != null, "Access control %s is not registered", name);

        SystemAccessControl systemAccessControl = systemAccessControlFactory.create(ImmutableMap.copyOf(properties));
        this.systemAccessControl.set(systemAccessControl);

        log.info("-- Loaded system access control %s --", name);
    }

    @Override
    public void checkCanSetUser(Principal principal, String userName)
    {
        requireNonNull(userName, "userName is null");

        authenticationCheck(() -> systemAccessControl.get().checkCanSetUser(principal, userName));
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogs, "catalogs is null");

        return systemAccessControl.get().filterCatalogs(identity, catalogs);
    }

    @Override
    public void checkCanCreateSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(schemaName, "schemaName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateSchema(identity, schemaName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, schemaName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateSchema(entry.getTransactionHandle(transactionId), identity, schemaName.getSchemaName()));
        }
    }

    @Override
    public void checkCanDropSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(schemaName, "schemaName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanDropSchema(identity, schemaName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, schemaName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropSchema(entry.getTransactionHandle(transactionId), identity, schemaName.getSchemaName()));
        }
    }

    @Override
    public void checkCanRenameSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName, String newSchemaName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(schemaName, "schemaName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanRenameSchema(identity, schemaName, newSchemaName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, schemaName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRenameSchema(entry.getTransactionHandle(transactionId), identity, schemaName.getSchemaName(), newSchemaName));
        }
    }

    @Override
    public void checkCanShowSchemas(TransactionId transactionId, Identity identity, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanShowSchemas(identity, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanShowSchemas(entry.getTransactionHandle(transactionId), identity));
        }
    }

    @Override
    public Set<String> filterSchemas(TransactionId transactionId, Identity identity, String catalogName, Set<String> schemaNames)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(schemaNames, "schemaNames is null");

        schemaNames = systemAccessControl.get().filterSchemas(identity, catalogName, schemaNames);

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            schemaNames = entry.getAccessControl().filterSchemas(entry.getTransactionHandle(transactionId), identity, schemaNames);
        }
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateTable(identity, tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateTable(entry.getTransactionHandle(transactionId), identity, tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanDropTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanDropTable(identity, tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropTable(entry.getTransactionHandle(transactionId), identity, tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanRenameTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(newTableName, "newTableName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanRenameTable(identity, tableName.asCatalogSchemaTableName(), newTableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRenameTable(entry.getTransactionHandle(transactionId), identity, tableName.asSchemaTableName(), newTableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanShowTables(TransactionId transactionId, Identity identity, CatalogSchemaName schema)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(schema, "schema is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanShowTables(identity, schema));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, schema.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanShowTables(entry.getTransactionHandle(transactionId), identity, schema.getSchemaName()));
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(TransactionId transactionId, Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(tableNames, "tableNames is null");

        tableNames = systemAccessControl.get().filterTables(identity, catalogName, tableNames);

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            tableNames = entry.getAccessControl().filterTables(entry.getTransactionHandle(transactionId), identity, tableNames);
        }
        return tableNames;
    }

    @Override
    public void checkCanAddColumns(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanAddColumn(identity, tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanAddColumn(entry.getTransactionHandle(transactionId), identity, tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanRenameColumn(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanRenameColumn(identity, tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRenameColumn(entry.getTransactionHandle(transactionId), identity, tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanSelectFromTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanSelectFromTable(identity, tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanSelectFromTable(entry.getTransactionHandle(transactionId), identity, tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanInsertIntoTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanInsertIntoTable(identity, tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanInsertIntoTable(entry.getTransactionHandle(transactionId), identity, tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanDeleteFromTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanDeleteFromTable(identity, tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDeleteFromTable(entry.getTransactionHandle(transactionId), identity, tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanCreateView(TransactionId transactionId, Identity identity, QualifiedObjectName viewName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(viewName, "viewName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateView(identity, viewName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, viewName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateView(entry.getTransactionHandle(transactionId), identity, viewName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanDropView(TransactionId transactionId, Identity identity, QualifiedObjectName viewName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(viewName, "viewName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanDropView(identity, viewName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, viewName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropView(entry.getTransactionHandle(transactionId), identity, viewName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanSelectFromView(TransactionId transactionId, Identity identity, QualifiedObjectName viewName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(viewName, "viewName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanSelectFromView(identity, viewName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, viewName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanSelectFromView(entry.getTransactionHandle(transactionId), identity, viewName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateViewWithSelectFromTable(identity, tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateViewWithSelectFromTable(entry.getTransactionHandle(transactionId), identity, tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(TransactionId transactionId, Identity identity, QualifiedObjectName viewName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(viewName, "viewName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateViewWithSelectFromView(identity, viewName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, viewName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateViewWithSelectFromView(entry.getTransactionHandle(transactionId), identity, viewName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(TransactionId transactionId, Identity identity, Privilege privilege, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privilege, "privilege is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanGrantTablePrivilege(identity, privilege, tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanGrantTablePrivilege(entry.getTransactionHandle(transactionId), identity, privilege, tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(TransactionId transactionId, Identity identity, Privilege privilege, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privilege, "privilege is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanRevokeTablePrivilege(identity, privilege, tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRevokeTablePrivilege(entry.getTransactionHandle(transactionId), identity, privilege, tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(propertyName, "propertyName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanSetSystemSessionProperty(identity, propertyName));
    }

    @Override
    public void checkCanSetCatalogSessionProperty(TransactionId transactionId, Identity identity, String catalogName, String propertyName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(propertyName, "propertyName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanSetCatalogSessionProperty(identity, catalogName, propertyName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanSetCatalogSessionProperty(identity, propertyName));
        }
    }

    private CatalogAccessControlEntry getConnectorAccessControl(TransactionId transactionId, String catalogName)
    {
        return transactionManager.getOptionalCatalogMetadata(transactionId, catalogName)
                .map(metadata -> connectorAccessControl.get(metadata.getConnectorId()))
                .orElse(null);
    }

    @Managed
    @Nested
    public CounterStat getAuthenticationSuccess()
    {
        return authenticationSuccess;
    }

    @Managed
    @Nested
    public CounterStat getAuthenticationFail()
    {
        return authenticationFail;
    }

    @Managed
    @Nested
    public CounterStat getAuthorizationSuccess()
    {
        return authorizationSuccess;
    }

    @Managed
    @Nested
    public CounterStat getAuthorizationFail()
    {
        return authorizationFail;
    }

    private void authenticationCheck(Runnable runnable)
    {
        try {
            runnable.run();
            authenticationSuccess.update(1);
        }
        catch (PrestoException e) {
            authenticationFail.update(1);
            throw e;
        }
    }

    private void authorizationCheck(Runnable runnable)
    {
        try {
            runnable.run();
            authorizationSuccess.update(1);
        }
        catch (PrestoException e) {
            authorizationFail.update(1);
            throw e;
        }
    }

    private static Map<String, String> loadProperties(File file)
            throws Exception
    {
        requireNonNull(file, "file is null");

        Properties properties = new Properties();
        try (FileInputStream in = new FileInputStream(file)) {
            properties.load(in);
        }
        return fromProperties(properties);
    }

    private class CatalogAccessControlEntry
    {
        private final ConnectorId connectorId;
        private final ConnectorAccessControl accessControl;

        public CatalogAccessControlEntry(ConnectorId connectorId, ConnectorAccessControl accessControl)
        {
            this.connectorId = requireNonNull(connectorId, "connectorId is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
        }

        public ConnectorId getConnectorId()
        {
            return connectorId;
        }

        public ConnectorAccessControl getAccessControl()
        {
            return accessControl;
        }

        public ConnectorTransactionHandle getTransactionHandle(TransactionId transactionId)
        {
            return transactionManager.getConnectorTransaction(transactionId, connectorId);
        }
    }

    private static class InitializingSystemAccessControl
            implements SystemAccessControl
    {
        @Override
        public void checkCanSetUser(Principal principal, String userName)
        {
            throw new PrestoException(SERVER_STARTING_UP, "Presto server is still initializing");
        }

        @Override
        public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
        {
            throw new PrestoException(SERVER_STARTING_UP, "Presto server is still initializing");
        }
    }
}
