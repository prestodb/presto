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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.io.File;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.metadata.MetadataUtil.toSchemaTableName;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_STARTING_UP;
import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
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
        addSystemAccessControlFactory(new FileBasedSystemAccessControl.Factory());
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
            Map<String, String> properties = loadProperties(ACCESS_CONTROL_CONFIGURATION);
            checkArgument(!isNullOrEmpty(properties.get(ACCESS_CONTROL_PROPERTY_NAME)),
                    "Access control configuration %s does not contain %s",
                    ACCESS_CONTROL_CONFIGURATION.getAbsoluteFile(),
                    ACCESS_CONTROL_PROPERTY_NAME);

            loadSystemAccessControl(properties);
        }
        else {
            setSystemAccessControl(AllowAllSystemAccessControl.NAME, ImmutableMap.of());
        }
    }

    public void loadSystemAccessControl(Map<String, String> properties)
    {
        properties = new HashMap<>(properties);
        String accessControlName = properties.remove(ACCESS_CONTROL_PROPERTY_NAME);
        checkArgument(!isNullOrEmpty(accessControlName), "%s property must be present", ACCESS_CONTROL_PROPERTY_NAME);

        setSystemAccessControl(accessControlName, properties);
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
    public void checkCanSetUser(Identity identity, AccessControlContext context, Optional<Principal> principal, String userName)
    {
        requireNonNull(principal, "principal is null");
        requireNonNull(userName, "userName is null");

        authenticationCheck(() -> systemAccessControl.get().checkCanSetUser(identity, context, principal, userName));
    }

    @Override
    public void checkQueryIntegrity(Identity identity, AccessControlContext context, String query)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(query, "query is null");

        authenticationCheck(() -> systemAccessControl.get().checkQueryIntegrity(identity, context, query));
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, AccessControlContext context, Set<String> catalogs)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogs, "catalogs is null");

        return systemAccessControl.get().filterCatalogs(identity, context, catalogs);
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, AccessControlContext context, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalog is null");

        authenticationCheck(() -> systemAccessControl.get().checkCanAccessCatalog(identity, context, catalogName));
    }

    @Override
    public void checkCanCreateSchema(TransactionId transactionId, Identity identity, AccessControlContext context, CatalogSchemaName schemaName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(schemaName, "schemaName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, schemaName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateSchema(identity, context, schemaName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, schemaName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateSchema(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(schemaName.getCatalogName()), context, schemaName.getSchemaName()));
        }
    }

    @Override
    public void checkCanDropSchema(TransactionId transactionId, Identity identity, AccessControlContext context, CatalogSchemaName schemaName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(schemaName, "schemaName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, schemaName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDropSchema(identity, context, schemaName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, schemaName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropSchema(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(schemaName.getCatalogName()), context, schemaName.getSchemaName()));
        }
    }

    @Override
    public void checkCanRenameSchema(TransactionId transactionId, Identity identity, AccessControlContext context, CatalogSchemaName schemaName, String newSchemaName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(schemaName, "schemaName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, schemaName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanRenameSchema(identity, context, schemaName, newSchemaName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, schemaName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRenameSchema(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(schemaName.getCatalogName()), context, schemaName.getSchemaName(), newSchemaName));
        }
    }

    @Override
    public void checkCanShowSchemas(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, catalogName));

        authorizationCheck(() -> systemAccessControl.get().checkCanShowSchemas(identity, context, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanShowSchemas(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), context));
        }
    }

    @Override
    public Set<String> filterSchemas(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName, Set<String> schemaNames)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(schemaNames, "schemaNames is null");

        if (filterCatalogs(identity, context, ImmutableSet.of(catalogName)).isEmpty()) {
            return ImmutableSet.of();
        }

        schemaNames = systemAccessControl.get().filterSchemas(identity, context, catalogName, schemaNames);

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            schemaNames = entry.getAccessControl().filterSchemas(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), context, schemaNames);
        }
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateTable(identity, context, toCatalogSchemaTableName(tableName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateTable(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), context, toSchemaTableName(tableName)));
        }
    }

    @Override
    public void checkCanDropTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDropTable(identity, context, toCatalogSchemaTableName(tableName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropTable(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), context, toSchemaTableName(tableName)));
        }
    }

    @Override
    public void checkCanRenameTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(newTableName, "newTableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanRenameTable(identity, context, toCatalogSchemaTableName(tableName), toCatalogSchemaTableName(newTableName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRenameTable(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), context, toSchemaTableName(tableName), toSchemaTableName(newTableName)));
        }
    }

    @Override
    public void checkCanShowTablesMetadata(TransactionId transactionId, Identity identity, AccessControlContext context, CatalogSchemaName schema)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(schema, "schema is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, schema.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanShowTablesMetadata(identity, context, schema));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, schema.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanShowTablesMetadata(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(), context, schema.getSchemaName()));
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(tableNames, "tableNames is null");

        if (filterCatalogs(identity, context, ImmutableSet.of(catalogName)).isEmpty()) {
            return ImmutableSet.of();
        }

        tableNames = systemAccessControl.get().filterTables(identity, context, catalogName, tableNames);

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            tableNames = entry.getAccessControl().filterTables(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), context, tableNames);
        }
        return tableNames;
    }

    @Override
    public void checkCanAddColumns(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanAddColumn(identity, context, toCatalogSchemaTableName(tableName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanAddColumn(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), context, toSchemaTableName(tableName)));
        }
    }

    @Override
    public void checkCanDropColumn(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDropColumn(identity, context, toCatalogSchemaTableName(tableName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropColumn(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), context, toSchemaTableName(tableName)));
        }
    }

    @Override
    public void checkCanRenameColumn(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanRenameColumn(identity, context, toCatalogSchemaTableName(tableName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRenameColumn(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), context, toSchemaTableName(tableName)));
        }
    }

    @Override
    public void checkCanInsertIntoTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanInsertIntoTable(identity, context, toCatalogSchemaTableName(tableName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanInsertIntoTable(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), context, toSchemaTableName(tableName)));
        }
    }

    @Override
    public void checkCanDeleteFromTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDeleteFromTable(identity, context, toCatalogSchemaTableName(tableName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDeleteFromTable(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), context, toSchemaTableName(tableName)));
        }
    }

    @Override
    public void checkCanCreateView(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName viewName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(viewName, "viewName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, viewName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateView(identity, context, toCatalogSchemaTableName(viewName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, viewName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateView(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(viewName.getCatalogName()), context, toSchemaTableName(viewName)));
        }
    }

    @Override
    public void checkCanDropView(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName viewName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(viewName, "viewName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, viewName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDropView(identity, context, toCatalogSchemaTableName(viewName)));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, viewName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropView(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(viewName.getCatalogName()), context, toSchemaTableName(viewName)));
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateViewWithSelectFromColumns(identity, context, toCatalogSchemaTableName(tableName), columnNames));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateViewWithSelectFromColumns(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), context, toSchemaTableName(tableName), columnNames));
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(TransactionId transactionId, Identity identity, AccessControlContext context, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal grantee, boolean withGrantOption)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privilege, "privilege is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanGrantTablePrivilege(identity, context, privilege, toCatalogSchemaTableName(tableName), grantee, withGrantOption));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanGrantTablePrivilege(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), context, privilege, toSchemaTableName(tableName), grantee, withGrantOption));
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(TransactionId transactionId, Identity identity, AccessControlContext context, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal revokee, boolean grantOptionFor)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privilege, "privilege is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanRevokeTablePrivilege(identity, context, privilege, toCatalogSchemaTableName(tableName), revokee, grantOptionFor));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRevokeTablePrivilege(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), context, privilege, toSchemaTableName(tableName), revokee, grantOptionFor));
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, AccessControlContext context, String propertyName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(propertyName, "propertyName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanSetSystemSessionProperty(identity, context, propertyName));
    }

    @Override
    public void checkCanSetCatalogSessionProperty(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName, String propertyName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(propertyName, "propertyName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, catalogName));

        authorizationCheck(() -> systemAccessControl.get().checkCanSetCatalogSessionProperty(identity, context, catalogName, propertyName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanSetCatalogSessionProperty(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), context, propertyName));
        }
    }

    @Override
    public void checkCanSelectFromColumns(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(columnNames, "columnNames is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanSelectFromColumns(identity, context, toCatalogSchemaTableName(tableName), columnNames));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanSelectFromColumns(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(tableName.getCatalogName()), context, toSchemaTableName(tableName), columnNames));
        }
    }

    @Override
    public void checkCanCreateRole(TransactionId transactionId, Identity identity, AccessControlContext context, String role, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(role, "role is null");
        requireNonNull(grantor, "grantor is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateRole(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), context, role, grantor));
        }
    }

    @Override
    public void checkCanDropRole(TransactionId transactionId, Identity identity, AccessControlContext context, String role, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(role, "role is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropRole(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), context, role));
        }
    }

    @Override
    public void checkCanGrantRoles(TransactionId transactionId, Identity identity, AccessControlContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(roles, "roles is null");
        requireNonNull(grantees, "grantees is null");
        requireNonNull(grantor, "grantor is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanGrantRoles(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), context, roles, grantees, withAdminOption, grantor, catalogName));
        }
    }

    @Override
    public void checkCanRevokeRoles(TransactionId transactionId, Identity identity, AccessControlContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(roles, "roles is null");
        requireNonNull(grantees, "grantees is null");
        requireNonNull(grantor, "grantor is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRevokeRoles(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), context, roles, grantees, adminOptionFor, grantor, catalogName));
        }
    }

    @Override
    public void checkCanSetRole(TransactionId transactionId, Identity identity, AccessControlContext context, String role, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(role, "role is null");
        requireNonNull(catalogName, "catalog is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanSetRole(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), context, role, catalogName));
        }
    }

    @Override
    public void checkCanShowRoles(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authenticationCheck(() -> entry.getAccessControl().checkCanShowRoles(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), context, catalogName));
        }
    }

    @Override
    public void checkCanShowCurrentRoles(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authenticationCheck(() -> entry.getAccessControl().checkCanShowCurrentRoles(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), context, catalogName));
        }
    }

    @Override
    public void checkCanShowRoleGrants(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, context, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authenticationCheck(() -> entry.getAccessControl().checkCanShowRoleGrants(entry.getTransactionHandle(transactionId), identity.toConnectorIdentity(catalogName), context, catalogName));
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

    private CatalogSchemaTableName toCatalogSchemaTableName(QualifiedObjectName qualifiedObjectName)
    {
        return new CatalogSchemaTableName(qualifiedObjectName.getCatalogName(), qualifiedObjectName.getSchemaName(), qualifiedObjectName.getObjectName());
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
        public void checkQueryIntegrity(Identity identity, AccessControlContext context, String query)
        {
            throw new PrestoException(SERVER_STARTING_UP, "Presto server is still initializing");
        }

        @Override
        public void checkCanSetUser(Identity identity, AccessControlContext context, Optional<Principal> principal, String userName)
        {
            throw new PrestoException(SERVER_STARTING_UP, "Presto server is still initializing");
        }

        @Override
        public void checkCanSetSystemSessionProperty(Identity identity, AccessControlContext context, String propertyName)
        {
            throw new PrestoException(SERVER_STARTING_UP, "Presto server is still initializing");
        }

        @Override
        public void checkCanAccessCatalog(Identity identity, AccessControlContext context, String catalogName)
        {
            throw new PrestoException(SERVER_STARTING_UP, "Presto server is still initializing");
        }
    }
}
