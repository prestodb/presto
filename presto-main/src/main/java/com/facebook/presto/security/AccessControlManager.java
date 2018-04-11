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

import com.facebook.presto.Session;
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
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.io.File;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
    public Set<String> filterCatalogs(Session session, Set<String> catalogs)
    {
        requireNonNull(session, "session is null");
        requireNonNull(catalogs, "catalogs is null");

        return systemAccessControl.get().filterCatalogs(session.toConnectorSession(), catalogs);
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
        requireNonNull(identity, "session is null");
        requireNonNull(catalogName, "catalog is null");

        authenticationCheck(() -> systemAccessControl.get().checkCanAccessCatalog(identity, catalogName));
    }

    @Override
    public void checkCanCreateSchema(Session session, CatalogSchemaName schemaName)
    {
        requireNonNull(session, "identity is null");
        requireNonNull(schemaName, "schemaName is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), schemaName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateSchema(session.toConnectorSession(), schemaName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), schemaName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateSchema(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), schemaName.getSchemaName()));
        }
    }

    @Override
    public void checkCanDropSchema(Session session, CatalogSchemaName schemaName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(schemaName, "schemaName is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), schemaName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDropSchema(session.toConnectorSession(), schemaName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), schemaName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropSchema(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), schemaName.getSchemaName()));
        }
    }

    @Override
    public void checkCanRenameSchema(Session session, CatalogSchemaName schemaName, String newSchemaName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(schemaName, "schemaName is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), schemaName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanRenameSchema(session.toConnectorSession(), schemaName, newSchemaName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), schemaName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRenameSchema(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), schemaName.getSchemaName(), newSchemaName));
        }
    }

    @Override
    public void checkCanShowSchemas(Session session, String catalogName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), catalogName));

        authorizationCheck(() -> systemAccessControl.get().checkCanShowSchemas(session.toConnectorSession(), catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanShowSchemas(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId())));
        }
    }

    @Override
    public Set<String> filterSchemas(Session session, String catalogName, Set<String> schemaNames)
    {
        requireNonNull(session, "session is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(schemaNames, "schemaNames is null");

        if (filterCatalogs(session, ImmutableSet.of(catalogName)).isEmpty()) {
            return ImmutableSet.of();
        }

        schemaNames = systemAccessControl.get().filterSchemas(session.toConnectorSession(), catalogName, schemaNames);

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), catalogName);
        if (entry != null) {
            schemaNames = entry.getAccessControl().filterSchemas(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), schemaNames);
        }
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(Session session, QualifiedObjectName tableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateTable(session.toConnectorSession(), tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateTable(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanDropTable(Session session, QualifiedObjectName tableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDropTable(session.toConnectorSession(), tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropTable(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanRenameTable(Session session, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(newTableName, "newTableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanRenameTable(session.toConnectorSession(), tableName.asCatalogSchemaTableName(), newTableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRenameTable(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), tableName.asSchemaTableName(), newTableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanShowTablesMetadata(Session session, CatalogSchemaName schema)
    {
        requireNonNull(session, "session is null");
        requireNonNull(schema, "schema is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), schema.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanShowTablesMetadata(session.toConnectorSession(), schema));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), schema.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanShowTablesMetadata(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), schema.getSchemaName()));
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(Session session, String catalogName, Set<SchemaTableName> tableNames)
    {
        requireNonNull(session, "session is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(tableNames, "tableNames is null");

        if (filterCatalogs(session, ImmutableSet.of(catalogName)).isEmpty()) {
            return ImmutableSet.of();
        }

        tableNames = systemAccessControl.get().filterTables(session.toConnectorSession(), catalogName, tableNames);

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), catalogName);
        if (entry != null) {
            tableNames = entry.getAccessControl().filterTables(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), tableNames);
        }
        return tableNames;
    }

    @Override
    public void checkCanAddColumns(Session session, QualifiedObjectName tableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanAddColumn(session.toConnectorSession(), tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanAddColumn(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanDropColumn(Session session, QualifiedObjectName tableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDropColumn(session.toConnectorSession(), tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropColumn(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanRenameColumn(Session session, QualifiedObjectName tableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanRenameColumn(session.toConnectorSession(), tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRenameColumn(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanSelectFromTable(Session session, QualifiedObjectName tableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanSelectFromTable(session.toConnectorSession(), tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanSelectFromTable(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanInsertIntoTable(Session session, QualifiedObjectName tableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanInsertIntoTable(session.toConnectorSession(), tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanInsertIntoTable(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanDeleteFromTable(Session session, QualifiedObjectName tableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDeleteFromTable(session.toConnectorSession(), tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDeleteFromTable(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanCreateView(Session session, QualifiedObjectName viewName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(viewName, "viewName is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), viewName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateView(session.toConnectorSession(), viewName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), viewName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateView(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), viewName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanDropView(Session session, QualifiedObjectName viewName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(viewName, "viewName is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), viewName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDropView(session.toConnectorSession(), viewName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), viewName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropView(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), viewName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanSelectFromView(Session session, QualifiedObjectName viewName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(viewName, "viewName is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), viewName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanSelectFromView(session.toConnectorSession(), viewName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), viewName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanSelectFromView(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), viewName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(Session session, QualifiedObjectName tableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateViewWithSelectFromTable(session.toConnectorSession(), tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateViewWithSelectFromTable(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(Session session, QualifiedObjectName viewName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(viewName, "viewName is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), viewName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateViewWithSelectFromView(session.toConnectorSession(), viewName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), viewName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateViewWithSelectFromView(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), viewName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(Session session, Privilege privilege, QualifiedObjectName tableName, String grantee, boolean withGrantOption)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privilege, "privilege is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanGrantTablePrivilege(session.toConnectorSession(), privilege, tableName.asCatalogSchemaTableName(), grantee, withGrantOption));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanGrantTablePrivilege(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), privilege, tableName.asSchemaTableName(), grantee, withGrantOption));
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(Session session, Privilege privilege, QualifiedObjectName tableName, String revokee, boolean grantOptionFor)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privilege, "privilege is null");

        authenticationCheck(() -> checkCanAccessCatalog(session.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanRevokeTablePrivilege(session.toConnectorSession(), privilege, tableName.asCatalogSchemaTableName(), revokee, grantOptionFor));

        CatalogAccessControlEntry entry = getConnectorAccessControl(session.getRequiredTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRevokeTablePrivilege(entry.getTransactionHandle(session.getRequiredTransactionId()), session.toConnectorSession(entry.getConnectorId()), privilege, tableName.asSchemaTableName(), revokee, grantOptionFor));
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
        requireNonNull(identity, "session is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(propertyName, "propertyName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, catalogName));

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

        @Override
        public void checkCanAccessCatalog(Identity identity, String catalogName)
        {
            throw new PrestoException(SERVER_STARTING_UP, "Presto server is still initializing");
        }
    }
}
