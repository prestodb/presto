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

import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.ConnectorAccessControl;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
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

    public static final String ALLOW_ALL_ACCESS_CONTROL = "allow-all";

    private final Map<String, SystemAccessControlFactory> systemAccessControlFactories = new ConcurrentHashMap<>();
    private final Map<String, ConnectorAccessControl> catalogAccessControl = new ConcurrentHashMap<>();

    private final AtomicReference<SystemAccessControl> systemAccessControl = new AtomicReference<>(new InitializingSystemAccessControl());
    private final AtomicBoolean systemAccessControlLoading = new AtomicBoolean();

    private final CounterStat authenticationSuccess = new CounterStat();
    private final CounterStat authenticationFail = new CounterStat();
    private final CounterStat authorizationSuccess = new CounterStat();
    private final CounterStat authorizationFail = new CounterStat();

    @Inject
    public AccessControlManager()
    {
        systemAccessControlFactories.put(ALLOW_ALL_ACCESS_CONTROL, new SystemAccessControlFactory()
        {
            @Override
            public String getName()
            {
                return ALLOW_ALL_ACCESS_CONTROL;
            }

            @Override
            public SystemAccessControl create(Map<String, String> config)
            {
                requireNonNull(config, "config is null");
                checkArgument(config.isEmpty(), "The none access-controller does not support any configuration properties");
                return new AllowAllSystemAccessControl();
            }
        });
    }

    public void addSystemAccessControlFactory(SystemAccessControlFactory accessControlFactory)
    {
        requireNonNull(accessControlFactory, "accessControlFactory is null");

        if (systemAccessControlFactories.putIfAbsent(accessControlFactory.getName(), accessControlFactory) != null) {
            throw new IllegalArgumentException(format("Access control '%s' is already registered", accessControlFactory.getName()));
        }
    }

    public void addCatalogAccessControl(String catalogName, ConnectorAccessControl accessControl)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(accessControl, "accessControl is null");

        if (catalogAccessControl.putIfAbsent(catalogName, accessControl) != null) {
            throw new IllegalArgumentException(format("Access control for catalog '%s' is already registered", catalogName));
        }
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
            setSystemAccessControl(ALLOW_ALL_ACCESS_CONTROL, ImmutableMap.of());
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
    public void checkCanCreateTable(Identity identity, QualifiedTableName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(tableName.getCatalogName());
        if (accessControl != null) {
            authorizationCheck(() -> accessControl.checkCanCreateTable(identity, tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanDropTable(Identity identity, QualifiedTableName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(tableName.getCatalogName());
        if (accessControl != null) {
            authorizationCheck(() -> accessControl.checkCanDropTable(identity, tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanRenameTable(Identity identity, QualifiedTableName tableName, QualifiedTableName newTableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(newTableName, "newTableName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(tableName.getCatalogName());
        if (accessControl != null) {
            authorizationCheck(() -> accessControl.checkCanRenameTable(identity, tableName.asSchemaTableName(), newTableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanAddColumns(Identity identity, QualifiedTableName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(tableName.getCatalogName());
        if (accessControl != null) {
            authorizationCheck(() -> accessControl.checkCanAddColumn(identity, tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanRenameColumn(Identity identity, QualifiedTableName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(tableName.getCatalogName());
        if (accessControl != null) {
            authorizationCheck(() -> accessControl.checkCanRenameColumn(identity, tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanSelectFromTable(Identity identity, QualifiedTableName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(tableName.getCatalogName());
        if (accessControl != null) {
            authorizationCheck(() -> accessControl.checkCanSelectFromTable(identity, tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, QualifiedTableName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(tableName.getCatalogName());
        if (accessControl != null) {
            authorizationCheck(() -> accessControl.checkCanInsertIntoTable(identity, tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, QualifiedTableName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(tableName.getCatalogName());
        if (accessControl != null) {
            authorizationCheck(() -> accessControl.checkCanDeleteFromTable(identity, tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanCreateView(Identity identity, QualifiedTableName viewName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(viewName, "viewName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(viewName.getCatalogName());
        if (accessControl != null) {
            authorizationCheck(() -> accessControl.checkCanCreateView(identity, viewName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanDropView(Identity identity, QualifiedTableName viewName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(viewName, "viewName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(viewName.getCatalogName());
        if (accessControl != null) {
            authorizationCheck(() -> accessControl.checkCanDropView(identity, viewName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanSelectFromView(Identity identity, QualifiedTableName viewName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(viewName, "viewName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(viewName.getCatalogName());
        if (accessControl != null) {
            authorizationCheck(() -> accessControl.checkCanSelectFromView(identity, viewName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(Identity identity, QualifiedTableName tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(tableName.getCatalogName());
        if (accessControl != null) {
            authorizationCheck(() -> accessControl.checkCanCreateViewWithSelectFromTable(identity, tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(Identity identity, QualifiedTableName viewName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(viewName, "viewName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(viewName.getCatalogName());
        if (accessControl != null) {
            authorizationCheck(() -> accessControl.checkCanCreateViewWithSelectFromView(identity, viewName.asSchemaTableName()));
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
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(propertyName, "propertyName is null");

        ConnectorAccessControl accessControl = catalogAccessControl.get(catalogName);
        if (accessControl != null) {
            authorizationCheck(() -> accessControl.checkCanSetCatalogSessionProperty(identity, propertyName));
        }
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

    private static class AllowAllSystemAccessControl
            implements SystemAccessControl
    {
        @Override
        public void checkCanSetUser(Principal principal, String userName)
        {
        }

        @Override
        public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
        {
        }
    }
}
