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
package com.facebook.presto.server;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.SessionPropertyConfigurationManagerContext;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.session.SessionConfigurationContext;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManager;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManagerFactory;
import com.facebook.presto.sql.SqlEnvironmentConfig;
import com.facebook.presto.sql.SqlPath;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.Session.SessionBuilder;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class QuerySessionSupplier
        implements SessionSupplier
{
    private static final Logger log = Logger.get(QuerySessionSupplier.class);
    private static final File SESSION_PROPERTY_CONFIGURATION = new File("etc/session-property-config.properties");
    private static final String SESSION_PROPERTY_MANAGER_NAME = "session-property-config.configuration-manager";

    private final SessionPropertyConfigurationManagerContext configurationManagerContext;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final SessionPropertyManager sessionPropertyManager;
    private final Map<String, SessionPropertyConfigurationManagerFactory> sessionPropertyConfigurationManagerFactories = new ConcurrentHashMap<>();
    private final AtomicReference<SessionPropertyConfigurationManager> sessionPropertyConfigurationManager = new AtomicReference<>();
    private final Optional<String> path;

    @Inject
    public QuerySessionSupplier(
            NodeInfo nodeInfo,
            TransactionManager transactionManager,
            AccessControl accessControl,
            SessionPropertyManager sessionPropertyManager,
            SqlEnvironmentConfig config)
    {
        this.configurationManagerContext = new SessionPropertyConfigurationManagerContextInstance(nodeInfo.getEnvironment());
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.path = requireNonNull(config.getPath(), "path is null");
    }

    @Override
    public void addConfigurationManager(SessionPropertyConfigurationManagerFactory sessionConfigFactory)
    {
        if (sessionPropertyConfigurationManagerFactories.putIfAbsent(sessionConfigFactory.getName(), sessionConfigFactory) != null) {
            throw new IllegalArgumentException(format("Session property configuration manager '%s' is already registered", sessionConfigFactory.getName()));
        }
    }

    @Override
    public void loadConfigurationManager()
            throws IOException
    {
        if (!SESSION_PROPERTY_CONFIGURATION.exists()) {
            return;
        }

        Map<String, String> propertyMap = new HashMap<>(loadProperties(SESSION_PROPERTY_CONFIGURATION));

        log.info("-- Loading session property configuration manager --");

        String configManagerName = propertyMap.remove(SESSION_PROPERTY_MANAGER_NAME);
        checkArgument(configManagerName != null, "Session property configuration %s does not contain %s", SESSION_PROPERTY_CONFIGURATION, SESSION_PROPERTY_MANAGER_NAME);

        setConfigurationManager(configManagerName, propertyMap);

        log.info("-- Loaded session property configuration manager %s --", configManagerName);
    }

    @VisibleForTesting
    public void setConfigurationManager(String name, Map<String, String> properties)
    {
        SessionPropertyConfigurationManagerFactory factory = sessionPropertyConfigurationManagerFactories.get(name);
        checkState(factory != null, "Session property configuration manager %s is not registered");

        SessionPropertyConfigurationManager manager = factory.create(properties, configurationManagerContext);
        checkState(sessionPropertyConfigurationManager.compareAndSet(null, manager), "sessionPropertyConfigurationManager is already set");
    }

    @Override
    public Session createSession(QueryId queryId, SessionContext context, Optional<String> queryType, ResourceGroupId resourceGroupId)
    {
        Identity identity = context.getIdentity();
        accessControl.checkCanSetUser(identity.getPrincipal().orElse(null), identity.getUser());

        SessionBuilder sessionBuilder = Session.builder(sessionPropertyManager)
                .setQueryId(queryId)
                .setIdentity(identity)
                .setSource(context.getSource())
                .setCatalog(context.getCatalog())
                .setSchema(context.getSchema())
                .setPath(new SqlPath(path))
                .setRemoteUserAddress(context.getRemoteUserAddress())
                .setUserAgent(context.getUserAgent())
                .setClientInfo(context.getClientInfo())
                .setClientTags(context.getClientTags())
                .setClientCapabilities(context.getClientCapabilities())
                .setTraceToken(context.getTraceToken())
                .setResourceEstimates(context.getResourceEstimates());

        if (context.getPath() != null) {
            sessionBuilder.setPath(new SqlPath(Optional.of(context.getPath())));
        }

        if (context.getTimeZoneId() != null) {
            sessionBuilder.setTimeZoneKey(getTimeZoneKey(context.getTimeZoneId()));
        }

        if (context.getLanguage() != null) {
            sessionBuilder.setLocale(Locale.forLanguageTag(context.getLanguage()));
        }

        if (sessionPropertyConfigurationManager.get() != null) {
            SessionConfigurationContext configContext = new SessionConfigurationContext(
                    context.getIdentity().getUser(),
                    Optional.ofNullable(context.getSource()),
                    context.getClientTags(),
                    queryType,
                    resourceGroupId);
            for (Entry<String, String> entry : sessionPropertyConfigurationManager.get().getSystemSessionProperties(configContext).entrySet()) {
                sessionBuilder.setSystemProperty(entry.getKey(), entry.getValue());
            }
            for (Entry<String, Map<String, String>> catalogProperties : sessionPropertyConfigurationManager.get().getCatalogSessionProperties(configContext).entrySet()) {
                String catalog = catalogProperties.getKey();
                for (Entry<String, String> entry : catalogProperties.getValue().entrySet()) {
                    sessionBuilder.setCatalogSessionProperty(catalog, entry.getKey(), entry.getValue());
                }
            }
        }

        for (Entry<String, String> entry : context.getSystemProperties().entrySet()) {
            sessionBuilder.setSystemProperty(entry.getKey(), entry.getValue());
        }
        for (Entry<String, Map<String, String>> catalogProperties : context.getCatalogSessionProperties().entrySet()) {
            String catalog = catalogProperties.getKey();
            for (Entry<String, String> entry : catalogProperties.getValue().entrySet()) {
                sessionBuilder.setCatalogSessionProperty(catalog, entry.getKey(), entry.getValue());
            }
        }

        for (Entry<String, String> preparedStatement : context.getPreparedStatements().entrySet()) {
            sessionBuilder.addPreparedStatement(preparedStatement.getKey(), preparedStatement.getValue());
        }

        if (context.supportClientTransaction()) {
            sessionBuilder.setClientTransactionSupport();
        }

        Session session = sessionBuilder.build();
        if (context.getTransactionId().isPresent()) {
            session = session.beginTransactionId(context.getTransactionId().get(), transactionManager, accessControl);
        }

        return session;
    }
}
