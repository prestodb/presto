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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.SessionPropertyConfigurationManagerContext;
import com.facebook.presto.spi.session.SessionConfigurationContext;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManager;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManager.SystemSessionPropertyConfiguration;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManagerFactory;
import com.google.common.annotations.VisibleForTesting;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.Session.SessionBuilder;
import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SessionPropertyDefaults
{
    private static final Logger log = Logger.get(SessionPropertyDefaults.class);
    private static final File SESSION_PROPERTY_CONFIGURATION = new File("etc/session-property-config.properties");
    private static final String SESSION_PROPERTY_MANAGER_NAME = "session-property-config.configuration-manager";

    private final SessionPropertyConfigurationManagerContext configurationManagerContext;
    private final Map<String, SessionPropertyConfigurationManagerFactory> factories = new ConcurrentHashMap<>();
    private final AtomicReference<SessionPropertyConfigurationManager> delegate = new AtomicReference<>();
    private final String prestoServerVersion;

    @Inject
    public SessionPropertyDefaults(NodeInfo nodeInfo, NodeVersion nodeVersion)
    {
        this.configurationManagerContext = new SessionPropertyConfigurationManagerContextInstance(nodeInfo.getEnvironment());
        this.prestoServerVersion = requireNonNull(nodeVersion.getVersion(), "prestoServerVersion is null");
    }

    public void addConfigurationManagerFactory(SessionPropertyConfigurationManagerFactory sessionConfigFactory)
    {
        if (factories.putIfAbsent(sessionConfigFactory.getName(), sessionConfigFactory) != null) {
            throw new IllegalArgumentException(format("Session property configuration manager '%s' is already registered", sessionConfigFactory.getName()));
        }
    }

    public void loadConfigurationManager()
            throws IOException
    {
        if (!SESSION_PROPERTY_CONFIGURATION.exists()) {
            return;
        }

        Map<String, String> properties = loadProperties(SESSION_PROPERTY_CONFIGURATION);
        checkArgument(!isNullOrEmpty(properties.get(SESSION_PROPERTY_MANAGER_NAME)),
                "Session property configuration %s does not contain %s",
                SESSION_PROPERTY_CONFIGURATION,
                SESSION_PROPERTY_MANAGER_NAME);

        loadConfigurationManager(properties);
    }

    public void loadConfigurationManager(Map<String, String> properties)
    {
        properties = new HashMap<>(properties);
        String sessionPropertyManagerName = properties.remove(SESSION_PROPERTY_MANAGER_NAME);
        checkArgument(!isNullOrEmpty(sessionPropertyManagerName), "%s property must be present", SESSION_PROPERTY_MANAGER_NAME);

        setConfigurationManager(sessionPropertyManagerName, properties);
    }

    @VisibleForTesting
    public void setConfigurationManager(String configManagerName, Map<String, String> properties)
    {
        log.info("-- Loading session property configuration manager --");

        SessionPropertyConfigurationManagerFactory factory = factories.get(configManagerName);
        checkState(factory != null, "Session property configuration manager '%s' is not registered", configManagerName);

        SessionPropertyConfigurationManager manager = factory.create(properties, configurationManagerContext);
        checkState(delegate.compareAndSet(null, manager), "sessionPropertyConfigurationManager is already set");

        log.info("-- Loaded session property configuration manager %s --", configManagerName);
    }

    public void applyDefaultProperties(
            SessionBuilder sessionBuilder,
            Optional<String> queryType,
            Optional<ResourceGroupId> resourceGroupId)
    {
        SessionPropertyConfigurationManager configurationManager = delegate.get();
        if (configurationManager == null) {
            return;
        }

        SessionConfigurationContext context = new SessionConfigurationContext(
                sessionBuilder.getIdentity().getUser(),
                sessionBuilder.getSource(),
                sessionBuilder.getClientTags(),
                queryType,
                resourceGroupId,
                sessionBuilder.getClientInfo(),
                prestoServerVersion);

        SystemSessionPropertyConfiguration systemPropertyConfiguration = configurationManager.getSystemSessionProperties(context);
        Map<String, Map<String, String>> catalogPropertyOverrides = configurationManager.getCatalogSessionProperties(context);
        sessionBuilder.applyDefaultProperties(systemPropertyConfiguration, catalogPropertyOverrides);
    }
}
