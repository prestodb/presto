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
package com.facebook.presto.spark;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.eventlistener.EventListenerModule;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.metadata.StaticCatalogStore;
import com.facebook.presto.metadata.StaticFunctionNamespaceStore;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.security.AccessControlModule;
import com.facebook.presto.server.PluginManager;
import com.facebook.presto.server.SessionPropertyDefaults;
import com.facebook.presto.server.security.PasswordAuthenticatorManager;
import com.facebook.presto.spark.classloader_interface.SparkProcessType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.presto.server.PrestoSystemRequirements.verifySystemTimeIsReasonable;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class PrestoSparkInjectorFactory
{
    private final SparkProcessType sparkProcessType;
    private final Map<String, String> configProperties;
    private final Map<String, Map<String, String>> catalogProperties;
    private final List<Module> additionalModules;
    private final Optional<Module> accessControlModuleOverride;

    public PrestoSparkInjectorFactory(
            SparkProcessType sparkProcessType,
            Map<String, String> configProperties,
            Map<String, Map<String, String>> catalogProperties,
            List<Module> additionalModules)
    {
        this(sparkProcessType, configProperties, catalogProperties, additionalModules, Optional.empty());
    }

    public PrestoSparkInjectorFactory(
            SparkProcessType sparkProcessType,
            Map<String, String> configProperties,
            Map<String, Map<String, String>> catalogProperties,
            List<Module> additionalModules,
            Optional<Module> accessControlModuleOverride)
    {
        this.sparkProcessType = requireNonNull(sparkProcessType, "sparkProcessType is null");
        this.configProperties = ImmutableMap.copyOf(requireNonNull(configProperties, "configProperties is null"));
        this.catalogProperties = requireNonNull(catalogProperties, "catalogProperties is null").entrySet().stream()
                .collect(toImmutableMap(Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue())));
        this.additionalModules = ImmutableList.copyOf(requireNonNull(additionalModules, "additionalModules is null"));
        this.accessControlModuleOverride = requireNonNull(accessControlModuleOverride, "accessControlModuleOverride is null");
    }

    public Injector create()
    {
        // TODO: migrate docker containers to a newer JVM, then re-enable it
        // verifyJvmRequirements();
        verifySystemTimeIsReasonable();

        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(
                new JsonModule(),
                new EventListenerModule(),
                new PrestoSparkModule(sparkProcessType));

        boolean initializeAccessControl = false;
        if (accessControlModuleOverride.isPresent()) {
            modules.add(accessControlModuleOverride.get());
        }
        else {
            modules.add(new AccessControlModule());
            initializeAccessControl = true;
        }

        modules.addAll(additionalModules);

        Bootstrap app = new Bootstrap(modules.build());

        // Stream redirect doesn't work well with spark logging
        app.doNotInitializeLogging();

        Map<String, String> requiredProperties = new HashMap<>();
        requiredProperties.put("node.environment", "spark");
        requiredProperties.putAll(configProperties);

        app.setRequiredConfigurationProperties(ImmutableMap.copyOf(requiredProperties));

        Injector injector = app.strictConfig().initialize();

        try {
            injector.getInstance(PluginManager.class).loadPlugins();
            injector.getInstance(StaticCatalogStore.class).loadCatalogs(catalogProperties);
            injector.getInstance(StaticFunctionNamespaceStore.class).loadFunctionNamespaceManagers();
            injector.getInstance(SessionPropertyDefaults.class).loadConfigurationManager();
            injector.getInstance(ResourceGroupManager.class).loadConfigurationManager();
            injector.getInstance(PasswordAuthenticatorManager.class).loadPasswordAuthenticator();
            injector.getInstance(EventListenerManager.class).loadConfiguredEventListener();

            if (initializeAccessControl) {
                injector.getInstance(AccessControlManager.class).loadSystemAccessControl();
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        return injector;
    }
}
