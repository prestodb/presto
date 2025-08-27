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
import com.facebook.airlift.json.smile.SmileModule;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.eventlistener.EventListenerModule;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.execution.warnings.WarningCollectorModule;
import com.facebook.presto.metadata.StaticCatalogStore;
import com.facebook.presto.metadata.StaticFunctionNamespaceStore;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.security.AccessControlModule;
import com.facebook.presto.server.PluginManager;
import com.facebook.presto.server.SessionPropertyDefaults;
import com.facebook.presto.server.security.PasswordAuthenticatorManager;
import com.facebook.presto.server.security.PrestoAuthenticatorManager;
import com.facebook.presto.spark.classloader_interface.PrestoSparkBootstrapTimer;
import com.facebook.presto.spark.classloader_interface.SparkProcessType;
import com.facebook.presto.spark.execution.property.NativeExecutionConfigModule;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.storage.TempStorageManager;
import com.facebook.presto.storage.TempStorageModule;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.testing.TestingTempStorageManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.presto.server.PrestoSystemRequirements.verifySystemTimeIsReasonable;
import static com.facebook.presto.spark.classloader_interface.SparkProcessType.DRIVER;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.util.Objects.requireNonNull;

public class PrestoSparkInjectorFactory
{
    private final SparkProcessType sparkProcessType;
    private final Map<String, String> configProperties;
    private final Map<String, Map<String, String>> catalogProperties;
    private final Optional<Map<String, String>> nativeWorkerConfigProperties;
    private final Optional<Map<String, String>> eventListenerProperties;
    private final Optional<Map<String, String>> accessControlProperties;
    private final Optional<Map<String, String>> sessionPropertyConfigurationProperties;
    private final Optional<Map<String, Map<String, String>>> functionNamespaceProperties;
    private final Optional<Map<String, Map<String, String>>> tempStorageProperties;
    private final SqlParserOptions sqlParserOptions;
    private final List<Module> additionalModules;
    private final boolean isForTesting;

    public PrestoSparkInjectorFactory(
            SparkProcessType sparkProcessType,
            Map<String, String> configProperties,
            Map<String, Map<String, String>> catalogProperties,
            Optional<Map<String, String>> nativeWorkerConfigProperties,
            Optional<Map<String, String>> eventListenerProperties,
            Optional<Map<String, String>> accessControlProperties,
            Optional<Map<String, String>> sessionPropertyConfigurationProperties,
            Optional<Map<String, Map<String, String>>> functionNamespaceProperties,
            Optional<Map<String, Map<String, String>>> tempStorageProperties,
            SqlParserOptions sqlParserOptions,
            List<Module> additionalModules)
    {
        this(
                sparkProcessType,
                configProperties,
                catalogProperties,
                nativeWorkerConfigProperties,
                eventListenerProperties,
                accessControlProperties,
                sessionPropertyConfigurationProperties,
                functionNamespaceProperties,
                tempStorageProperties,
                sqlParserOptions,
                additionalModules,
                false);
    }

    public PrestoSparkInjectorFactory(
            SparkProcessType sparkProcessType,
            Map<String, String> configProperties,
            Map<String, Map<String, String>> catalogProperties,
            Optional<Map<String, String>> nativeWorkerConfigProperties,
            Optional<Map<String, String>> eventListenerProperties,
            Optional<Map<String, String>> accessControlProperties,
            Optional<Map<String, String>> sessionPropertyConfigurationProperties,
            Optional<Map<String, Map<String, String>>> functionNamespaceProperties,
            Optional<Map<String, Map<String, String>>> tempStorageProperties,
            SqlParserOptions sqlParserOptions,
            List<Module> additionalModules,
            boolean isForTesting)
    {
        this.sparkProcessType = requireNonNull(sparkProcessType, "sparkProcessType is null");
        this.configProperties = ImmutableMap.copyOf(requireNonNull(configProperties, "configProperties is null"));
        this.catalogProperties = requireNonNull(catalogProperties, "catalogProperties is null").entrySet().stream()
                .collect(toImmutableMap(Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue())));
        this.nativeWorkerConfigProperties = requireNonNull(nativeWorkerConfigProperties, "nativeWorkerConfigProperties is null").map(ImmutableMap::copyOf);
        this.eventListenerProperties = requireNonNull(eventListenerProperties, "eventListenerProperties is null").map(ImmutableMap::copyOf);
        this.accessControlProperties = requireNonNull(accessControlProperties, "accessControlProperties is null").map(ImmutableMap::copyOf);
        this.sessionPropertyConfigurationProperties = requireNonNull(sessionPropertyConfigurationProperties, "sessionPropertyConfigurationProperties is null").map(ImmutableMap::copyOf);
        this.functionNamespaceProperties = requireNonNull(functionNamespaceProperties, "functionNamespaceProperties is null")
                .map(map -> map.entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue()))));
        this.tempStorageProperties = requireNonNull(tempStorageProperties, "tempStorageProperties is null")
                .map(map -> map.entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue()))));
        this.sqlParserOptions = requireNonNull(sqlParserOptions, "sqlParserOptions is null");
        this.additionalModules = ImmutableList.copyOf(requireNonNull(additionalModules, "additionalModules is null"));
        this.isForTesting = isForTesting;
    }

    public Injector create(PrestoSparkBootstrapTimer bootstrapTimer)
    {
        bootstrapTimer.beginInjectorCreation();

        // TODO: migrate docker containers to a newer JVM, then re-enable it
        // verifyJvmRequirements();
        verifySystemTimeIsReasonable();

        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(
                new JsonModule(),
                new SmileModule(),
                new EventListenerModule(),
                new PrestoSparkModule(sparkProcessType, sqlParserOptions),
                new WarningCollectorModule());

        if (isForTesting) {
            modules.add(binder -> {
                binder.bind(TestingAccessControlManager.class).in(Scopes.SINGLETON);
                binder.bind(AccessControlManager.class).to(TestingAccessControlManager.class).in(Scopes.SINGLETON);
                binder.bind(AccessControl.class).to(AccessControlManager.class).in(Scopes.SINGLETON);

                binder.bind(TestingTempStorageManager.class).in(Scopes.SINGLETON);
                binder.bind(TempStorageManager.class).to(TestingTempStorageManager.class).in(Scopes.SINGLETON);

                newSetBinder(binder, PrestoSparkServiceWaitTimeMetrics.class).addBinding()
                        .to(PrestoSparkTestingServiceWaitTimeMetrics.class).in(Scopes.SINGLETON);
            });
        }
        else {
            modules.add(new AccessControlModule());
            modules.add(new TempStorageModule());
        }

        Map<String, String> nativeWorkerConfigs = new HashMap<>(
                this.nativeWorkerConfigProperties.orElse(ImmutableMap.of()));
        nativeWorkerConfigs.put("node.environment", "spark");
        modules.add(new NativeExecutionConfigModule(nativeWorkerConfigs));

        modules.addAll(additionalModules);

        Bootstrap app = new Bootstrap(modules.build());

        // Stream redirect doesn't work well with spark logging
        app.doNotInitializeLogging();

        Map<String, String> requiredConfigProperties = new HashMap<>();
        requiredConfigProperties.put("node.environment", "spark");
        requiredConfigProperties.putAll(configProperties);
        app.setRequiredConfigurationProperties(ImmutableMap.copyOf(requiredConfigProperties));

        bootstrapTimer.beginInjectorInitialization();
        Injector injector = app.initialize();
        bootstrapTimer.endInjectorInitialization();

        try {
            bootstrapTimer.beginSharedModulesLoading();
            injector.getInstance(PluginManager.class).loadPlugins();
            injector.getInstance(StaticCatalogStore.class).loadCatalogs(catalogProperties);
            injector.getInstance(ResourceGroupManager.class).loadConfigurationManager();
            injector.getInstance(PasswordAuthenticatorManager.class).loadPasswordAuthenticator();
            injector.getInstance(PrestoAuthenticatorManager.class).loadPrestoAuthenticator();
            eventListenerProperties.ifPresent(properties -> injector.getInstance(EventListenerManager.class).loadConfiguredEventListener(properties));
            bootstrapTimer.endSharedModulesLoading();

            bootstrapTimer.beginNonTestingModulesLoading();
            if (!isForTesting) {
                if (accessControlProperties.isPresent()) {
                    injector.getInstance(AccessControlManager.class).loadSystemAccessControl(accessControlProperties.get());
                }
                else {
                    injector.getInstance(AccessControlManager.class).loadSystemAccessControl();
                }

                if (tempStorageProperties.isPresent()) {
                    injector.getInstance(TempStorageManager.class).loadTempStorages(tempStorageProperties.get());
                }
                else {
                    injector.getInstance(TempStorageManager.class).loadTempStorages();
                }
            }
            bootstrapTimer.endNonTestingModulesLoading();

            bootstrapTimer.beginDriverModulesLoading();
            if ((sparkProcessType.equals(DRIVER))) {
                if (sessionPropertyConfigurationProperties.isPresent()) {
                    injector.getInstance(SessionPropertyDefaults.class).loadConfigurationManager(sessionPropertyConfigurationProperties.get());
                }
                else {
                    injector.getInstance(SessionPropertyDefaults.class).loadConfigurationManager();
                }
            }

            if (sparkProcessType.equals(DRIVER) ||
                    !injector.getInstance(FeaturesConfig.class).isInlineSqlFunctions()) {
                if (functionNamespaceProperties.isPresent()) {
                    injector.getInstance(StaticFunctionNamespaceStore.class).loadFunctionNamespaceManagers(functionNamespaceProperties.get());
                }
                else {
                    injector.getInstance(StaticFunctionNamespaceStore.class).loadFunctionNamespaceManagers();
                }
            }
            bootstrapTimer.endDriverModulesLoading();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        bootstrapTimer.endInjectorCreation();
        return injector;
    }
}
