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
package com.facebook.presto.features.config;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.features.ConfigurationSource;
import com.facebook.presto.spi.features.ConfigurationSourceFactory;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static com.facebook.presto.features.config.FeatureToggleConfig.FEATURES_CONFIG_SOURCE_TYPE;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.Files.getNameWithoutExtension;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FeatureToggleConfigurationManager
{
    private static final Logger log = Logger.get(FeatureToggleConfigurationManager.class);
    private static final Path FEATURE_TOGGLE_CONFIGURATION_DIR = Paths.get("etc", "feature-toggle");
    private final Map<String, ConfigurationSourceFactory> configurationSourceFactories = new ConcurrentHashMap<>();
    private final Map<String, ConfigurationSource> loadedConfigurationSources = new ConcurrentHashMap<>();
    private final AtomicBoolean configurationLoading = new AtomicBoolean();
    private final String configDirectory;

    @Inject
    public FeatureToggleConfigurationManager(FeatureToggleConfig featureToggleConfig)
    {
        requireNonNull(featureToggleConfig, "Feature Toggle Config is null");
        this.configDirectory = featureToggleConfig.getConfigDirectory();
        // always load default configuration source
        loadedConfigurationSources.put(DefaultConfigurationSource.NAME, new DefaultConfigurationSource.Factory().create(ImmutableMap.of()));
    }

    public void addConfigurationSourceFactory(ConfigurationSourceFactory configurationSourceFactory)
    {
        requireNonNull(configurationSourceFactory, "configurationSourceFactory is null");
        if (configurationSourceFactories.putIfAbsent(configurationSourceFactory.getName(), configurationSourceFactory) != null) {
            throw new IllegalArgumentException(format("Feature Toggle Configuration Source '%s' is already registered", configurationSourceFactory.getName()));
        }
    }

    public ConfigurationSource getConfigurationSource(String name)
    {
        ConfigurationSource configurationSource = loadedConfigurationSources.get(name);
        checkState(configurationSource != null, "configurationSource %s was not loaded", name);
        return configurationSource;
    }

    public void loadConfigurationSources()
            throws IOException
    {
        ImmutableMap.Builder<String, Map<String, String>> configurationProperties = ImmutableMap.builder();
        Path configurationDirectory;
        if (configDirectory == null) {
            configurationDirectory = FEATURE_TOGGLE_CONFIGURATION_DIR;
        }
        else {
            configurationDirectory = Paths.get(configDirectory);
        }
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(configurationDirectory, "*.properties")) {
            for (Path path : paths) {
                String name = getNameWithoutExtension(path.getFileName().toString());
                Map<String, String> properties = loadPropertiesFrom(path.toString());
                configurationProperties.put(name, properties);
            }
        }
        loadConfigurationSources(configurationProperties.build());
    }

    public void loadConfigurationSources(Map<String, Map<String, String>> configurationProperties)
    {
        if (!configurationLoading.compareAndSet(false, true)) {
            return;
        }
        configurationProperties.forEach(this::loadConfigurationSource);
    }

    protected void loadConfigurationSource(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        log.info(format("-- Loading configuration source %s --", name));

        String configurationSourceFactoryName = null;
        ImmutableMap.Builder<String, String> configurationSourceProperties = ImmutableMap.builder();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equals(FEATURES_CONFIG_SOURCE_TYPE)) {
                configurationSourceFactoryName = entry.getValue();
            }
            else {
                configurationSourceProperties.put(entry.getKey(), entry.getValue());
            }
        }
        checkState(configurationSourceFactoryName != null, "Configuration for Configuration source %s does not contain features.config-source-type", name);

        ConfigurationSourceFactory factory = configurationSourceFactories.get(configurationSourceFactoryName);
        checkState(factory != null, "Configuration Source Factory %s is not registered", configurationSourceFactoryName);

        ConfigurationSource configurationSource = factory.create(configurationSourceProperties.build());
        if (loadedConfigurationSources.putIfAbsent(name, configurationSource) != null) {
            throw new IllegalArgumentException(format("Configuration Source '%s' is already loaded", name));
        }

        log.info(format("-- Loaded Configuration Source %s --", name));
    }
}
