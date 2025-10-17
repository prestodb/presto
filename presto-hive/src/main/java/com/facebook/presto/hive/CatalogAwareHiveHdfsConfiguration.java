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
package com.facebook.presto.hive;

import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.hive.util.ConfigurationUtils.copy;
import static com.facebook.presto.hive.util.ConfigurationUtils.getInitialConfiguration;
import static java.util.Objects.requireNonNull;

/**
 * Catalog-aware HDFS configuration that maintains separate configurations
 * for each catalog to support different auth_to_local rules.
 */
public class CatalogAwareHiveHdfsConfiguration
        implements HdfsConfiguration
{
    private static final Configuration INITIAL_CONFIGURATION = getInitialConfiguration();
    
    private final CatalogAwareHdfsConfigurationInitializer initializer;
    private final Set<DynamicConfigurationProvider> dynamicProviders;
    private final boolean isCopyOnFirstWriteConfigurationEnabled;
    
    // Cache configurations per catalog to avoid repeated initialization
    private final ConcurrentMap<String, Configuration> catalogConfigurations = new ConcurrentHashMap<>();
    
    @Inject
    public CatalogAwareHiveHdfsConfiguration(
            CatalogAwareHdfsConfigurationInitializer initializer,
            Set<DynamicConfigurationProvider> dynamicProviders,
            HiveClientConfig hiveClientConfig)
    {
        this.initializer = requireNonNull(initializer, "initializer is null");
        this.dynamicProviders = requireNonNull(dynamicProviders, "dynamicProviders is null");
        this.isCopyOnFirstWriteConfigurationEnabled = hiveClientConfig.isCopyOnFirstWriteConfigurationEnabled();
    }
    
    @Override
    public Configuration getConfiguration(HdfsContext context, URI uri)
    {
        String catalogName = context.getIdentity().getCatalog().orElse("default");
        
        // Get or create catalog-specific configuration
        Configuration catalogConfig = catalogConfigurations.computeIfAbsent(catalogName, this::createCatalogConfiguration);
        
        if (dynamicProviders.isEmpty()) {
            // Use the cached catalog configuration
            return isCopyOnFirstWriteConfigurationEnabled ? 
                new CopyOnFirstWriteConfiguration(catalogConfig) : catalogConfig;
        }
        
        // Apply dynamic providers to a copy of the catalog configuration
        Configuration config = new Configuration(catalogConfig);
        for (DynamicConfigurationProvider provider : dynamicProviders) {
            provider.updateConfiguration(config, context, uri);
        }
        
        return isCopyOnFirstWriteConfigurationEnabled ? 
            new CopyOnFirstWriteConfiguration(config) : config;
    }
    
    /**
     * Create a new configuration for the specified catalog.
     */
    private Configuration createCatalogConfiguration(String catalogName)
    {
        Configuration configuration = new Configuration(false);
        copy(INITIAL_CONFIGURATION, configuration);
        
        // Apply catalog-specific configuration
        initializer.updateConfiguration(configuration, catalogName);
        
        return configuration;
    }
    
    /**
     * Register auth_to_local rules for a specific catalog.
     */
    public void registerCatalogAuthToLocalRules(String catalogName, String authToLocalRules)
    {
        initializer.registerCatalogAuthToLocalRules(catalogName, authToLocalRules);
        
        // Invalidate cached configuration for this catalog to force recreation
        catalogConfigurations.remove(catalogName);
    }
    
    /**
     * Remove catalog-specific configuration.
     */
    public void removeCatalogConfiguration(String catalogName)
    {
        initializer.removeCatalogAuthToLocalRules(catalogName);
        catalogConfigurations.remove(catalogName);
    }
    
    /**
     * Get the auth_to_local rules for a specific catalog.
     */
    public String getCatalogAuthToLocalRules(String catalogName)
    {
        return initializer.getCatalogAuthToLocalRules(catalogName);
    }
    
    /**
     * Check if a catalog has specific auth_to_local rules configured.
     */
    public boolean hasCatalogSpecificRules(String catalogName)
    {
        return initializer.hasCatalogSpecificRules(catalogName);
    }
}