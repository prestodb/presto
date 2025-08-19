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

import com.facebook.presto.hive.gcs.GcsConfigurationInitializer;
import com.facebook.presto.hive.s3.S3ConfigurationUpdater;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Catalog-aware HDFS configuration initializer that supports per-catalog
 * auth_to_local rules for Kerberos cross-realm authentication.
 */
public class CatalogAwareHdfsConfigurationInitializer
        extends HdfsConfigurationInitializer
{
    private final Map<String, String> catalogAuthToLocalRules;
    
    @Inject
    public CatalogAwareHdfsConfigurationInitializer(
            HiveClientConfig hiveClientConfig,
            MetastoreClientConfig metastoreClientConfig,
            S3ConfigurationUpdater s3ConfigurationUpdater,
            GcsConfigurationInitializer gcsConfigurationInitializer)
    {
        super(hiveClientConfig, metastoreClientConfig, s3ConfigurationUpdater, gcsConfigurationInitializer);
        this.catalogAuthToLocalRules = new ConcurrentHashMap<>();
    }
    
    /**
     * Register catalog-specific auth_to_local rules.
     * 
     * @param catalogName the name of the catalog
     * @param authToLocalRules the auth_to_local rules for this catalog
     */
    public void registerCatalogAuthToLocalRules(String catalogName, String authToLocalRules)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(authToLocalRules, "authToLocalRules is null");
        
        catalogAuthToLocalRules.put(catalogName, authToLocalRules);
    }
    
    /**
     * Update configuration with catalog-specific settings.
     * 
     * @param configuration the Hadoop configuration to update
     * @param catalogName the catalog name for which to apply specific rules
     */
    public void updateConfiguration(Configuration configuration, String catalogName)
    {
        // First apply the base configuration
        super.updateConfiguration(configuration);
        
        // Then apply catalog-specific auth_to_local rules if available
        String catalogAuthRules = catalogAuthToLocalRules.get(catalogName);
        if (catalogAuthRules != null) {
            configuration.set("hadoop.security.auth_to_local", catalogAuthRules);
        }
    }
    
    /**
     * Get the auth_to_local rules for a specific catalog.
     * 
     * @param catalogName the catalog name
     * @return the auth_to_local rules, or null if not configured
     */
    public String getCatalogAuthToLocalRules(String catalogName)
    {
        return catalogAuthToLocalRules.get(catalogName);
    }
    
    /**
     * Remove catalog-specific auth_to_local rules.
     * 
     * @param catalogName the catalog name
     */
    public void removeCatalogAuthToLocalRules(String catalogName)
    {
        catalogAuthToLocalRules.remove(catalogName);
    }
    
    /**
     * Check if a catalog has specific auth_to_local rules configured.
     * 
     * @param catalogName the catalog name
     * @return true if the catalog has specific rules, false otherwise
     */
    public boolean hasCatalogSpecificRules(String catalogName)
    {
        return catalogAuthToLocalRules.containsKey(catalogName);
    }
}