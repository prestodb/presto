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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Configuration for catalog-specific Kerberos settings, particularly
 * auth_to_local rules for cross-realm authentication.
 */
public class CatalogKerberosConfig
{
    private Map<String, String> catalogAuthToLocalRules = new ConcurrentHashMap<>();
    private boolean enableCatalogAwareKerberos = false;
    
    @NotNull
    public Map<String, String> getCatalogAuthToLocalRules()
    {
        return catalogAuthToLocalRules;
    }
    
    @Config("hive.catalog.auth-to-local-rules")
    @ConfigDescription("Catalog-specific auth_to_local rules in format: catalog1=rule1,catalog2=rule2")
    public CatalogKerberosConfig setCatalogAuthToLocalRules(Map<String, String> catalogAuthToLocalRules)
    {
        this.catalogAuthToLocalRules = catalogAuthToLocalRules;
        return this;
    }
    
    public boolean isEnableCatalogAwareKerberos()
    {
        return enableCatalogAwareKerberos;
    }
    
    @Config("hive.catalog.kerberos.enabled")
    @ConfigDescription("Enable catalog-aware Kerberos authentication with separate auth_to_local rules per catalog")
    public CatalogKerberosConfig setEnableCatalogAwareKerberos(boolean enableCatalogAwareKerberos)
    {
        this.enableCatalogAwareKerberos = enableCatalogAwareKerberos;
        return this;
    }
    
    /**
     * Get auth_to_local rules for a specific catalog.
     */
    public String getAuthToLocalRulesForCatalog(String catalogName)
    {
        return catalogAuthToLocalRules.get(catalogName);
    }
    
    /**
     * Set auth_to_local rules for a specific catalog.
     */
    public void setAuthToLocalRulesForCatalog(String catalogName, String rules)
    {
        catalogAuthToLocalRules.put(catalogName, rules);
    }
    
    /**
     * Check if a catalog has specific auth_to_local rules.
     */
    public boolean hasRulesForCatalog(String catalogName)
    {
        return catalogAuthToLocalRules.containsKey(catalogName);
    }
}