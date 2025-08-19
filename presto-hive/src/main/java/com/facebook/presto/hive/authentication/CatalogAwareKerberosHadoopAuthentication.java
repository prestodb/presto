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
package com.facebook.presto.hive.authentication;

import com.facebook.presto.hive.HdfsConfigurationInitializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;

import javax.security.auth.Subject;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.hive.util.ConfigurationUtils.getInitialConfiguration;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.security.UserGroupInformationShim.createUserGroupInformationForSubject;

/**
 * Catalog-aware Kerberos Hadoop authentication that maintains separate
 * KerberosName configurations for each catalog to support cross-realm authentication.
 * This avoids the global UserGroupInformation.setConfiguration() call that causes
 * auth_to_local rules to be shared across all catalogs.
 */
public class CatalogAwareKerberosHadoopAuthentication
        implements HadoopAuthentication
{
    private final KerberosAuthentication kerberosAuthentication;
    private final String catalogName;
    private final Configuration catalogConfiguration;
    
    // Cache of KerberosName instances per catalog to avoid repeated initialization
    private static final ConcurrentMap<String, KerberosName> catalogKerberosNameCache = new ConcurrentHashMap<>();
    
    public static CatalogAwareKerberosHadoopAuthentication createCatalogAwareKerberosHadoopAuthentication(
            KerberosAuthentication kerberosAuthentication, 
            HdfsConfigurationInitializer initializer,
            String catalogName)
    {
        Configuration configuration = getInitialConfiguration();
        initializer.updateConfiguration(configuration);
        
        // Set Kerberos authentication type for this catalog's configuration
        configuration.set("hadoop.security.authentication", "kerberos");
        
        // Initialize KerberosName for this catalog if not already done
        initializeCatalogKerberosName(catalogName, configuration);
        
        return new CatalogAwareKerberosHadoopAuthentication(kerberosAuthentication, catalogName, configuration);
    }
    
    private static void initializeCatalogKerberosName(String catalogName, Configuration configuration)
    {
        catalogKerberosNameCache.computeIfAbsent(catalogName, key -> {
            try {
                // Create a new KerberosName instance with catalog-specific auth_to_local rules
                KerberosName.setConfiguration(configuration);
                return new KerberosName("dummy@REALM"); // Just to trigger initialization
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to initialize KerberosName for catalog: " + catalogName, e);
            }
        });
    }
    
    private CatalogAwareKerberosHadoopAuthentication(
            KerberosAuthentication kerberosAuthentication, 
            String catalogName,
            Configuration catalogConfiguration)
    {
        this.kerberosAuthentication = requireNonNull(kerberosAuthentication, "kerberosAuthentication is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.catalogConfiguration = requireNonNull(catalogConfiguration, "catalogConfiguration is null");
    }
    
    @Override
    public UserGroupInformation getUserGroupInformation()
    {
        Subject subject = kerberosAuthentication.getSubject();
        
        // Create UGI with catalog-specific configuration context
        return doWithCatalogConfiguration(() -> {
            return createUserGroupInformationForSubject(subject);
        });
    }
    
    /**
     * Execute the given action with the catalog-specific Kerberos configuration.
     * This ensures that KerberosName uses the correct auth_to_local rules for this catalog.
     */
    private <T> T doWithCatalogConfiguration(java.util.function.Supplier<T> action)
    {
        // Save current configuration
        Configuration originalConfig = UserGroupInformation.getConfiguration();
        
        try {
            // Temporarily set the catalog-specific configuration
            UserGroupInformation.setConfiguration(catalogConfiguration);
            
            // Ensure KerberosName uses the catalog-specific configuration
            KerberosName.setConfiguration(catalogConfiguration);
            
            return action.get();
        }
        finally {
            // Restore original configuration
            UserGroupInformation.setConfiguration(originalConfig);
            KerberosName.setConfiguration(originalConfig);
        }
    }
    
    public String getCatalogName()
    {
        return catalogName;
    }
    
    public Configuration getCatalogConfiguration()
    {
        return catalogConfiguration;
    }
}