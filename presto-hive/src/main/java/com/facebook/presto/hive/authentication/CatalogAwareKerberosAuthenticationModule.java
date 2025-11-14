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

import com.facebook.presto.hive.CatalogAwareHdfsConfigurationInitializer;
import com.facebook.presto.hive.CatalogAwareHiveHdfsConfiguration;
import com.facebook.presto.hive.CatalogKerberosConfig;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;

/**
 * Authentication module for catalog-aware Kerberos authentication.
 * This module provides separate Kerberos authentication contexts for each catalog,
 * allowing different auth_to_local rules per catalog.
 */
public class CatalogAwareKerberosAuthenticationModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        // Bind catalog-specific Kerberos configuration
        configBinder(binder).bindConfig(CatalogKerberosConfig.class);
        
        // Bind catalog-aware HDFS configuration initializer
        binder.bind(HdfsConfigurationInitializer.class)
                .to(CatalogAwareHdfsConfigurationInitializer.class)
                .in(Scopes.SINGLETON);
        
        // Bind catalog-aware HDFS configuration
        binder.bind(HdfsConfiguration.class)
                .to(CatalogAwareHiveHdfsConfiguration.class)
                .in(Scopes.SINGLETON);
        
        // Bind catalog-aware Hadoop authentication
        binder.bind(HadoopAuthentication.class)
                .to(CatalogAwareKerberosHadoopAuthentication.class)
                .in(Scopes.SINGLETON);
    }
}