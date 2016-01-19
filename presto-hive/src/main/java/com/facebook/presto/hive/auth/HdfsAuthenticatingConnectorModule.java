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

package com.facebook.presto.hive.auth;

import com.facebook.presto.hive.ForHdfs;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveClientConfig.HdfsAuthenticationType;
import com.facebook.presto.hive.HiveMetadataFactory;
import com.facebook.presto.hive.HivePageSinkProvider;
import com.facebook.presto.hive.HivePageSourceProvider;
import com.facebook.presto.hive.HiveSplitManager;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;

public class HdfsAuthenticatingConnectorModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(HiveMetadataFactory.class).in(Scopes.SINGLETON);
        bind(ConnectorMetadata.class).to(HdfsAuthenticatingMetadata.class);

        bind(HiveSplitManager.class).in(Scopes.SINGLETON);
        bind(ConnectorSplitManager.class).to(HdfsAuthenticatingSplitManager.class).in(Scopes.SINGLETON);

        bind(HivePageSourceProvider.class).in(Scopes.SINGLETON);
        bind(ConnectorPageSourceProvider.class).to(HdfsAuthenticatingPageSourceProvider.class).in(Scopes.SINGLETON);

        bind(HivePageSinkProvider.class).in(Scopes.SINGLETON);
        bind(ConnectorPageSinkProvider.class).to(HdfsAuthenticatingPageSinkProvider.class).in(Scopes.SINGLETON);
    }

    @Inject
    @Provides
    @Singleton
    @ForHdfs
    HadoopAuthentication createHadoopAuthentication(HiveClientConfig hiveClientConfig,
            HdfsConfiguration hdfsConfiguration)
    {
        String hdfsPrestoPrincipal = hiveClientConfig.getHdfsPrestoPrincipal();
        String hdfsPrestoKeytab = hiveClientConfig.getHdfsPrestoKeytab();
        Configuration configuration = createConfiguration(hiveClientConfig);
        HdfsAuthenticationType authenticationType = hiveClientConfig.getHdfsAuthenticationType();

        HadoopAuthentication authentication = createAuthentication(
                hdfsPrestoPrincipal,
                hdfsPrestoKeytab,
                configuration,
                authenticationType
        );

        authentication.authenticate();
        return authentication;
    }

    private Configuration createConfiguration(HiveClientConfig hiveClientConfig)
    {
        List<String> configurationFiles = firstNonNull(hiveClientConfig.getResourceConfigFiles(), ImmutableList.of());
        Configuration configuration = new Configuration();
        configurationFiles.forEach(filePath -> configuration.addResource(new Path(filePath)));
        return configuration;
    }

    private HadoopAuthentication createAuthentication(String principal,
            String keytab, Configuration configuration, HdfsAuthenticationType authenticationType)
    {
        switch (authenticationType) {
            case KERBEROS:
                return new HadoopKerberosAuthentication(principal, keytab, configuration);
            case KERBEROS_IMPERSONATION:
                return new HadoopKerberosImpersonatingAuthentication(principal, keytab, configuration);
            case SIMPLE_IMPERSONATION:
                return new HadoopSimpleImpersonatingAuthentication();
            default:
                throw new IllegalArgumentException("Authentication type is not supported: " + authenticationType);
        }
    }
}
