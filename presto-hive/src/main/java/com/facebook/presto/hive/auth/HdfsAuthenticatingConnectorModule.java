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

import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveMetadata;
import com.facebook.presto.hive.HivePageSinkProvider;
import com.facebook.presto.hive.HivePageSourceProvider;
import com.facebook.presto.hive.HiveSplitManager;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;

public class HdfsAuthenticatingConnectorModule
        extends PrivateModule
{
    @Override
    protected void configure()
    {
        bind(HiveMetadata.class).in(Scopes.SINGLETON);
        bind(ConnectorMetadata.class).to(HdfsAuthenticatingMetadata.class);
        expose(ConnectorMetadata.class);

        bind(HiveSplitManager.class).in(Scopes.SINGLETON);
        bind(ConnectorSplitManager.class).to(HdfsAuthenticatingSplitManager.class).in(Scopes.SINGLETON);
        expose(ConnectorSplitManager.class);

        bind(HivePageSourceProvider.class).in(Scopes.SINGLETON);
        bind(ConnectorPageSourceProvider.class).to(HdfsAuthenticatingPageSourceProvider.class).in(Scopes.SINGLETON);
        expose(ConnectorPageSourceProvider.class);

        bind(HivePageSinkProvider.class).in(Scopes.SINGLETON);
        bind(ConnectorPageSinkProvider.class).to(HdfsAuthenticatingPageSinkProvider.class).in(Scopes.SINGLETON);
        expose(ConnectorPageSinkProvider.class);
    }

    @Inject
    @Provides
    @Singleton
    HadoopKerberosAuthentication getKerberosAuthentication(HiveClientConfig hiveClientConfig)
    {
        String hdfsPrestoPrincipal = hiveClientConfig.getHdfsPrestoPrincipal();
        String hdfsPrestoKeytab = hiveClientConfig.getHdfsPrestoKeytab();
        List<String> configurationFiles = firstNonNull(hiveClientConfig.getResourceConfigFiles(), ImmutableList.of());
        Configuration configuration = new Configuration();
        configurationFiles.forEach(filePath -> configuration.addResource(new Path(filePath)));
        HadoopKerberosAuthentication authentication = new HadoopKerberosAuthentication(
                hdfsPrestoPrincipal, hdfsPrestoKeytab, configuration);
        authentication.authenticate();
        return authentication;
    }
}
