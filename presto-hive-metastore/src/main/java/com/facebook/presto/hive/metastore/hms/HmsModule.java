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
package com.facebook.presto.hive.metastore.hms;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.hive.ForCachingHiveMetastore;
import com.facebook.presto.hive.ForRecordingHiveMetastore;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.InMemoryCachingHiveMetastore;
import com.facebook.presto.hive.metastore.RecordingHiveMetastore;
import com.facebook.presto.hive.metastore.hms.http.HttpHiveMetastoreClientFactory;
import com.facebook.presto.hive.metastore.hms.http.HttpHiveMetastoreConfig;
import com.facebook.presto.hive.metastore.hms.thrift.ThriftHiveMetastoreClientFactory;
import com.facebook.presto.spi.ConnectorId;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.OptionalBinder;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HmsModule
        extends AbstractConfigurationAwareModule
{
    private final String connectorId;

    public HmsModule(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        StaticMetastoreConfig staticMetastoreConfig = buildConfigObject(StaticMetastoreConfig.class);
        requireNonNull(staticMetastoreConfig.getMetastoreUris(), "metastoreUris is null");
        boolean hasHttpOrHttpsMetastore = staticMetastoreConfig.getMetastoreUris().stream()
                .anyMatch(uri -> ("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme())));

        if (hasHttpOrHttpsMetastore) {
            OptionalBinder.newOptionalBinder(binder, MetastoreClientFactory.class)
                    .setDefault().to(HttpHiveMetastoreClientFactory.class).in(Scopes.SINGLETON);
            binder.bind(HiveCluster.class).to(StaticHiveCluster.class).in(Scopes.SINGLETON);
            configBinder(binder).bindConfig(StaticMetastoreConfig.class);
            HttpHiveMetastoreConfig metastoreClientConfig = buildConfigObject(HttpHiveMetastoreConfig.class);
            boolean hasHttpsMetastore = staticMetastoreConfig.getMetastoreUris().stream().anyMatch(uri -> "https".equalsIgnoreCase(uri.getScheme()));
            if (hasHttpsMetastore && !metastoreClientConfig.getHttpMetastoreTlsEnabled()) {
                throw new IllegalStateException("'hive.metastore.http.client.tls.enabled' must be set to 'true' while using https metastore URIs in 'hive.metastore.uri'");
            }
            if (hasHttpsMetastore && metastoreClientConfig.getHttpMetastoreTlsTruststorePath() == null && metastoreClientConfig.getHttpMetastoreTlsTruststorePassword().isEmpty()) {
                throw new IllegalStateException("'hive.metastore.http.client.tls.truststore.path' & 'hive.metastore.http.client.tls.truststore.password' must be set while using https metastore URIs in 'hive.metastore.uri'");
            }
            if (hasHttpsMetastore && metastoreClientConfig.getHttpMetastoreTlsKeystorePath() == null && metastoreClientConfig.getHttpMetastoreTlsKeystorePassword().isEmpty()) {
                throw new IllegalStateException("'hive.metastore.http.client.tls.keystore.path' & 'hive.metastore.http.client.tls.keystore.password' must be set while using https metastore URIs in 'hive.metastore.uri'");
            }
        }
        else {
            OptionalBinder.newOptionalBinder(binder, MetastoreClientFactory.class)
                    .setDefault().to(ThriftHiveMetastoreClientFactory.class).in(Scopes.SINGLETON);
            binder.bind(HiveCluster.class).to(StaticHiveCluster.class).in(Scopes.SINGLETON);
            configBinder(binder).bindConfig(StaticMetastoreConfig.class);
        }

        binder.bind(HiveMetastore.class).to(ThriftHiveMetastore.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorId.class).toInstance(new ConnectorId(connectorId));

        if (buildConfigObject(MetastoreClientConfig.class).getRecordingPath() != null) {
            binder.bind(ExtendedHiveMetastore.class)
                    .annotatedWith(ForRecordingHiveMetastore.class)
                    .to(BridgingHiveMetastore.class)
                    .in(Scopes.SINGLETON);
            binder.bind(ExtendedHiveMetastore.class)
                    .annotatedWith(ForCachingHiveMetastore.class)
                    .to(RecordingHiveMetastore.class)
                    .in(Scopes.SINGLETON);
            binder.bind(RecordingHiveMetastore.class).in(Scopes.SINGLETON);
            newExporter(binder).export(RecordingHiveMetastore.class)
                    .as(generatedNameOf(RecordingHiveMetastore.class, connectorId));
        }
        else {
            binder.bind(ExtendedHiveMetastore.class)
                    .annotatedWith(ForCachingHiveMetastore.class)
                    .to(BridgingHiveMetastore.class)
                    .in(Scopes.SINGLETON);
        }

        binder.bind(ExtendedHiveMetastore.class).to(InMemoryCachingHiveMetastore.class).in(Scopes.SINGLETON);
        newExporter(binder).export(HiveMetastore.class)
                .as(generatedNameOf(ThriftHiveMetastore.class, connectorId));
        newExporter(binder).export(ExtendedHiveMetastore.class)
                .as(generatedNameOf(InMemoryCachingHiveMetastore.class, connectorId));
    }
}
