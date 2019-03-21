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
package com.facebook.presto.hive.metastore.thrift;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.hive.ForCachingHiveMetastore;
import com.facebook.presto.hive.ForRecordingHiveMetastore;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.RecordingHiveMetastore;
import com.facebook.presto.spi.ConnectorId;
import com.google.inject.Binder;
import com.google.inject.Scopes;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ThriftMetastoreModule
        extends AbstractConfigurationAwareModule
{
    private final String connectorId;

    public ThriftMetastoreModule(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(HiveMetastoreClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(HiveCluster.class).to(StaticHiveCluster.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(StaticMetastoreConfig.class);

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

        binder.bind(ExtendedHiveMetastore.class).to(CachingHiveMetastore.class).in(Scopes.SINGLETON);
        newExporter(binder).export(HiveMetastore.class)
                .as(generatedNameOf(ThriftHiveMetastore.class, connectorId));
        newExporter(binder).export(ExtendedHiveMetastore.class)
                .as(generatedNameOf(CachingHiveMetastore.class, connectorId));
    }
}
