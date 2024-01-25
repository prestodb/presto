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
package com.facebook.presto.iceberg;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.PartitionMutator;
import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HiveMetastoreCacheStats;
import com.facebook.presto.hive.metastore.HiveMetastoreModule;
import com.facebook.presto.hive.metastore.HivePartitionMutator;
import com.facebook.presto.hive.metastore.MetastoreCacheStats;
import com.facebook.presto.hive.metastore.MetastoreConfig;
import com.google.inject.Binder;
import com.google.inject.Scopes;

import java.util.Optional;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class IcebergHiveModule
        extends AbstractConfigurationAwareModule
{
    private final String connectorId;
    private final Optional<ExtendedHiveMetastore> metastore;

    public IcebergHiveModule(String connectorId, Optional<ExtendedHiveMetastore> metastore)
    {
        this.connectorId = connectorId;
        this.metastore = metastore;
    }

    @Override
    public void setup(Binder binder)
    {
        install(new HiveMetastoreModule(this.connectorId, this.metastore));
        binder.bind(ExtendedHiveMetastore.class).to(CachingHiveMetastore.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(MetastoreClientConfig.class);
        binder.bind(PartitionMutator.class).to(HivePartitionMutator.class).in(Scopes.SINGLETON);

        binder.bind(MetastoreCacheStats.class).to(HiveMetastoreCacheStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(MetastoreCacheStats.class).as(generatedNameOf(MetastoreCacheStats.class, connectorId));

        binder.bind(IcebergMetadataFactory.class).to(IcebergHiveMetadataFactory.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(MetastoreConfig.class);
    }
}
