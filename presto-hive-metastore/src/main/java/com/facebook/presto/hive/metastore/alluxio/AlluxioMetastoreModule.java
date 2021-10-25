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
package com.facebook.presto.hive.metastore.alluxio;

import alluxio.ClientContext;
import alluxio.client.table.RetryHandlingTableMasterClient;
import alluxio.client.table.TableMasterClient;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterClientContext;
import alluxio.util.ConfigurationUtils;
import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.HiveMetastore;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

/**
 * Module for an Alluxio metastore implementation of the {@link HiveMetastore} interface.
 */
public class AlluxioMetastoreModule
         extends AbstractConfigurationAwareModule
{
    public AlluxioMetastoreModule()
    {
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(AlluxioHiveMetastoreConfig.class);

        binder.bind(ExtendedHiveMetastore.class).to(AlluxioHiveMetastore.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ExtendedHiveMetastore.class).as(generatedNameOf(AlluxioHiveMetastore.class));
    }

    @Provides
    @Inject
    TableMasterClient provideCatalogMasterClient(AlluxioHiveMetastoreConfig config)
    {
        InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
        if (config.isZookeeperEnabled()) {
            conf.set(PropertyKey.ZOOKEEPER_ENABLED, true);
            conf.set(PropertyKey.ZOOKEEPER_ADDRESS, config.getZookeeperAddress());
        }
        else {
            String address = config.getMasterAddress();
            String[] parts = address.split(":", 2);
            conf.set(PropertyKey.MASTER_HOSTNAME, parts[0]);
            if (parts.length > 1) {
                conf.set(PropertyKey.MASTER_RPC_PORT, parts[1]);
            }
        }
        MasterClientContext context = MasterClientContext.newBuilder(ClientContext.create(conf)).build();
        return new RetryHandlingTableMasterClient(context);
    }
}
