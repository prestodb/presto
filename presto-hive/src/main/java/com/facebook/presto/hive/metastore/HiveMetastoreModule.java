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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.hive.metastore.file.FileMetastoreModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import java.util.Optional;

import static io.airlift.configuration.ConditionalModule.installModuleIf;

public class HiveMetastoreModule
        extends AbstractConfigurationAwareModule
{
    private final String connectorId;
    private final Optional<ExtendedHiveMetastore> metastore;

    public HiveMetastoreModule(String connectorId, Optional<ExtendedHiveMetastore> metastore)
    {
        this.connectorId = connectorId;
        this.metastore = metastore;
    }

    @Override
    protected void setup(Binder binder)
    {
        if (metastore.isPresent()) {
            binder.bind(ExtendedHiveMetastore.class).toInstance(metastore.get());
        }
        else {
            bindMetastoreModule("thrift", new ThriftMetastoreModule(connectorId));
            bindMetastoreModule("file", new FileMetastoreModule(connectorId));
        }
    }

    private void bindMetastoreModule(String name, Module module)
    {
        install(installModuleIf(
                MetastoreConfig.class,
                metastore -> name.equalsIgnoreCase(metastore.getMetastoreType()),
                module));
    }
}
