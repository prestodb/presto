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
package io.prestosql.plugin.hive.metastore;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.hive.metastore.file.FileMetastoreModule;
import io.prestosql.plugin.hive.metastore.glue.GlueMetastoreModule;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreModule;

import java.util.Optional;

import static io.airlift.configuration.ConditionalModule.installModuleIf;

public class HiveMetastoreModule
        extends AbstractConfigurationAwareModule
{
    private final Optional<ExtendedHiveMetastore> metastore;

    public HiveMetastoreModule(Optional<ExtendedHiveMetastore> metastore)
    {
        this.metastore = metastore;
    }

    @Override
    protected void setup(Binder binder)
    {
        if (metastore.isPresent()) {
            binder.bind(ExtendedHiveMetastore.class).toInstance(metastore.get());
        }
        else {
            bindMetastoreModule("thrift", new ThriftMetastoreModule());
            bindMetastoreModule("file", new FileMetastoreModule());
            bindMetastoreModule("glue", new GlueMetastoreModule());
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
