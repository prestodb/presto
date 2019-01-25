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
package io.prestosql.plugin.hive.metastore.thrift;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.hive.ForCachingHiveMetastore;
import io.prestosql.plugin.hive.ForRecordingHiveMetastore;
import io.prestosql.plugin.hive.HiveClientConfig;
import io.prestosql.plugin.hive.metastore.CachingHiveMetastore;
import io.prestosql.plugin.hive.metastore.ExtendedHiveMetastore;
import io.prestosql.plugin.hive.metastore.RecordingHiveMetastore;
import io.prestosql.plugin.hive.metastore.WriteHiveMetastoreRecordingProcedure;
import io.prestosql.spi.procedure.Procedure;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ThriftMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(HiveMetastoreClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(HiveCluster.class).to(StaticHiveCluster.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(StaticMetastoreConfig.class);

        binder.bind(HiveMetastore.class).to(ThriftHiveMetastore.class).in(Scopes.SINGLETON);

        if (buildConfigObject(HiveClientConfig.class).getRecordingPath() != null) {
            binder.bind(ExtendedHiveMetastore.class)
                    .annotatedWith(ForRecordingHiveMetastore.class)
                    .to(BridgingHiveMetastore.class)
                    .in(Scopes.SINGLETON);
            binder.bind(ExtendedHiveMetastore.class)
                    .annotatedWith(ForCachingHiveMetastore.class)
                    .to(RecordingHiveMetastore.class)
                    .in(Scopes.SINGLETON);
            binder.bind(RecordingHiveMetastore.class).in(Scopes.SINGLETON);
            newExporter(binder).export(RecordingHiveMetastore.class).withGeneratedName();

            Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
            procedures.addBinding().toProvider(WriteHiveMetastoreRecordingProcedure.class).in(Scopes.SINGLETON);
        }
        else {
            binder.bind(ExtendedHiveMetastore.class)
                    .annotatedWith(ForCachingHiveMetastore.class)
                    .to(BridgingHiveMetastore.class)
                    .in(Scopes.SINGLETON);
        }

        binder.bind(ExtendedHiveMetastore.class).to(CachingHiveMetastore.class).in(Scopes.SINGLETON);
        newExporter(binder).export(HiveMetastore.class)
                .as(generator -> generator.generatedNameOf(ThriftHiveMetastore.class));
        newExporter(binder).export(ExtendedHiveMetastore.class)
                .as(generator -> generator.generatedNameOf(CachingHiveMetastore.class));
    }
}
