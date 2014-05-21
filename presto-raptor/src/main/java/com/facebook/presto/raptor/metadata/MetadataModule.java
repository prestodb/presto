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
package com.facebook.presto.raptor.metadata;

import com.facebook.presto.guice.AbstractConfigurationAwareModule;
import com.facebook.presto.raptor.RaptorPartitionKey;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.dbpool.H2EmbeddedDataSourceModule;
import io.airlift.dbpool.MySqlDataSourceModule;

import java.lang.annotation.Annotation;

import static com.facebook.presto.guice.ConditionalModule.installIfPropertyEquals;
import static com.facebook.presto.raptor.util.DbiProvider.bindDbiToDataSource;
import static com.facebook.presto.raptor.util.DbiProvider.bindResultSetMapper;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public class MetadataModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(ShardManager.class).to(DatabaseShardManager.class).in(Scopes.SINGLETON);

        bindDataSource(binder, "metadata", ForMetadata.class, ForShardManager.class);
        bindResultSetMapper(binder, TableColumn.Mapper.class, ForMetadata.class);
        bindResultSetMapper(binder, RaptorPartitionKey.Mapper.class, ForShardManager.class);

        bindConfig(binder).to(ShardCleanerConfig.class);
        binder.bind(ShardCleaner.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("raptor-shard-cleaner", ForShardCleaner.class);
    }

    @SafeVarargs
    private final void bindDataSource(Binder binder, String type, Class<? extends Annotation> annotation, Class<? extends Annotation>... aliases)
    {
        String property = type + ".db.type";
        install(installIfPropertyEquals(new MySqlDataSourceModule(type, annotation, aliases), property, "mysql"));
        install(installIfPropertyEquals(new H2EmbeddedDataSourceModule(type, annotation, aliases), property, "h2"));

        bindDbiToDataSource(binder, annotation);
        for (Class<? extends Annotation> alias : aliases) {
            bindDbiToDataSource(binder, alias);
        }
    }
}
