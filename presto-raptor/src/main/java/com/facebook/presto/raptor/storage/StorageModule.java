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
package com.facebook.presto.raptor.storage;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.dbpool.H2EmbeddedDataSource;
import io.airlift.dbpool.H2EmbeddedDataSourceConfig;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

import javax.inject.Singleton;

import java.io.File;

import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class StorageModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        bindConfig(binder).to(DatabaseLocalStorageManagerConfig.class);
        binder.bind(LocalStorageManager.class).to(DatabaseLocalStorageManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(LocalStorageManager.class).withGeneratedName();

        // TODO: figure out how to add this dynamically
        binder.bind(ShardResource.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForLocalStorageManager
    public IDBI createLocalStorageManagerDBI(DatabaseLocalStorageManagerConfig config)
            throws Exception
    {
        return new DBI(new H2EmbeddedDataSource(new H2EmbeddedDataSourceConfig()
                .setFilename(new File(config.getDataDirectory(), "db/StorageManager").getAbsolutePath())
                .setMaxConnections(500)
                .setMaxConnectionWait(new Duration(1, SECONDS))));
    }
}
