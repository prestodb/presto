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
package com.facebook.presto.hive.metastore.glue;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.presto.hive.ForCachingHiveMetastore;
import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

import java.util.concurrent.Executor;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class GlueMetastoreModule
        implements Module
{
    private final String connectorId;

    public GlueMetastoreModule(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(GlueHiveMetastoreConfig.class);
        binder.bind(GlueHiveMetastore.class).in(Scopes.SINGLETON);
        binder.bind(ExtendedHiveMetastore.class).annotatedWith(ForCachingHiveMetastore.class).to(GlueHiveMetastore.class).in(Scopes.SINGLETON);
        binder.bind(ExtendedHiveMetastore.class).to(CachingHiveMetastore.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ExtendedHiveMetastore.class)
                .as(generatedNameOf(CachingHiveMetastore.class, connectorId));
        newExporter(binder).export(GlueHiveMetastore.class)
                .as(generatedNameOf(GlueHiveMetastore.class, connectorId));
    }

    @Provides
    @Singleton
    @ForGlueHiveMetastore
    public Executor createExecutor(GlueHiveMetastoreConfig hiveConfig)
    {
        if (hiveConfig.getGetPartitionThreads() == 1) {
            return directExecutor();
        }
        return new BoundedExecutor(
            newCachedThreadPool(daemonThreadsNamed("hive-glue-%s")),
            hiveConfig.getGetPartitionThreads());
    }
}
