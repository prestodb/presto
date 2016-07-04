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
package com.facebook.presto.hive.metastore.file;

import com.facebook.presto.hive.ForCachingHiveMetastore;
import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class FileMetastoreModule
        implements Module
{
    private final String connectorId;

    public FileMetastoreModule(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(FileHiveMetastoreConfig.class).in(Scopes.SINGLETON);
        binder.bind(ExtendedHiveMetastore.class).annotatedWith(ForCachingHiveMetastore.class).to(FileHiveMetastore.class).in(Scopes.SINGLETON);
        binder.bind(ExtendedHiveMetastore.class).to(CachingHiveMetastore.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ExtendedHiveMetastore.class)
                .as(generatedNameOf(CachingHiveMetastore.class, connectorId));
    }
}
