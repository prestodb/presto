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
package com.facebook.presto.connector.system;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.SystemTable;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

import static com.google.inject.multibindings.MapBinder.newMapBinder;

public class SystemTablesModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        newMapBinder(binder, String.class, Connector.class).addBinding("system").to(SystemConnector.class);
        binder.bind(SystemTablesManager.class).in(Scopes.SINGLETON);
        binder.bind(SystemTablesMetadata.class).in(Scopes.SINGLETON);
        binder.bind(SystemSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(SystemDataStreamProvider.class).in(Scopes.SINGLETON);
        Multibinder<SystemTable> globalTableBinder = Multibinder.newSetBinder(binder, SystemTable.class);
        globalTableBinder.addBinding().to(NodesSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(QuerySystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(TaskSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(AliasSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(CatalogSystemTable.class).in(Scopes.SINGLETON);
    }
}
