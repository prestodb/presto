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
package com.facebook.presto.tablestore;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import javax.inject.Singleton;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class TablestoreModule
        implements Module
{
    private final String connectorId;
    private final TablestoreSessionProperties tablestoreSessionProperties = new TablestoreSessionProperties();

    public TablestoreModule(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(TablestoreConnectorId.class).toInstance(new TablestoreConnectorId(connectorId));
        binder.bind(TablestoreMetadata.class).in(Scopes.SINGLETON);
        binder.bind(TablestoreSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(TablestoreRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(TablestoreRecordSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(TablestoreHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(TablestoreConnector.class).in(Scopes.SINGLETON);
        binder.bind(TablestoreSessionProperties.class).toInstance(tablestoreSessionProperties);
        configBinder(binder).bindConfig(TablestoreConfig.class);
    }

    @Singleton
    @Provides
    public TablestoreFacade buildTablestoreFacade(TablestoreConfig config)
    {
        return new TablestoreFacade(config);
    }
}
