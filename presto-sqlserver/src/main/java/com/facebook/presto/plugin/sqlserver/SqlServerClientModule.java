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
package com.facebook.presto.plugin.sqlserver;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcConnector;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcMetadataConfig;
import com.facebook.presto.plugin.jdbc.JdbcMetadataFactory;
import com.facebook.presto.plugin.jdbc.JdbcRecordSetProvider;
import com.facebook.presto.plugin.jdbc.JdbcRecordSinkProvider;
import com.facebook.presto.plugin.jdbc.JdbcSplitManager;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class SqlServerClientModule
        implements Module
{
    private final String connectorId;

    public SqlServerClientModule(String connectorId)
    {
        checkArgument(!isNullOrEmpty(connectorId), "connectorId is null or empty");
        this.connectorId = connectorId;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(JdbcConnectorId.class).toInstance(new JdbcConnectorId(connectorId));
        binder.bind(JdbcMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(JdbcSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(JdbcRecordSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(JdbcConnector.class).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).to(SqlServerClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(JdbcMetadataConfig.class);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
    }
}
