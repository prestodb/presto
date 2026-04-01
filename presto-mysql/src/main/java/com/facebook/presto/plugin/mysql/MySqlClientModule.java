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
package com.facebook.presto.plugin.mysql;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.DefaultTableLocationProvider;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcConnector;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcMetadataCache;
import com.facebook.presto.plugin.jdbc.JdbcMetadataCacheStats;
import com.facebook.presto.plugin.jdbc.JdbcMetadataConfig;
import com.facebook.presto.plugin.jdbc.JdbcMetadataFactory;
import com.facebook.presto.plugin.jdbc.JdbcPageSinkProvider;
import com.facebook.presto.plugin.jdbc.JdbcRecordSetProvider;
import com.facebook.presto.plugin.jdbc.JdbcSessionPropertiesProvider;
import com.facebook.presto.plugin.jdbc.JdbcSplitManager;
import com.facebook.presto.plugin.jdbc.TableLocationProvider;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.util.StringUtils;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class MySqlClientModule
        extends AbstractConfigurationAwareModule
{
    private final String connectorId;

    public MySqlClientModule(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        newOptionalBinder(binder, ConnectorAccessControl.class);
        newSetBinder(binder, Procedure.class);
        binder.bind(JdbcConnectorId.class).toInstance(new JdbcConnectorId(connectorId));

        binder.bind(JdbcMetadataCache.class).in(Scopes.SINGLETON);
        binder.bind(JdbcMetadataCacheStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(JdbcMetadataCacheStats.class).as(generatedNameOf(JdbcMetadataCacheStats.class, connectorId));

        binder.bind(JdbcMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(JdbcSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(JdbcPageSinkProvider.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, JdbcSessionPropertiesProvider.class);
        binder.bind(JdbcConnector.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(JdbcMetadataConfig.class);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        binder.bind(TableLocationProvider.class).to(DefaultTableLocationProvider.class).in(Scopes.SINGLETON);
        binder.bind(MySqlMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).to(MySqlClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfigDefaults(BaseJdbcConfig.class, baseJdbcConfig -> {
            baseJdbcConfig.setlistSchemasIgnoredSchemas("information_schema,mysql");
        });
        ensureCatalogIsEmpty(buildConfigObject(BaseJdbcConfig.class).getConnectionUrl());
        configBinder(binder).bindConfig(MySqlConfig.class);
    }

    private static void ensureCatalogIsEmpty(String connectionUrl)
    {
        checkArgument(ConnectionUrlParser.isConnectionStringSupported(connectionUrl), "Invalid JDBC URL for MySQL connector");
        checkArgument(StringUtils.isNullOrEmpty(ConnectionUrl.getConnectionUrlInstance(connectionUrl, null).getDatabase()), "Database (catalog) must not be specified in JDBC URL for MySQL connector");
    }
}
