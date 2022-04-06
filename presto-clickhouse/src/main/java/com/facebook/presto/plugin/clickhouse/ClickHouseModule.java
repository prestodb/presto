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
package com.facebook.presto.plugin.clickhouse;

import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import ru.yandex.clickhouse.ClickHouseDriver;

import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.util.Objects.requireNonNull;

public class ClickHouseModule
        implements Module
{
    private final String connectorId;

    public ClickHouseModule(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
    }

    @Override
    public void configure(Binder binder)
    {
        newOptionalBinder(binder, ConnectorAccessControl.class);
        newSetBinder(binder, Procedure.class);
        binder.bind(ClickHouseConnectorId.class).toInstance(new ClickHouseConnectorId(connectorId));
        binder.bind(ClickHouseMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(ClickHouseClient.class).in(Scopes.SINGLETON);
        binder.bind(ClickHouseSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ClickHouseRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ClickHousePageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ClickHouseConnector.class).in(Scopes.SINGLETON);
        bindTablePropertiesProvider(binder, ClickHouseTableProperties.class);
        configBinder(binder).bindConfig(ClickHouseConfig.class);
    }

    @Provides
    @Singleton
    public static ConnectionFactory connectionFactory(ClickHouseConfig clickhouseConfig)
            throws SQLException
    {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("useInformationSchema", "true");
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("tinyInt1isBit", "false");

        return new DriverConnectionFactory(
                new ClickHouseDriver(),
                clickhouseConfig.getConnectionUrl(),
                Optional.ofNullable(clickhouseConfig.getUserCredential()),
                Optional.ofNullable(clickhouseConfig.getPasswordCredential()),
                connectionProperties);
    }

    public static Multibinder<TablePropertiesProvider> tablePropertiesProviderBinder(Binder binder)
    {
        return newSetBinder(binder, TablePropertiesProvider.class);
    }

    public static void bindTablePropertiesProvider(Binder binder, Class<? extends TablePropertiesProvider> type)
    {
        tablePropertiesProviderBinder(binder).addBinding().to(type).in(Scopes.SINGLETON);
    }
}
