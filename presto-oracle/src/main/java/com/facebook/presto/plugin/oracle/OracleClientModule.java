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
package com.facebook.presto.plugin.oracle;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DefaultTableLocationProvider;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
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
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import oracle.jdbc.OracleDriver;

import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.util.Objects.requireNonNull;
import static oracle.jdbc.OracleConnection.CONNECTION_PROPERTY_INCLUDE_SYNONYMS;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class OracleClientModule
        extends AbstractConfigurationAwareModule
{
    private final String connectorId;

    public OracleClientModule(String connectorId)
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

        binder.bind(JdbcMetadataFactory.class).to(OracleMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(JdbcSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(JdbcPageSinkProvider.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, JdbcSessionPropertiesProvider.class);
        binder.bind(JdbcConnector.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(JdbcMetadataConfig.class);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        binder.bind(TableLocationProvider.class).to(DefaultTableLocationProvider.class).in(Scopes.SINGLETON);

        binder.bind(OracleClient.class).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).to(OracleClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(OracleConfig.class);
    }

    @Provides
    @Singleton
    public static ConnectionFactory connectionFactory(BaseJdbcConfig config, OracleConfig oracleConfig)
            throws SQLException
    {
        Properties connectionProperties = new Properties();

        requireNonNull(oracleConfig, "oracle config is null");
        requireNonNull(config, "BaseJdbc config is null");

        if (config.getConnectionUser() != null && config.getConnectionPassword() != null) {
            connectionProperties.setProperty("user", config.getConnectionUser());
            connectionProperties.setProperty("password", config.getConnectionPassword());
        }

        if (oracleConfig.isTlsEnabled()) {
            requireNonNull(oracleConfig.getTrustStorePath(), "oracle.tls.truststore-path is null");
            connectionProperties.setProperty("javax.net.ssl.trustStore", oracleConfig.getTrustStorePath());
            connectionProperties.setProperty("javax.net.ssl.trustStorePassword", oracleConfig.getTruststorePassword());
        }

        connectionProperties.setProperty(CONNECTION_PROPERTY_INCLUDE_SYNONYMS, String.valueOf(oracleConfig.isSynonymsEnabled()));

        return new DriverConnectionFactory(
                new OracleDriver(),
                config.getConnectionUrl(),
                Optional.empty(),
                Optional.empty(),
                connectionProperties);
    }
}
