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
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
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
import com.facebook.presto.plugin.jdbc.procedure.ExecuteProcedure;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.procedure.Procedure;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.util.StringUtils;
import jakarta.inject.Inject;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
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
        // Standard JDBC wiring — duplicated from JdbcModule because MySqlConnectorFactory
        // bootstraps only this module (no JdbcModule), avoiding the duplicate-binding conflict
        // that would arise if both JdbcModule and a connector module bound JdbcMetadataFactory.
        newOptionalBinder(binder, ConnectorAccessControl.class);
        newSetBinder(binder, Procedure.class).addBinding().toProvider(ExecuteProcedure.class).in(Scopes.SINGLETON);
        binder.bind(JdbcConnectorId.class).toInstance(new JdbcConnectorId(connectorId));
        binder.bind(JdbcMetadataCache.class).in(Scopes.SINGLETON);
        binder.bind(JdbcMetadataCacheStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(JdbcMetadataCacheStats.class).as(generatedNameOf(JdbcMetadataCacheStats.class, connectorId));
        binder.bind(JdbcMetadataFactory.class).to(MySqlMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(JdbcSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(JdbcPageSinkProvider.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, JdbcSessionPropertiesProvider.class);
        binder.bind(JdbcConnector.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(JdbcMetadataConfig.class);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        binder.bind(TableLocationProvider.class).to(DefaultTableLocationProvider.class).in(Scopes.SINGLETON);

        // MySQL-specific bindings
        binder.bind(MySqlClient.class).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).to(MySqlClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(MySqlConfig.class);
        configBinder(binder).bindConfigDefaults(BaseJdbcConfig.class, baseJdbcConfig -> {
            baseJdbcConfig.setlistSchemasIgnoredSchemas("information_schema,mysql");
        });
        ensureCatalogIsEmpty(buildConfigObject(BaseJdbcConfig.class).getConnectionUrl());

        // JSON codec required by MySqlClient for ViewDefinition serialization
        binder.install(new JsonModule());
        jsonCodecBinder(binder).bindJsonCodec(ViewDefinition.class);
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
    }

    private static void ensureCatalogIsEmpty(String connectionUrl)
    {
        checkArgument(ConnectionUrlParser.isConnectionStringSupported(connectionUrl), "Invalid JDBC URL for MySQL connector");
        checkArgument(StringUtils.isNullOrEmpty(ConnectionUrl.getConnectionUrlInstance(connectionUrl, null).getDatabase()), "Database (catalog) must not be specified in JDBC URL for MySQL connector");
    }

    private static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private static final long serialVersionUID = 1L;

        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = typeManager.getType(parseTypeSignature(value));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }
}
