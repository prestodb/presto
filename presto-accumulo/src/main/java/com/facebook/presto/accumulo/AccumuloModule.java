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
package com.facebook.presto.accumulo;

import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.conf.AccumuloSessionProperties;
import com.facebook.presto.accumulo.conf.AccumuloTableProperties;
import com.facebook.presto.accumulo.io.AccumuloPageSinkProvider;
import com.facebook.presto.accumulo.io.AccumuloRecordSetProvider;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.metadata.ZooKeeperMetadataManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.log4j.JulAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;

import javax.inject.Inject;
import javax.inject.Provider;

import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Presto module to do all kinds of run Guice injection stuff!
 * <p>
 * WARNING: Contains black magick
 */
public class AccumuloModule
        implements Module
{
    private final String connectorId;
    private final TypeManager typeManager;

    public AccumuloModule(
            String connectorId,
            TypeManager typeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        // Add appender to Log4J root logger
        JulAppender appender = new JulAppender(); //create appender
        appender.setLayout(new PatternLayout("%d %-5p %c - %m%n"));
        appender.setThreshold(Level.INFO);
        appender.activateOptions();
        org.apache.log4j.Logger.getRootLogger().addAppender(appender);

        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(AccumuloConnector.class).in(Scopes.SINGLETON);
        binder.bind(AccumuloConnectorId.class).toInstance(new AccumuloConnectorId(connectorId));
        binder.bind(AccumuloMetadata.class).in(Scopes.SINGLETON);
        binder.bind(AccumuloMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(AccumuloClient.class).in(Scopes.SINGLETON);
        binder.bind(AccumuloSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(AccumuloRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(AccumuloPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(AccumuloHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(AccumuloSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(AccumuloTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(ZooKeeperMetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(AccumuloTableManager.class).in(Scopes.SINGLETON);
        binder.bind(Connector.class).toProvider(ConnectorProvider.class);

        configBinder(binder).bindConfig(AccumuloConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, JsonCodec.listJsonCodec(AccumuloTable.class));
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
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

    private static class ConnectorProvider
            implements Provider<Connector>
    {
        private static final Logger LOG = Logger.get(ConnectorProvider.class);

        private final String instance;
        private final String zooKeepers;
        private final String username;
        private final String password;

        @Inject
        public ConnectorProvider(AccumuloConfig config)
        {
            requireNonNull(config, "config is null");
            this.instance = config.getInstance();
            this.zooKeepers = config.getZooKeepers();
            this.username = config.getUsername();
            this.password = config.getPassword();
        }

        @Override
        public Connector get()
        {
            try {
                Instance inst = new ZooKeeperInstance(instance, zooKeepers);
                Connector connector = inst.getConnector(username, new PasswordToken(password.getBytes(UTF_8)));
                LOG.info("Connection to instance %s at %s established, user %s", instance, zooKeepers, username);
                return connector;
            }
            catch (AccumuloException | AccumuloSecurityException e) {
                throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to get connector to Accumulo", e);
            }
        }
    }
}
