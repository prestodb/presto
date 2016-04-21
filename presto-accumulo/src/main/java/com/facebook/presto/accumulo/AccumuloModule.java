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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
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

    public AccumuloModule(String connectorId, TypeManager typeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
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

        configBinder(binder).bindConfig(AccumuloConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, JsonCodec.listJsonCodec(AccumuloTable.class));
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private static final long serialVersionUID = -3547534717872348558L;
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
