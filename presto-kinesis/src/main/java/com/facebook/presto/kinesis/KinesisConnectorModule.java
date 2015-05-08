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
package com.facebook.presto.kinesis;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

import javax.inject.Inject;

import com.facebook.presto.kinesis.decoder.KinesisDecoderModule;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

public class KinesisConnectorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(KinesisConnector.class).in(Scopes.SINGLETON);

        binder.bind(KinesisHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(KinesisMetadata.class).in(Scopes.SINGLETON);
        binder.bind(KinesisSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(KinesisRecordSetProvider.class).in(Scopes.SINGLETON);

        binder.bind(KinesisClientManager.class).in(Scopes.SINGLETON);

        bindConfig(binder).to(KinesisConnectorConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindJsonCodec(KinesisStreamDescription.class);

        binder.install(new KinesisDecoderModule());

        for (KinesisInternalFieldDescription internalFieldDescription : KinesisInternalFieldDescription.getInternalFields()) {
            bindInternalColumn(binder, internalFieldDescription);
        }
    }

    private static void bindInternalColumn(Binder binder, KinesisInternalFieldDescription fieldDescription)
    {
        Multibinder<KinesisInternalFieldDescription> fieldDescriptionBinder = Multibinder.newSetBinder(binder, KinesisInternalFieldDescription.class);
        fieldDescriptionBinder.addBinding().toInstance(fieldDescription);
    }

    public static final class TypeDeserializer
        extends FromStringDeserializer<Type>
    {
        private static final long serialVersionUID = 1L;

        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = checkNotNull(typeManager, "typeManager is null");
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
