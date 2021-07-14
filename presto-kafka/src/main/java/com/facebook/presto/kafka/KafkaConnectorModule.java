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
package com.facebook.presto.kafka;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.decoder.DecoderModule;
import com.facebook.presto.kafka.encoder.EncoderModule;
import com.facebook.presto.kafka.schema.file.FileTableDescriptionSupplier;
import com.facebook.presto.kafka.schema.file.FileTableDescriptionSupplierModule;
import com.facebook.presto.kafka.server.file.FileKafkaClusterMetadataSupplier;
import com.facebook.presto.kafka.server.file.FileKafkaClusterMetadataSupplierModule;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import javax.inject.Inject;

import java.util.function.Function;

import static com.facebook.airlift.configuration.ConditionalModule.installModuleIf;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.util.Objects.requireNonNull;

/**
 * Guice module for the Apache Kafka connector.
 */
public class KafkaConnectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.bind(KafkaConnector.class).in(Scopes.SINGLETON);

        binder.bind(KafkaMetadata.class).in(Scopes.SINGLETON);
        binder.bind(KafkaSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(KafkaRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(KafkaPageSinkProvider.class).in(Scopes.SINGLETON);

        binder.bind(KafkaConsumerManager.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(KafkaConnectorConfig.class);
        bindTopicSchemaProviderModule(FileTableDescriptionSupplier.NAME, new FileTableDescriptionSupplierModule(), KafkaConnectorConfig::getTableDescriptionSupplier);
        bindTopicSchemaProviderModule(FileKafkaClusterMetadataSupplier.NAME, new FileKafkaClusterMetadataSupplierModule(), KafkaConnectorConfig::getClusterMetadataSupplier);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindJsonCodec(KafkaTopicDescription.class);

        binder.install(new DecoderModule());
        binder.install(new EncoderModule());
        binder.install(new KafkaProducerModule());
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
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            return typeManager.getType(parseTypeSignature(value));
        }
    }

    public void bindTopicSchemaProviderModule(String name, Module module, Function<KafkaConnectorConfig, String> configSupplier)
    {
        install(installModuleIf(
                KafkaConnectorConfig.class,
                kafkaConfig -> name.equalsIgnoreCase(configSupplier.apply(kafkaConfig)),
                module));
    }
}
