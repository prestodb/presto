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

import com.facebook.presto.decoder.DecoderModule;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import javax.inject.Inject;

import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
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
        bindKafkaClusterModule();
        binder.bind(KafkaConnector.class).in(Scopes.SINGLETON);

        binder.bind(KafkaMetadata.class).in(Scopes.SINGLETON);
        binder.bind(KafkaSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(KafkaRecordSetProvider.class).in(Scopes.SINGLETON);

        binder.bind(KafkaConsumerManager.class).in(Scopes.SINGLETON);
        binder.bind(KafkaRowTypeParser.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(KafkaConnectorConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindJsonCodec(KafkaTopicDescription.class);

        binder.install(new DecoderModule());
    }

    private void bindKafkaClusterModule()
    {
        install(installModuleIf(
                KafkaConnectorConfig.class,
                zkKafkaConfig -> zkKafkaConfig.getDiscoveryMode().equals("static"),
                new KafkaStaticModule()));
        install(installModuleIf(
                KafkaConnectorConfig.class,
                zkKafkaConfig -> zkKafkaConfig.getDiscoveryMode().equals("zookeeper"),
                new KafkaZookeeperBasedModule()));
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private static final long serialVersionUID = 1L;

        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager, KafkaRowTypeParser nestedRowType)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            return KafkaRowTypeParser.getNestedRowType(value, typeManager);
        }
    }
}
