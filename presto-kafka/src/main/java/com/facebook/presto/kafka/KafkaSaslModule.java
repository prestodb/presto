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

import com.facebook.presto.kafka.security.ForKafkaSasl;
import com.facebook.presto.kafka.security.KafkaSaslConfig;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;

public class KafkaSaslModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(KafkaSaslConfig.class);
        binder.bind(KafkaProducerFactory.class).annotatedWith(ForKafkaSasl.class).to(PlainTextKafkaProducerFactory.class).in(Scopes.SINGLETON);
        binder.bind(KafkaProducerFactory.class).to(SaslKafkaProducerFactory.class).in(Scopes.SINGLETON);
        binder.bind(KafkaConsumerManager.class).annotatedWith(ForKafkaSasl.class).to(PlainTextKafkaConsumerManager.class).in(Scopes.SINGLETON);
        binder.bind(KafkaConsumerManager.class).to(SaslKafkaConsumerManager.class).in(Scopes.SINGLETON);
    }
}
