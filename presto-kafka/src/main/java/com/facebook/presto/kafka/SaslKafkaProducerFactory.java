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
import com.facebook.presto.spi.HostAddress;
import com.google.inject.Inject;

import java.util.List;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class SaslKafkaProducerFactory
        implements KafkaProducerFactory
{
    private final KafkaSaslConfig saslConfig;
    private final KafkaProducerFactory delegate;

    @Inject
    public SaslKafkaProducerFactory(@ForKafkaSasl KafkaProducerFactory delegate, KafkaSaslConfig saslConfig)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.saslConfig = requireNonNull(saslConfig, "saslConfig is null");
    }

    @Override
    public Properties configure(List<HostAddress> bootstrapServers)
    {
        Properties properties = new Properties();
        properties.putAll(delegate.configure(bootstrapServers));
        properties.putAll(saslConfig.getKafkaSaslProperties());
        return properties;
    }
}
