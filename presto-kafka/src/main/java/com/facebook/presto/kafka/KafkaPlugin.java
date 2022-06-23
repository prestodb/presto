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

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import com.google.inject.util.Modules;

import static java.util.Objects.requireNonNull;

/**
 * Presto plugin to use Apache Kafka as a data source.
 */
public class KafkaPlugin
        implements Plugin
{
    private final Module extension;

    public KafkaPlugin()
    {
        this(Modules.EMPTY_MODULE);
    }

    public KafkaPlugin(Module extension)
    {
        this.extension = requireNonNull(extension, "extension is null");
    }

    @Override
    public synchronized Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new KafkaConnectorFactory(extension));
    }
}
