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
package com.facebook.presto.kafka.server.file;

import com.facebook.airlift.configuration.Config;
import com.facebook.presto.spi.HostAddress;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import java.util.List;

import static com.google.common.collect.Iterables.transform;

public class FileKafkaClusterMetadataSupplierConfig
{
    private static final int KAFKA_DEFAULT_PORT = 9092;

    /**
     * Seed nodes for Kafka cluster. At least one must exist.
     */
    private List<HostAddress> nodes;

    public List<HostAddress> getNodes()
    {
        return nodes;
    }

    @Config("kafka.nodes")
    public FileKafkaClusterMetadataSupplierConfig setNodes(String nodes)
    {
        this.nodes = (nodes == null) ? null : parseNodes(nodes).asList();
        return this;
    }

    public static ImmutableSet<HostAddress> parseNodes(String nodes)
    {
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
        return ImmutableSet.copyOf(transform(splitter.split(nodes), FileKafkaClusterMetadataSupplierConfig::toHostAddress));
    }

    private static HostAddress toHostAddress(String value)
    {
        return HostAddress.fromString(value).withDefaultPort(KAFKA_DEFAULT_PORT);
    }
}
