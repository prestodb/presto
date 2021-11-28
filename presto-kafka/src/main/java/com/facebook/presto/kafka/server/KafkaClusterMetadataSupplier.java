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
package com.facebook.presto.kafka.server;

import com.facebook.presto.spi.HostAddress;

import java.util.List;

/**
 * This is mainly used to get Kafka cluster metadata such as broker list so that Kafka Connector can communicate with Kafka cluster
 */
public interface KafkaClusterMetadataSupplier
{
    /**
     * Gets kafka broker list for specified kafka cluster name
     * @param clusterName the kafka cluster name
     * @return kafka broker list
     */
    List<HostAddress> getNodes(String clusterName);
}
