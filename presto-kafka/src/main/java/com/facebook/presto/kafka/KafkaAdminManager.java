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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.HostAddress;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;

import javax.inject.Inject;

import java.util.Properties;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;

public class KafkaAdminManager
{
    private static final Logger log = Logger.get(KafkaAdminManager.class);

    @Inject
    public KafkaAdminManager(KafkaConnectorConfig kafkaConnectorConfig)
    {
        requireNonNull(kafkaConnectorConfig, "kafkaConnectorConfig is null");
    }

    AdminClient createAdmin(HostAddress hostAddress)
    {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, hostAddress.toString());
        return KafkaAdminClient.create(properties);
    }
}
