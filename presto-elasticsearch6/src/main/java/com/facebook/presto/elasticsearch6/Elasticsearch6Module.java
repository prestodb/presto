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
package com.facebook.presto.elasticsearch6;

import com.facebook.presto.elasticsearch.BaseClient;
import com.facebook.presto.elasticsearch.conf.ElasticsearchConfig;
import com.facebook.presto.spi.PrestoException;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import javax.inject.Provider;

import java.io.IOException;
import java.net.InetAddress;

import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.UNEXPECTED_ES_ERROR;
import static java.util.Objects.requireNonNull;

public class Elasticsearch6Module
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(BaseClient.class).to(Elasticsearch6Client.class);
        binder.bind(Client.class).toProvider(ConnectionProvider.class);
    }

    private static class ConnectionProvider
            implements Provider<Client>
    {
        private final String clusterName;
        private final String hosts;

        @Inject
        public ConnectionProvider(ElasticsearchConfig config)
        {
            requireNonNull(config, "config is null");
            this.clusterName = config.getClusterName();
            this.hosts = config.getElasticsearchHosts();
        }

        @Override
        public Client get()
        {
            try {
                Settings settings = Settings.builder().put("cluster.name", clusterName)
                        .put("client.transport.sniff", true).build();

                TransportClient client = new PreBuiltTransportClient(settings);
                for (String ip : hosts.split(",")) {
                    client.addTransportAddress(
                            new TransportAddress(InetAddress.getByName(ip.split(":")[0]),
                                    Integer.parseInt(ip.split(":")[1])));
                }
                return client;
            }
            catch (IOException e) {
                throw new PrestoException(UNEXPECTED_ES_ERROR, "Failed to get connection to Elasticsearch", e);
            }
        }
    }
}
