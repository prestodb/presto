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
package com.facebook.presto.elasticsearch;

import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.elasticsearch.RetryDriver.retry;
import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION;
import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH;
import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD;
import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMCERT_FILEPATH;
import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_FILEPATH;
import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_PASSWORD;
import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMTRUSTEDCAS_FILEPATH;
import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH;
import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_PASSWORD;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class ElasticsearchClient
{
    private final TransportClient client;
    private final Duration requestTimeout;
    private final int maxAttempts;
    private final Duration maxRetryTime;

    @Inject
    public ElasticsearchClient(ElasticsearchConfig config)
            throws IOException
    {
        requireNonNull(config, "config is null");
        requestTimeout = config.getRequestTimeout();
        maxAttempts = config.getMaxRequestRetries();
        maxRetryTime = config.getMaxRetryTime();

        TransportAddress address = new TransportAddress(InetAddress.getByName(config.getHost()), config.getPort());
        client = createTransportClient(config, address, Optional.of(config.getClusterName()));
    }

    @PreDestroy
    public void tearDown()
    {
        client.close();
    }

    public List<Shard> getSearchShards(String index)
    {
        try {
            ClusterSearchShardsResponse result = retry()
                    .maxAttempts(maxAttempts)
                    .exponentialBackoff(maxRetryTime)
                    .run("getSearchShardsResponse", () -> client.admin()
                            .cluster()
                            .searchShards(new ClusterSearchShardsRequest(index))
                            .actionGet(requestTimeout.toMillis()));

            ImmutableList.Builder<Shard> shards = ImmutableList.builder();
            DiscoveryNode[] nodes = result.getNodes();
            Map<String, DiscoveryNode> nodeById = Arrays.stream(nodes)
                    .collect(Collectors.toMap(DiscoveryNode::getId, node -> node));

            for (ClusterSearchShardsGroup group : result.getGroups()) {
                Optional<ShardRouting> routing = Arrays.stream(group.getShards())
                        .filter(ShardRouting::assignedToNode)
                        .sorted(this::shardPreference)
                        .findFirst();

                DiscoveryNode node;
                if (routing.isPresent()) {
                    node = nodeById.get(routing.get().currentNodeId());
                }
                else {
                    // pick an arbitrary node
                    node = nodes[group.getShardId().getId() % nodes.length];
                }

                shards.add(new Shard(group.getShardId().getId(), node.getHostName(), node.getAddress().getPort()));
            }
            return shards.build();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int shardPreference(ShardRouting left, ShardRouting right)
    {
        // Favor non-primary shards
        if (left.primary() == right.primary()) {
            return 0;
        }
        return left.primary() ? 1 : -1;
    }

    public ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> getMappings(String index, String type)
    {
        try {
            GetMappingsRequest request = new GetMappingsRequest().types(type);
            if (!isNullOrEmpty(index)) {
                request.indices(index);
            }

            return retry()
                    .maxAttempts(maxAttempts)
                    .exponentialBackoff(maxRetryTime)
                    .run("getMappings", () -> client.admin()
                            .indices()
                            .getMappings(request)
                            .actionGet(requestTimeout.toMillis())
                            .getMappings());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static TransportClient createTransportClient(ElasticsearchConfig config, TransportAddress address)
    {
        return createTransportClient(config, address, Optional.empty());
    }

    static TransportClient createTransportClient(ElasticsearchConfig config, TransportAddress address, Optional<String> clusterName)
    {
        Settings settings;
        Builder builder;
        TransportClient client;
        if (clusterName.isPresent()) {
            builder = Settings.builder()
                    .put("cluster.name", clusterName.get());
        }
        else {
            builder = Settings.builder()
                    .put("client.transport.ignore_cluster_name", true);
        }
        switch (config.getCertificateFormat()) {
            case PEM:
                settings = builder
                        .put(SEARCHGUARD_SSL_TRANSPORT_PEMCERT_FILEPATH, config.getPemcertFilepath())
                        .put(SEARCHGUARD_SSL_TRANSPORT_PEMKEY_FILEPATH, config.getPemkeyFilepath())
                        .put(SEARCHGUARD_SSL_TRANSPORT_PEMKEY_PASSWORD, config.getPemkeyPassword())
                        .put(SEARCHGUARD_SSL_TRANSPORT_PEMTRUSTEDCAS_FILEPATH, config.getPemtrustedcasFilepath())
                        .put(SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, false)
                        .build();
                client = new PreBuiltTransportClient(settings, SearchGuardSSLPlugin.class).addTransportAddress(address);
                break;
            case JKS:
                settings = Settings.builder()
                        .put(SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH, config.getKeystoreFilepath())
                        .put(SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH, config.getTruststoreFilepath())
                        .put(SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD, config.getKeystorePassword())
                        .put(SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_PASSWORD, config.getTruststorePassword())
                        .put(SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, false)
                        .build();
                client = new PreBuiltTransportClient(settings, SearchGuardSSLPlugin.class).addTransportAddress(address);
                break;
            default:
                settings = builder.build();
                client = new PreBuiltTransportClient(settings).addTransportAddress(address);
                break;
        }
        return client;
    }
}
