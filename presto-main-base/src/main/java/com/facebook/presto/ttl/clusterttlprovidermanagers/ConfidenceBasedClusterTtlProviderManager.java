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
package com.facebook.presto.ttl.clusterttlprovidermanagers;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ttl.ClusterTtlProvider;
import com.facebook.presto.spi.ttl.ClusterTtlProviderFactory;
import com.facebook.presto.spi.ttl.ConfidenceBasedTtlInfo;
import com.facebook.presto.ttl.nodettlfetchermanagers.NodeTtlFetcherManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ConfidenceBasedClusterTtlProviderManager
        implements ClusterTtlProviderManager
{
    private static final Logger log = Logger.get(ConfidenceBasedClusterTtlProviderManager.class);
    private static final File CLUSTER_TTL_PROVIDER_CONFIG = new File("etc/cluster-ttl-provider.properties");
    private static final String CLUSTER_TTL_PROVIDER_PROPERTY_NAME = "cluster-ttl-provider.factory";
    private final AtomicReference<ClusterTtlProvider> clusterTtlProvider = new AtomicReference<>();
    private final NodeTtlFetcherManager nodeTtlFetcherManager;
    private final Map<String, ClusterTtlProviderFactory> clusterTtlProviderFactories = new ConcurrentHashMap<>();

    @Inject
    public ConfidenceBasedClusterTtlProviderManager(NodeTtlFetcherManager nodeTtlFetcherManager)
    {
        this.nodeTtlFetcherManager = requireNonNull(nodeTtlFetcherManager, "nodeTtlFetcherManager is null");
    }

    @Override
    public ConfidenceBasedTtlInfo getClusterTtl()
    {
        return clusterTtlProvider.get().getClusterTtl(ImmutableList.copyOf(nodeTtlFetcherManager.getAllTtls().values()));
    }

    @Override
    public void addClusterTtlProviderFactory(ClusterTtlProviderFactory clusterTtlProviderFactory)
    {
        requireNonNull(clusterTtlProviderFactory, "clusterTtlProviderFactory is null");
        if (clusterTtlProviderFactories.putIfAbsent(clusterTtlProviderFactory.getName(), clusterTtlProviderFactory) != null) {
            throw new IllegalArgumentException(format("Query Prerequisites '%s' is already registered", clusterTtlProviderFactory.getName()));
        }
    }

    @Override
    public void loadClusterTtlProvider()
            throws Exception
    {
        if (CLUSTER_TTL_PROVIDER_CONFIG.exists()) {
            Map<String, String> properties = new HashMap<>(loadProperties(CLUSTER_TTL_PROVIDER_CONFIG));
            String factoryName = properties.remove(CLUSTER_TTL_PROVIDER_PROPERTY_NAME);

            checkArgument(!isNullOrEmpty(factoryName),
                    "Cluster Ttl Provider configuration %s does not contain %s", CLUSTER_TTL_PROVIDER_CONFIG.getAbsoluteFile(), CLUSTER_TTL_PROVIDER_PROPERTY_NAME);
            load(factoryName, properties);
        }
        else {
            load("infinite", ImmutableMap.of());
        }
    }

    @VisibleForTesting
    public void load(String factoryName, Map<String, String> properties)
    {
        log.info("-- Loading Cluster Ttl Provider factory --");

        ClusterTtlProviderFactory clusterTtlProviderFactory = clusterTtlProviderFactories.get(factoryName);
        checkState(clusterTtlProviderFactory != null, "Cluster Ttl Provider factory %s is not registered", factoryName);

        ClusterTtlProvider clusterTtlProvider = clusterTtlProviderFactory.create(properties);
        checkState(this.clusterTtlProvider.compareAndSet(null, clusterTtlProvider), "Cluster Ttl Provider has already been set!");

        log.info("-- Loaded Cluster Ttl Provider %s --", factoryName);
    }
}
