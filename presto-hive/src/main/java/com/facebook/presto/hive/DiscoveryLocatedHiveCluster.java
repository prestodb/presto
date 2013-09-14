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
package com.facebook.presto.hive;

import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TTransportException;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.airlift.discovery.client.DiscoveryException;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceState;
import io.airlift.discovery.client.ServiceType;

import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DiscoveryLocatedHiveCluster
        implements HiveCluster
{
    private final ServiceSelector selector;
    private final HiveMetastoreClientFactory clientFactory;

    @Inject
    public DiscoveryLocatedHiveCluster(@ServiceType("hive-metastore") ServiceSelector selector, HiveMetastoreClientFactory clientFactory)
    {
        this.selector = checkNotNull(selector, "selector is null");
        this.clientFactory = checkNotNull(clientFactory, "clientFactory is null");
    }

    @Override
    public HiveMetastoreClient createMetastoreClient()
    {
        List<ServiceDescriptor> descriptors = Lists.newArrayList(Iterables.filter(selector.selectAllServices(), runningPredicate()));
        if (descriptors.isEmpty()) {
            throw new DiscoveryException("No metastore servers available for pool: " + selector.getPool());
        }

        Collections.shuffle(descriptors);
        TTransportException lastException = null;
        for (ServiceDescriptor descriptor : descriptors) {
            String thrift = descriptor.getProperties().get("thrift");
            if (thrift != null) {
                try {
                    HostAndPort metastore = HostAndPort.fromString(thrift);
                    checkArgument(metastore.hasPort());
                    return clientFactory.create(metastore.getHostText(), metastore.getPort());
                }
                catch (IllegalArgumentException ignored) {
                    // Ignore entries with parse issues
                }
                catch (TTransportException e) {
                    lastException = e;
                }
            }
        }

        throw new DiscoveryException("Unable to connect to any metastore servers in pool: " + selector.getPool(), lastException);
    }

    private static Predicate<? super ServiceDescriptor> runningPredicate()
    {
        return new Predicate<ServiceDescriptor>()
        {
            @Override
            public boolean apply(ServiceDescriptor input)
            {
                return input.getState() != ServiceState.STOPPED;
            }
        };
    }
}
