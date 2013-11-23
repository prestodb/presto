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

import com.facebook.hive.metastore.api.ThriftHiveMetastore;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;

import io.airlift.discovery.client.DiscoveryException;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceState;
import io.airlift.discovery.client.ServiceType;

import java.util.Set;

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
    public ThriftHiveMetastore createMetastoreClient()
    {
        Set<ServiceDescriptor> descriptors = ImmutableSet.copyOf(Iterables.filter(selector.selectAllServices(), Predicates.and(runningPredicate(), thriftPredicate())));

        if (descriptors.isEmpty()) {
            throw new DiscoveryException("No metastore servers available for pool: " + selector.getPool());
        }

        Set<HostAndPort> hostAndPorts = ImmutableSet.copyOf(Iterables.transform(descriptors, getHostAndPortFunction()));

        return clientFactory.create(hostAndPorts);
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

    private static Predicate<? super ServiceDescriptor> thriftPredicate()
    {
        return new Predicate<ServiceDescriptor>()
        {
            @Override
            public boolean apply(ServiceDescriptor input)
            {
                String thrift = input.getProperties().get("thrift");
                if (thrift == null) {
                    return false;
                }
                try {
                    HostAndPort hostAndPort = HostAndPort.fromString(thrift);
                    checkArgument(hostAndPort.hasPort());
                    return true;
                }
                catch (IllegalArgumentException iae) {
                    return false;
                }
            }
        };
    }

    private static Function<ServiceDescriptor, HostAndPort> getHostAndPortFunction()
    {
        return new Function<ServiceDescriptor, HostAndPort>()
        {
            @Override
            public HostAndPort apply(ServiceDescriptor descriptor)
            {
                String thrift = descriptor.getProperties().get("thrift");
                checkNotNull(thrift, "thrift is null");
                HostAndPort hostAndPort = HostAndPort.fromString(thrift);
                checkArgument(hostAndPort.hasPort());
                return hostAndPort;
            }
        };
    }

}
