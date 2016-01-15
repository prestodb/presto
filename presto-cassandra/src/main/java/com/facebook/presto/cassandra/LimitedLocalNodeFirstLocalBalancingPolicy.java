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
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/* original from cassandra-2.1.3 org.apache.cassandra.hadoop.cql3 */
package com.facebook.presto.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * This load balancing policy is intended to be used only for CqlRecordReader when it fetches a particular split.
 * <p/>
 * It chooses alive hosts only from the set of the given contact points - because the connection is used to load the data from
 * the particular split, with a strictly defined list of the allowed hosts, it is pointless to try the other nodes.
 * The policy tracks which of the allowed hosts are alive, and when a new query plan is requested, it returns those hosts
 * in the following order:
 * <ul>
 * <li>the local node</li>
 * <li>the collection of the remaining hosts (which is shuffled on each request)</li>
 * </ul>
 */
class LimitedLocalNodeFirstLocalBalancingPolicy implements LoadBalancingPolicy
{
    private static final Logger logger = Logger.get(LimitedLocalNodeFirstLocalBalancingPolicy.class);

    private static final Set<InetAddress> localAddresses = Collections.unmodifiableSet(getLocalInetAddresses());

    private final CopyOnWriteArraySet<Host> allowedLiveHosts = new CopyOnWriteArraySet<>();

    private final Set<InetAddress> allowedAddresses = new HashSet<>();

    public LimitedLocalNodeFirstLocalBalancingPolicy(String[] contactPoints)
    {
        for (String contactPoint : contactPoints) {
            try {
                InetAddress[] addresses = InetAddress.getAllByName(contactPoint);
                Collections.addAll(allowedAddresses, addresses);
            }
            catch (UnknownHostException e) {
                logger.warn("Invalid contact point host name: {}, skipping it", contactPoint);
            }
        }
        logger.debug("Created instance with the following contact points: {}", Arrays.asList(contactPoints));
    }

    @Override
    public void init(Cluster cluster, Collection<Host> hosts)
    {
        List<Host> addedHosts = new ArrayList<>();
        for (Host host : hosts) {
            if (allowedAddresses.contains(host.getAddress())) {
                addedHosts.add(host);
            }
        }
        allowedLiveHosts.addAll(addedHosts);
        logger.debug("Initialized with allowed hosts: {}", addedHosts);
    }

    @Override
    public HostDistance distance(Host host)
    {
        if (isLocalHost(host)) {
            return HostDistance.LOCAL;
        }
        else {
            return HostDistance.REMOTE;
        }
    }

    @Override
    public Iterator<Host> newQueryPlan(String keyspace, Statement statement)
    {
        List<Host> local = new ArrayList<>(1);
        List<Host> remote = new ArrayList<>(allowedLiveHosts.size());
        for (Host allowedHost : allowedLiveHosts) {
            if (isLocalHost(allowedHost)) {
                local.add(allowedHost);
            }
            else {
                remote.add(allowedHost);
            }
        }

        Collections.shuffle(remote);

        logger.debug("Using the following hosts order for the new query plan: {} | {}", local, remote);

        return Iterators.concat(local.iterator(), remote.iterator());
    }

    @Override
    public void onAdd(Host host)
    {
        if (allowedAddresses.contains(host.getAddress())) {
            allowedLiveHosts.add(host);
            logger.debug("Added a new host {}", host);
        }
    }

    @Override
    public void onUp(Host host)
    {
        if (allowedAddresses.contains(host.getAddress())) {
            allowedLiveHosts.add(host);
            logger.debug("The host {} is now up", host);
        }
    }

    @Override
    public void onDown(Host host)
    {
        if (allowedLiveHosts.remove(host)) {
            logger.debug("The host {} is now down", host);
        }
    }

    @Override
    public void onRemove(Host host)
    {
        if (allowedLiveHosts.remove(host)) {
            logger.debug("Removed the host {}", host);
        }
    }

    @Override
    public void onSuspected(Host host)
    {
        // not supported by this load balancing policy
    }

    private static boolean isLocalHost(Host host)
    {
        InetAddress hostAddress = host.getAddress();
        return hostAddress.isLoopbackAddress() || localAddresses.contains(hostAddress);
    }

    private static Set<InetAddress> getLocalInetAddresses()
    {
        try {
            return Sets.newHashSet(Iterators.concat(
                    Iterators.transform(
                            Iterators.forEnumeration(NetworkInterface.getNetworkInterfaces()),
                            (Function<NetworkInterface, Iterator<InetAddress>>) netIface -> Iterators.forEnumeration(netIface.getInetAddresses()))));
        }
        catch (SocketException e) {
            logger.warn("Could not retrieve local network interfaces.", e);
            return Collections.emptySet();
        }
    }
}
