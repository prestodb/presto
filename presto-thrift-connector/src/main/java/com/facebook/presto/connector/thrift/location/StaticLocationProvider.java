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
package com.facebook.presto.connector.thrift.location;

import com.facebook.presto.spi.HostAddress;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StaticLocationProvider
        implements HostLocationProvider
{
    private final List<HostAddress> hosts;
    private final AtomicInteger index = new AtomicInteger(0);

    @Inject
    public StaticLocationProvider(StaticLocationConfig config)
    {
        requireNonNull(config, "config is null");
        List<HostAddress> hosts = config.getHosts().getHosts();
        checkArgument(!hosts.isEmpty(), "hosts is empty");
        this.hosts = new ArrayList<>(hosts);
        Collections.shuffle(this.hosts);
    }

    /**
     * Provides the next host from a configured list of hosts in a round-robin fashion.
     */
    @Override
    public HostAddress getAnyHost()
    {
        return hosts.get(index.getAndUpdate(this::next));
    }

    @Override
    public HostAddress getAnyOf(List<HostAddress> requestedHosts)
    {
        checkArgument(requestedHosts != null && !requestedHosts.isEmpty(), "requestedHosts is null or empty");
        return requestedHosts.get(ThreadLocalRandom.current().nextInt(requestedHosts.size()));
    }

    private int next(int x)
    {
        return (x + 1) % hosts.size();
    }
}
