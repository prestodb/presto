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
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public final class HostList
{
    private final List<HostAddress> hosts;

    private HostList(List<HostAddress> hosts)
    {
        this.hosts = ImmutableList.copyOf(requireNonNull(hosts, "hosts is null"));
    }

    // needed for automatic config parsing
    @SuppressWarnings("unused")
    public static HostList fromString(String hosts)
    {
        return new HostList(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(hosts).stream().map(HostAddress::fromString).collect(toImmutableList()));
    }

    public static HostList of(HostAddress... hosts)
    {
        return new HostList(asList(hosts));
    }

    public static HostList fromList(List<HostAddress> hosts)
    {
        return new HostList(hosts);
    }

    public List<HostAddress> getHosts()
    {
        return hosts;
    }

    public String stringValue()
    {
        return Joiner.on(',').join(hosts);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HostList hostList = (HostList) o;
        return hosts.equals(hostList.hosts);
    }

    @Override
    public int hashCode()
    {
        return hosts.hashCode();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hosts", hosts)
                .toString();
    }
}
