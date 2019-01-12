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
package io.prestosql.plugin.cassandra.util;

import com.datastax.driver.core.Host;
import io.prestosql.spi.HostAddress;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HostAddressFactory
{
    private final Map<String, HostAddress> hostMap = new HashMap<>();

    public HostAddress toHostAddress(Host host)
    {
        return toHostAddress(host.getAddress().getHostAddress());
    }

    public List<HostAddress> toHostAddressList(Collection<Host> hosts)
    {
        ArrayList<HostAddress> list = new ArrayList<>(hosts.size());
        for (Host host : hosts) {
            list.add(toHostAddress(host));
        }
        return list;
    }

    public HostAddress toHostAddress(String hostAddressName)
    {
        HostAddress address = hostMap.get(hostAddressName);
        if (address == null) {
            address = HostAddress.fromString(hostAddressName);
            hostMap.put(hostAddressName, address);
        }
        return address;
    }

    public List<HostAddress> AddressNamesToHostAddressList(Collection<String> hosts)
    {
        ArrayList<HostAddress> list = new ArrayList<>(hosts.size());
        for (String host : hosts) {
            list.add(toHostAddress(host));
        }
        return list;
    }
}
