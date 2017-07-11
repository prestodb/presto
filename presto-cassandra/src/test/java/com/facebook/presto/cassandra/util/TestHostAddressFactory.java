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
package com.facebook.presto.cassandra.util;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.TestHost;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertEquals;

public class TestHostAddressFactory
{
    @Test
    public void testToHostAddressList()
            throws Exception
    {
        Set<Host> hosts = ImmutableSet.of(
                new TestHost(
                    new InetSocketAddress(
                        InetAddress.getByAddress(new byte[] {
                            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
                        }),
                        3000)),
                new TestHost(new InetSocketAddress(InetAddress.getByAddress(new byte[] {1, 2, 3, 4}), 3000)));

        HostAddressFactory hostAddressFactory = new HostAddressFactory();
        List<HostAddress> list = hostAddressFactory.toHostAddressList(hosts);

        assertEquals(list.toString(), "[[102:304:506:708:90a:b0c:d0e:f10], 1.2.3.4]");
    }
}
