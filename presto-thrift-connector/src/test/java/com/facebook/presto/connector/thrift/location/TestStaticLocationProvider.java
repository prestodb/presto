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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestStaticLocationProvider
{
    @Test
    public void testGetAnyHostRoundRobin()
            throws Exception
    {
        List<HostAddress> expected = ImmutableList.of(
                HostAddress.fromParts("localhost1", 11111),
                HostAddress.fromParts("localhost2", 22222),
                HostAddress.fromParts("localhost3", 33333));
        HostLocationProvider provider = new StaticLocationProvider(new StaticLocationConfig().setHosts(HostList.fromList(expected)));
        List<HostAddress> actual = new ArrayList<>(expected.size());
        for (int i = 0; i < expected.size(); i++) {
            actual.add(provider.getAnyHost());
        }
        assertEquals(ImmutableSet.copyOf(actual), ImmutableSet.copyOf(expected));
    }
}
