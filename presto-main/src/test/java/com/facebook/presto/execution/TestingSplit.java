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
package com.facebook.presto.execution;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class TestingSplit
        implements ConnectorSplit
{
    private static HostAddress localHost = HostAddress.fromString("127.0.0.1");

    private List<HostAddress> addresses;

    public static TestingSplit createLocalSplit()
    {
        return new TestingSplit(ImmutableList.of(localHost));
    }

    public static TestingSplit createEmptySplit()
    {
        return new TestingSplit(ImmutableList.<HostAddress>of());
    }

    public TestingSplit(@JsonProperty("dummy") int dummy)
    {
        this.addresses = ImmutableList.of(localHost);
    }

    public TestingSplit(List<HostAddress> addresses)
    {
        this.addresses = addresses;
    }

    // we need a dummy property because Jackson refuses to serialize/deserialize an empty object
    @JsonProperty
    public int getDummy()
    {
        return 0;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
