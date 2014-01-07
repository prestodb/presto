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
package com.facebook.presto.split;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Split;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public class NativeSplit
        implements Split
{
    private final UUID shardUuid;
    private final List<HostAddress> addresses;

    @JsonCreator
    public NativeSplit(
            @JsonProperty("shardUuid") UUID shardUuid,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.shardUuid = checkNotNull(shardUuid, "shardUuid is null");

        checkNotNull(addresses, "addresses is null");
        this.addresses = ImmutableList.copyOf(addresses);
    }

    @Override
    @JsonProperty
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public UUID getShardUuid()
    {
        return shardUuid;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("shardUuid", shardUuid)
                .add("hosts", addresses)
                .toString();
    }
}
