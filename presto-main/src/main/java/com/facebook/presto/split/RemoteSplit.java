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
import com.facebook.presto.tuple.TupleInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;

public class RemoteSplit
        implements Split
{
    private final URI location;
    private final List<TupleInfo> tupleInfos;

    @JsonCreator
    public RemoteSplit(
            @JsonProperty("location") URI location,
            @JsonProperty("tupleInfos") List<TupleInfo> tupleInfos)
    {
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");

        this.location = location;
        this.tupleInfos = ImmutableList.copyOf(tupleInfos);
    }

    @JsonProperty
    public URI getLocation()
    {
        return location;
    }

    @JsonProperty
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("location", location)
                .add("tupleInfos", tupleInfos)
                .toString();
    }
}
