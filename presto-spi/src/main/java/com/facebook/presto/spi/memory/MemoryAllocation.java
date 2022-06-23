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
package com.facebook.presto.spi.memory;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class MemoryAllocation
{
    private final String tag;
    private final long allocation;

    @ThriftConstructor
    @JsonCreator
    public MemoryAllocation(
            @JsonProperty("tag") String tag,
            @JsonProperty("allocation") long allocation)
    {
        this.tag = requireNonNull(tag, "tag is null");
        this.allocation = allocation;
    }

    @ThriftField(1)
    @JsonProperty
    public String getTag()
    {
        return tag;
    }

    @ThriftField(2)
    @JsonProperty
    public long getAllocation()
    {
        return allocation;
    }
}
