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
package com.facebook.presto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class OutputBuffers
{
    private final Set<String> bufferIds;
    private final boolean noMoreBufferIds;

    @JsonCreator
    public OutputBuffers(
            @JsonProperty("bufferIds") Set<String> bufferIds,
            @JsonProperty("noMoreBufferIds") boolean noMoreBufferIds)
    {
        this.bufferIds = ImmutableSet.copyOf(checkNotNull(bufferIds, "bufferIds is null"));
        this.noMoreBufferIds = noMoreBufferIds;
    }

    @JsonProperty
    public Set<String> getBufferIds()
    {
        return bufferIds;
    }

    @JsonProperty
    public boolean isNoMoreBufferIds()
    {
        return noMoreBufferIds;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("bufferIds", bufferIds)
                .add("noMoreBufferIds", noMoreBufferIds)
                .toString();
    }
}
