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
import static com.google.common.base.Preconditions.checkState;

public final class OutputBuffers
{
    private final long version;
    private final boolean noMoreBufferIds;
    private final Set<String> bufferIds;

    public OutputBuffers(long version, boolean noMoreBufferIds, String... bufferIds)
    {
        this(version, noMoreBufferIds, ImmutableSet.copyOf(bufferIds));
    }

    @JsonCreator
    public OutputBuffers(
            @JsonProperty("version") long version,
            @JsonProperty("noMoreBufferIds") boolean noMoreBufferIds,
            @JsonProperty("bufferIds") Set<String> bufferIds)
    {
        this.version = version;
        this.bufferIds = ImmutableSet.copyOf(checkNotNull(bufferIds, "bufferIds is null"));
        this.noMoreBufferIds = noMoreBufferIds;
    }

    @JsonProperty
    public long getVersion()
    {
        return version;
    }

    @JsonProperty
    public boolean isNoMoreBufferIds()
    {
        return noMoreBufferIds;
    }

    @JsonProperty
    public Set<String> getBufferIds()
    {
        return bufferIds;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(version, noMoreBufferIds, bufferIds);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final OutputBuffers other = (OutputBuffers) obj;
        return Objects.equal(this.version, other.version) &&
                Objects.equal(this.noMoreBufferIds, other.noMoreBufferIds) &&
                Objects.equal(this.bufferIds, other.bufferIds);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("version", version)
                .add("noMoreBufferIds", noMoreBufferIds)
                .add("bufferIds", bufferIds)
                .toString();
    }

    public static OutputBuffers addOutputBufferId(OutputBuffers outputBuffers, String bufferId)
    {
        checkNotNull(outputBuffers, "outputBuffers is null");
        checkNotNull(bufferId, "bufferId is null");
        checkState(!outputBuffers.isNoMoreBufferIds(), "No more buffer ids already set");
        return new OutputBuffers(
                outputBuffers.getVersion() + 1,
                false,
                ImmutableSet.<String>builder()
                        .addAll(outputBuffers.getBufferIds())
                        .add(bufferId).build());
    }

    public static OutputBuffers setNoMoreOutputBufferIds(OutputBuffers outputBuffers)
    {
        checkNotNull(outputBuffers, "outputBuffers is null");
        if (outputBuffers.isNoMoreBufferIds()) {
            return outputBuffers;
        }

        return new OutputBuffers(
                outputBuffers.getVersion() + 1,
                true,
                outputBuffers.getBufferIds());
    }
}
