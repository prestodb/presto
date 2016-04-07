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

import com.facebook.presto.sql.planner.PartitioningHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static com.facebook.presto.OutputBuffers.BufferType.BROADCAST;
import static com.facebook.presto.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

public final class OutputBuffers
{
    public static final int BROADCAST_PARTITION_ID = 0;

    public static OutputBuffers createInitialEmptyOutputBuffers(BufferType type)
    {
        return new OutputBuffers(type, 0, false, ImmutableMap.of());
    }

    public static OutputBuffers createInitialEmptyOutputBuffers(PartitioningHandle partitioningHandle)
    {
        BufferType type;
        if (partitioningHandle.equals(FIXED_BROADCAST_DISTRIBUTION)) {
            type = BROADCAST;
        }
        else {
            type = PARTITIONED;
        }
        return new OutputBuffers(type, 0, false, ImmutableMap.of());
    }

    public enum BufferType
    {
        PARTITIONED,
        BROADCAST,
    }

    private final BufferType type;
    private final long version;
    private final boolean noMoreBufferIds;
    private final Map<OutputBufferId, Integer> buffers;

    // Visible only for Jackson... Use the "with" methods instead
    @JsonCreator
    public OutputBuffers(
            @JsonProperty("type") BufferType type,
            @JsonProperty("version") long version,
            @JsonProperty("noMoreBufferIds") boolean noMoreBufferIds,
            @JsonProperty("buffers") Map<OutputBufferId, Integer> buffers)
    {
        this.type = type;
        this.version = version;
        this.buffers = ImmutableMap.copyOf(requireNonNull(buffers, "buffers is null"));
        this.noMoreBufferIds = noMoreBufferIds;
    }

    @JsonProperty
    public BufferType getType()
    {
        return type;
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
    public Map<OutputBufferId, Integer> getBuffers()
    {
        return buffers;
    }

    public void checkValidTransition(OutputBuffers newOutputBuffers)
    {
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");
        checkArgument(type == newOutputBuffers.getType(), "newOutputBuffers has a different type");

        if (noMoreBufferIds) {
            checkArgument(this.equals(newOutputBuffers), "Expected buffer to not change after no more buffers is set");
            return;
        }

        if (version > newOutputBuffers.version) {
            throw new IllegalArgumentException("newOutputBuffers version is older");
        }

        if (version == newOutputBuffers.version) {
            checkArgument(this.equals(newOutputBuffers), "newOutputBuffers is the same version but contains different information");
        }

        // assure we have not changed the buffer assignments
        for (Entry<OutputBufferId, Integer> entry : buffers.entrySet()) {
            if (!entry.getValue().equals(newOutputBuffers.buffers.get(entry.getKey()))) {
                throw new IllegalArgumentException("newOutputBuffers has changed the assignment for task " + entry.getKey());
            }
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(version, noMoreBufferIds, buffers);
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
        OutputBuffers other = (OutputBuffers) obj;
        return Objects.equals(this.version, other.version) &&
                Objects.equals(this.noMoreBufferIds, other.noMoreBufferIds) &&
                Objects.equals(this.buffers, other.buffers);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("version", version)
                .add("noMoreBufferIds", noMoreBufferIds)
                .add("bufferIds", buffers)
                .toString();
    }

    public OutputBuffers withBuffer(OutputBufferId bufferId, int partition)
    {
        requireNonNull(bufferId, "bufferId is null");

        if (buffers.containsKey(bufferId)) {
            checkHasBuffer(bufferId, partition);
            return this;
        }

        // verify no new buffers is not set
        checkState(!noMoreBufferIds, "No more buffer ids already set");

        return new OutputBuffers(
                type,
                version + 1,
                false,
                ImmutableMap.<OutputBufferId, Integer>builder()
                        .putAll(buffers)
                        .put(bufferId, partition)
                        .build());
    }

    public OutputBuffers withBuffers(Map<OutputBufferId, Integer> buffers)
    {
        requireNonNull(buffers, "buffers is null");

        Map<OutputBufferId, Integer> newBuffers = new HashMap<>();
        for (Entry<OutputBufferId, Integer> entry : buffers.entrySet()) {
            OutputBufferId bufferId = entry.getKey();
            int partition = entry.getValue();

            // it is ok to have a duplicate buffer declaration but it must have the same page partition
            if (this.buffers.containsKey(bufferId)) {
                checkHasBuffer(bufferId, partition);
                continue;
            }

            newBuffers.put(bufferId, partition);
        }

        // if we don't have new buffers, don't update
        if (newBuffers.isEmpty()) {
            return this;
        }

        // verify no new buffers is not set
        checkState(!noMoreBufferIds, "No more buffer ids already set");

        // add the existing buffers
        newBuffers.putAll(this.buffers);

        return new OutputBuffers(type, version + 1, false, newBuffers);
    }

    public OutputBuffers withNoMoreBufferIds()
    {
        if (noMoreBufferIds) {
            return this;
        }

        return new OutputBuffers(type, version + 1, true, buffers);
    }

    private void checkHasBuffer(OutputBufferId bufferId, int partition)
    {
        checkArgument(
                Objects.equals(buffers.get(bufferId), partition),
                "OutputBuffers already contains task %s, but partition is set to %s not %s",
                bufferId,
                buffers.get(bufferId),
                partition);
    }

    public static class OutputBufferId
    {
        // this is needed by JAX-RS
        public static OutputBufferId fromString(String id)
        {
            return new OutputBufferId(parseInt(id));
        }

        private final int id;

        @JsonCreator
        public OutputBufferId(int id)
        {
            checkArgument(id >= 0, "id is negative");
            this.id = id;
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
            OutputBufferId that = (OutputBufferId) o;
            return id == that.id;
        }

        @JsonValue
        public int getId()
        {
            return id;
        }

        @Override
        public int hashCode()
        {
            return id;
        }

        @Override
        public String toString()
        {
            return String.valueOf(id);
        }
    }
}
