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

import com.facebook.presto.execution.TaskId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public final class OutputBuffers
{
    public static final OutputBuffers INITIAL_EMPTY_OUTPUT_BUFFERS = new OutputBuffers(0, false, ImmutableMap.<TaskId, PagePartitionFunction>of());

    private final long version;
    private final boolean noMoreBufferIds;
    private final Map<TaskId, PagePartitionFunction> buffers;

    // Visible only for Jackson... Use the "with" methods instead
    @JsonCreator
    public OutputBuffers(
            @JsonProperty("version") long version,
            @JsonProperty("noMoreBufferIds") boolean noMoreBufferIds,
            @JsonProperty("buffers") Map<TaskId, PagePartitionFunction> buffers)
    {
        this.version = version;
        this.buffers = ImmutableMap.copyOf(checkNotNull(buffers, "buffers is null"));
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
    public Map<TaskId, PagePartitionFunction> getBuffers()
    {
        return buffers;
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
        final OutputBuffers other = (OutputBuffers) obj;
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

    public OutputBuffers withBuffer(TaskId bufferId, PagePartitionFunction pagePartitionFunction)
    {
        checkNotNull(bufferId, "bufferId is null");

        if (buffers.containsKey(bufferId)) {
            checkHasBuffer(bufferId, pagePartitionFunction);
            return this;
        }

        // verify no new buffers is not set
        checkState(!noMoreBufferIds, "No more buffer ids already set");

        return new OutputBuffers(
                version + 1,
                false,
                ImmutableMap.<TaskId, PagePartitionFunction>builder()
                        .putAll(buffers)
                        .put(bufferId, pagePartitionFunction)
                        .build());
    }

    public OutputBuffers withBuffers(Map<TaskId, PagePartitionFunction> buffers)
    {
        checkNotNull(buffers, "buffers is null");

        Map<TaskId, PagePartitionFunction> newBuffers = new HashMap<>();
        for (Entry<TaskId, PagePartitionFunction> entry : buffers.entrySet()) {
            TaskId bufferId = entry.getKey();
            PagePartitionFunction pagePartitionFunction = entry.getValue();

            // it is ok to have a duplicate buffer declaration but it must have the same page partition function
            if (this.buffers.containsKey(bufferId)) {
                checkHasBuffer(bufferId, pagePartitionFunction);
                continue;
            }

            newBuffers.put(bufferId, pagePartitionFunction);
        }

        // if we don't have new buffers, don't update
        if (newBuffers.isEmpty()) {
            return this;
        }

        // verify no new buffers is not set
        checkState(!noMoreBufferIds, "No more buffer ids already set");

        // add the existing buffers
        newBuffers.putAll(this.buffers);

        return new OutputBuffers(version + 1, false, newBuffers);
    }

    public OutputBuffers withNoMoreBufferIds()
    {
        checkNotNull(this, "this is null");
        if (noMoreBufferIds) {
            return this;
        }

        return new OutputBuffers(version + 1, true, buffers);
    }

    private void checkHasBuffer(TaskId bufferId, PagePartitionFunction pagePartitionFunction)
    {
        checkState(getBuffers().get(bufferId).equals(pagePartitionFunction),
                "outputBuffers already contains buffer %s, but partition function is %s not %s",
                bufferId,
                buffers.get(bufferId),
                pagePartitionFunction);
    }
}
