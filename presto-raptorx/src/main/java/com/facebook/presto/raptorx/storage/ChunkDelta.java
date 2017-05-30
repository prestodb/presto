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
package com.facebook.presto.raptorx.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ChunkDelta
{
    private final Set<Long> oldChunkIds;
    private final List<ChunkInfo> newChunks;

    @JsonCreator
    public ChunkDelta(
            @JsonProperty("oldChunkIds") Set<Long> oldChunkIds,
            @JsonProperty("newChunks") List<ChunkInfo> newChunks)
    {
        this.oldChunkIds = ImmutableSet.copyOf(requireNonNull(oldChunkIds, "oldChunkIds is null"));
        this.newChunks = ImmutableList.copyOf(requireNonNull(newChunks, "newChunks is null"));
    }

    @JsonProperty
    public Set<Long> getOldChunkIds()
    {
        return oldChunkIds;
    }

    @JsonProperty
    public List<ChunkInfo> getNewChunks()
    {
        return newChunks;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("oldChunkIds", oldChunkIds)
                .add("newChunks", newChunks)
                .toString();
    }
}
