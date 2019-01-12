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
package io.prestosql.plugin.raptor.legacy.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShardDelta
{
    private final List<UUID> oldShardUuids;
    private final List<ShardInfo> newShards;

    @JsonCreator
    public ShardDelta(
            @JsonProperty("oldShardUuids") List<UUID> oldShardUuids,
            @JsonProperty("newShards") List<ShardInfo> newShards)
    {
        this.oldShardUuids = ImmutableList.copyOf(requireNonNull(oldShardUuids, "oldShardUuids is null"));
        this.newShards = ImmutableList.copyOf(requireNonNull(newShards, "newShards is null"));
    }

    @JsonProperty
    public List<UUID> getOldShardUuids()
    {
        return oldShardUuids;
    }

    @JsonProperty
    public List<ShardInfo> getNewShards()
    {
        return newShards;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("oldShardUuids", oldShardUuids)
                .add("newShards", newShards)
                .toString();
    }
}
