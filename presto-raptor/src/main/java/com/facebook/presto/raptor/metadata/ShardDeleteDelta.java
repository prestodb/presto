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
package com.facebook.presto.raptor.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShardDeleteDelta
{
    private final UUID oldShardUuid;
    private final Optional<UUID> oldDeltaDeleteShard;
    // Optional.empty() means delete original file
    private final Optional<ShardInfo> newDeltaDeleteShard;

    @JsonCreator
    public ShardDeleteDelta(
            @JsonProperty("oldShardUuid") UUID oldShardUuid,
            @JsonProperty("oldDeltaDeleteShard") Optional<UUID> oldDeltaDeleteShard,
            @JsonProperty("newDeltaDeleteShard") Optional<ShardInfo> newDeltaDeleteShard)
    {
        this.oldShardUuid = requireNonNull(oldShardUuid, "oldShardUuids is null");
        this.oldDeltaDeleteShard = requireNonNull(oldDeltaDeleteShard, "deltaDeleteShardPair is null");
        this.newDeltaDeleteShard = requireNonNull(newDeltaDeleteShard, "deltaDeleteShardPair is null");
    }

    @JsonProperty
    public UUID getOldShardUuid()
    {
        return oldShardUuid;
    }

    @JsonProperty
    public Optional<UUID> getOldDeltaDeleteShard()
    {
        return oldDeltaDeleteShard;
    }

    @JsonProperty
    public Optional<ShardInfo> getNewDeltaDeleteShard()
    {
        return newDeltaDeleteShard;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("oldShardUuid", oldShardUuid)
                .add("oldDeltaDeleteShard", oldDeltaDeleteShard)
                .add("newDeltaDeleteShard", newDeltaDeleteShard)
                .toString();
    }
}
