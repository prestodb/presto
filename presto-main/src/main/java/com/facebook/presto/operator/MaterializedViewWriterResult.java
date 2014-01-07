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
package com.facebook.presto.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public class MaterializedViewWriterResult
{
    private final UUID shardUuid;
    private final String nodeIdentifier;

    public static MaterializedViewWriterResult forMap(Map<String, Object> map)
    {
        return new MaterializedViewWriterResult(
                UUID.fromString((String) map.get("shardUuid")),
                (String) map.get("nodeIdentifier"));
    }

    @JsonCreator
    public MaterializedViewWriterResult(
            @JsonProperty("shardUuid") UUID shardUuid,
            @JsonProperty("nodeIdentifier") String nodeIdentifier)
    {
        this.shardUuid = checkNotNull(shardUuid, "shardUuid is null");
        this.nodeIdentifier = checkNotNull(nodeIdentifier, "nodeIdentifier is null");
    }

    @JsonProperty
    public UUID getShardUuid()
    {
        return shardUuid;
    }

    @JsonProperty
    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("shardUuid", shardUuid)
                .add("nodeIdentifier", nodeIdentifier)
                .toString();
    }
}
