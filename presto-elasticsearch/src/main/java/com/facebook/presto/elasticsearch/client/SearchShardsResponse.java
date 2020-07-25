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
package com.facebook.presto.elasticsearch.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class SearchShardsResponse
{
    private final List<List<Shard>> shardGroups;

    @JsonCreator
    public SearchShardsResponse(@JsonProperty("shards") List<List<Shard>> shardGroups)
    {
        requireNonNull(shardGroups, "shardGroups is null");

        this.shardGroups = ImmutableList.copyOf(shardGroups);
    }

    public List<List<Shard>> getShardGroups()
    {
        return shardGroups;
    }

    public static class Shard
    {
        private final String index;
        private final boolean primary;
        private final String node;
        private final int shard;

        @JsonCreator
        public Shard(
                @JsonProperty("index") String index,
                @JsonProperty("shard") int shard,
                @JsonProperty("primary") boolean primary,
                @JsonProperty("node") String node)
        {
            this.index = requireNonNull(index, "index is null");
            this.shard = shard;
            this.primary = primary;
            this.node = requireNonNull(node, "node is null");
        }

        public String getIndex()
        {
            return index;
        }

        public boolean isPrimary()
        {
            return primary;
        }

        public String getNode()
        {
            return node;
        }

        public int getShard()
        {
            return shard;
        }

        @Override
        public String toString()
        {
            return index + ":" + shard + "@" + node + (primary ? "[primary]" : "[replica]");
        }
    }
}
