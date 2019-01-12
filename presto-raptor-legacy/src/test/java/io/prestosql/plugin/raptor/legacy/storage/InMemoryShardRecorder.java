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
package io.prestosql.plugin.raptor.legacy.storage;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.raptor.legacy.metadata.ShardRecorder;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class InMemoryShardRecorder
        implements ShardRecorder
{
    private final List<RecordedShard> shards = new ArrayList<>();

    public List<RecordedShard> getShards()
    {
        return ImmutableList.copyOf(shards);
    }

    @Override
    public void recordCreatedShard(long transactionId, UUID shardUuid)
    {
        shards.add(new RecordedShard(transactionId, shardUuid));
    }

    public static class RecordedShard
    {
        private final long transactionId;
        private final UUID shardUuid;

        public RecordedShard(long transactionId, UUID shardUuid)
        {
            this.transactionId = transactionId;
            this.shardUuid = requireNonNull(shardUuid, "shardUuid is null");
        }

        public long getTransactionId()
        {
            return transactionId;
        }

        public UUID getShardUuid()
        {
            return shardUuid;
        }
    }
}
