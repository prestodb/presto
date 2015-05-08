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
package com.facebook.presto.kinesis;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.amazonaws.services.kinesis.model.Shard;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.TupleDomain;

public class KinesisShard
        implements ConnectorPartition
{
    private final String streamName;
    private final Shard shard;

    public KinesisShard(String streamName, Shard shard)
    {
        this.streamName = streamName;
        this.shard = shard;
    }

    @Override
    public String getPartitionId()
    {
        return shard.getShardId();
    }

    @Override
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return TupleDomain.all();
    }

    public String getStreamName()
    {
        return streamName;
    }

    public Shard getShard()
    {
        return shard;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("streamName", streamName)
                .add("shard", shard)
                .toString();
    }
}
