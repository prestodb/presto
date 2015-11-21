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
package com.facebook.presto.kafka;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Kafka specific partition representation. Each partition maps to a topic partition and is split along segment boundaries.
 */
public class KafkaPartition
        implements ConnectorPartition
{
    private final String topicName;
    private final int partitionId;
    private final HostAddress partitionLeader;
    private final List<HostAddress> partitionNodes;

    public KafkaPartition(String topicName,
            int partitionId,
            HostAddress partitionLeader,
            List<HostAddress> partitionNodes)
    {
        this.topicName = requireNonNull(topicName, "schema name is null");
        this.partitionId = partitionId;
        this.partitionLeader = requireNonNull(partitionLeader, "partitionLeader is null");
        this.partitionNodes = ImmutableList.copyOf(requireNonNull(partitionNodes, "partitionNodes is null"));
    }

    @Override
    public String getPartitionId()
    {
        return Integer.toString(partitionId);
    }

    public String getTopicName()
    {
        return topicName;
    }

    public int getPartitionIdAsInt()
    {
        return partitionId;
    }

    public HostAddress getPartitionLeader()
    {
        return partitionLeader;
    }

    public List<HostAddress> getPartitionNodes()
    {
        return partitionNodes;
    }

    @Override
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return TupleDomain.all();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("topicName", topicName)
                .add("partitionId", partitionId)
                .add("partitionLeader", partitionLeader)
                .add("partitionNodes", partitionNodes)
                .toString();
    }
}
