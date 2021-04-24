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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

public class KafkaFilterResult
{
    private final List<PartitionInfo> partitionInfos;
    private final Map<TopicPartition, Long> partitionBeginOffsets;
    private final Map<TopicPartition, Long> partitionEndOffsets;

    public KafkaFilterResult(List<PartitionInfo> partitionInfos,
            Map<TopicPartition, Long> partitionBeginOffsets,
            Map<TopicPartition, Long> partitionEndOffsets)
    {
        this.partitionInfos = ImmutableList.copyOf(partitionInfos);
        this.partitionBeginOffsets = ImmutableMap.copyOf(partitionBeginOffsets);
        this.partitionEndOffsets = ImmutableMap.copyOf(partitionEndOffsets);
    }

    public List<PartitionInfo> getPartitionInfos()
    {
        return partitionInfos;
    }

    public Map<TopicPartition, Long> getPartitionBeginOffsets()
    {
        return partitionBeginOffsets;
    }

    public Map<TopicPartition, Long> getPartitionEndOffsets()
    {
        return partitionEndOffsets;
    }
}
