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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Represents a kafka specific {@link ConnectorSplit}. Each split is mapped to a segment file on disk (based off the segment offset start() and end() values) so that
 * a partition can be processed by reading segment files from partition leader. Otherwise, a Kafka topic could only be processed along partition boundaries.
 * <p/>
 * When planning to process a Kafka topic with Presto, using smaller than the recommended segment size (default is 1G) allows Presto to optimize early and process a topic
 * with more workers in parallel.
 */
public class KafkaSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String topicName;
    private final String keyDataFormat;
    private final String messageDataFormat;
    private final int partitionId;
    private final long start;
    private final long end;
    private final HostAddress leader;

    @JsonCreator
    public KafkaSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("topicName") String topicName,
            @JsonProperty("keyDataFormat") String keyDataFormat,
            @JsonProperty("messageDataFormat") String messageDataFormat,
            @JsonProperty("partitionId") int partitionId,
            @JsonProperty("start") long start,
            @JsonProperty("end") long end,
            @JsonProperty("leader") HostAddress leader)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.topicName = requireNonNull(topicName, "topicName is null");
        this.keyDataFormat = requireNonNull(keyDataFormat, "dataFormat is null");
        this.messageDataFormat = requireNonNull(messageDataFormat, "messageDataFormat is null");
        this.partitionId = partitionId;
        this.start = start;
        this.end = end;
        this.leader = requireNonNull(leader, "leader address is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getEnd()
    {
        return end;
    }

    @JsonProperty
    public String getTopicName()
    {
        return topicName;
    }

    @JsonProperty
    public String getKeyDataFormat()
    {
        return keyDataFormat;
    }

    @JsonProperty
    public String getMessageDataFormat()
    {
        return messageDataFormat;
    }

    @JsonProperty
    public int getPartitionId()
    {
        return partitionId;
    }

    @JsonProperty
    public HostAddress getLeader()
    {
        return leader;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of(leader);
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("topicName", topicName)
                .add("keyDataFormat", keyDataFormat)
                .add("messageDataFormat", messageDataFormat)
                .add("partitionId", partitionId)
                .add("start", start)
                .add("end", end)
                .add("leader", leader)
                .toString();
    }
}
