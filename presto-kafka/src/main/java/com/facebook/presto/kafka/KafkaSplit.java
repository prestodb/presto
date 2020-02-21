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
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Represents a kafka specific {@link ConnectorSplit}. Each split is mapped to consecutive set of messages on disk (based off the message offset start and end values) so that
 * a partition can be processed by reading these messages from partition leader. Otherwise, a Kafka topic could only be processed along partition boundaries.
 */
public class KafkaSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String topicName;
    private final String keyDataFormat;
    private final String messageDataFormat;
    private final Optional<String> keyDataSchemaContents;
    private final Optional<String> messageDataSchemaContents;
    private final int partitionId;
    private final long startTimestamp;
    private final long endTimestamp;
    private long startOffset;
    private long endOffset;
    private final HostAddress leader;

    @JsonCreator
    public KafkaSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("topicName") String topicName,
            @JsonProperty("keyDataFormat") String keyDataFormat,
            @JsonProperty("messageDataFormat") String messageDataFormat,
            @JsonProperty("keyDataSchemaContents") Optional<String> keyDataSchemaContents,
            @JsonProperty("messageDataSchemaContents") Optional<String> messageDataSchemaContents,
            @JsonProperty("partitionId") int partitionId,
            @JsonProperty("startTimestamp") long startTimestamp,
            @JsonProperty("endTimestamp") long endTimestamp,
            @JsonProperty("leader") HostAddress leader)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.topicName = requireNonNull(topicName, "topicName is null");
        this.keyDataFormat = requireNonNull(keyDataFormat, "dataFormat is null");
        this.messageDataFormat = requireNonNull(messageDataFormat, "messageDataFormat is null");
        this.keyDataSchemaContents = keyDataSchemaContents;
        this.messageDataSchemaContents = messageDataSchemaContents;
        this.partitionId = partitionId;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.startOffset = 0;
        this.endOffset = 0;
        this.leader = requireNonNull(leader, "leader address is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public long getStartTimestamp()
    {
        return startTimestamp;
    }

    @JsonProperty
    public long getEndTimestamp()
    {
        return endTimestamp;
    }

    @JsonProperty
    public long getStartOffset()
    {
        return startOffset;
    }

    public void setStartOffset(long startOffset)
    {
        this.startOffset = startOffset;
    }

    @JsonProperty
    public long getEndOffset()
    {
        return endOffset;
    }

    public void setEndOffset(long endOffset)
    {
        this.endOffset = endOffset;
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
    public Optional<String> getKeyDataSchemaContents()
    {
        return keyDataSchemaContents;
    }

    @JsonProperty
    public Optional<String> getMessageDataSchemaContents()
    {
        return messageDataSchemaContents;
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
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
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
                .add("keyDataSchemaContents", keyDataSchemaContents)
                .add("messageDataSchemaContents", messageDataSchemaContents)
                .add("partitionId", partitionId)
                .add("startTimestamp", startTimestamp)
                .add("endTimestamp", endTimestamp)
                .add("startOffset", startOffset)
                .add("endOffset", endOffset)
                .add("leader", leader)
                .toString();
    }
}
