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
package com.facebook.presto.pinot;

import com.facebook.presto.pinot.query.PinotQueryGenerator;
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
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PinotSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final SplitType splitType;
    private final List<PinotColumnHandle> expectedColumnHandles;

    // Properties needed for broker split type
    private final Optional<PinotQueryGenerator.GeneratedPinotQuery> brokerPinotQuery;

    // Properties needed for segment split type
    private final Optional<String> segmentPinotQuery;
    private final List<String> segments;
    private final Optional<String> segmentHost;
    private final Optional<Integer> grpcPort;

    @JsonCreator
    public PinotSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("splitType") SplitType splitType,
            @JsonProperty("expectedColumnHandles") List<PinotColumnHandle> expectedColumnHandles,
            @JsonProperty("brokerQuery") Optional<PinotQueryGenerator.GeneratedPinotQuery> brokerPinotQuery,
            @JsonProperty("segmentPinotQuery") Optional<String> segmentPinotQuery,
            @JsonProperty("segments") List<String> segments,
            @JsonProperty("segmentHost") Optional<String> segmentHost,
            @JsonProperty("grpcPort") Optional<Integer> grpcPort)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.splitType = requireNonNull(splitType, "splitType id is null");
        this.expectedColumnHandles = requireNonNull(expectedColumnHandles, "expected column handles is null");
        this.brokerPinotQuery = requireNonNull(brokerPinotQuery, "brokerPinotQuery is null");
        this.segmentPinotQuery = requireNonNull(segmentPinotQuery, "segmentPinotQuery is null");
        this.segments = ImmutableList.copyOf(requireNonNull(segments, "segment is null"));
        this.segmentHost = requireNonNull(segmentHost, "host is null");
        this.grpcPort = grpcPort;

        // make sure the segment properties are present when the split type is segment
        if (splitType == SplitType.SEGMENT) {
            checkArgument(segmentPinotQuery.isPresent(), "segmentPinotQuery is missing from the split");
            checkArgument(!segments.isEmpty(), "Segments are missing from the split");
            checkArgument(segmentHost.isPresent(), "Segment host address is missing from the split");
        }
        else {
            checkArgument(brokerPinotQuery.isPresent(), "brokerPinotQuery is missing from the split");
        }
    }

    public static PinotSplit createBrokerSplit(String connectorId, List<PinotColumnHandle> expectedColumnHandles, PinotQueryGenerator.GeneratedPinotQuery brokerQuery)
    {
        return new PinotSplit(
                requireNonNull(connectorId, "connector id is null"),
                SplitType.BROKER,
                expectedColumnHandles,
                Optional.of(requireNonNull(brokerQuery, "brokerQuery is null")),
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty());
    }

    public static PinotSplit createSegmentSplit(String connectorId, String segmentPinotQuery, List<PinotColumnHandle> expectedColumnHandles, List<String> segments, String segmentHost, int grpcPort)
    {
        return new PinotSplit(
                requireNonNull(connectorId, "connector id is null"),
                SplitType.SEGMENT,
                expectedColumnHandles,
                Optional.empty(),
                Optional.of(requireNonNull(segmentPinotQuery, "segmentPinotQuery is null")),
                requireNonNull(segments, "segments are null"),
                Optional.of(requireNonNull(segmentHost, "segmentHost is null")),
                Optional.of(grpcPort));
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public SplitType getSplitType()
    {
        return splitType;
    }

    @JsonProperty
    public Optional<PinotQueryGenerator.GeneratedPinotQuery> getBrokerPinotQuery()
    {
        return brokerPinotQuery;
    }

    @JsonProperty
    public Optional<String> getSegmentPinotQuery()
    {
        return segmentPinotQuery;
    }

    @JsonProperty
    public Optional<String> getSegmentHost()
    {
        return segmentHost;
    }

    @JsonProperty
    public List<String> getSegments()
    {
        return segments;
    }

    // Extract grpc host name from the segmentHost.
    // Usually segmentHost is in the format of `Server_127.0.0.1_8090`, so the hostname can be parsed from it, or will just use the entire string for that.
    public Optional<String> getGrpcHost()
    {
        if (segmentHost.isPresent()) {
            String[] hostSplits = segmentHost.get().split("_");
            return (hostSplits.length > 1) ? Optional.of(hostSplits[hostSplits.length - 2]) : segmentHost;
        }
        return Optional.empty();
    }

    @JsonProperty
    public Optional<Integer> getGrpcPort()
    {
        return grpcPort;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("splitType", splitType)
                .add("columnHandle", expectedColumnHandles)
                .add("segmentPinotQuery", segmentPinotQuery)
                .add("brokerPinotQuery", brokerPinotQuery)
                .add("segments", segments)
                .add("segmentHost", segmentHost)
                .toString();
    }

    @JsonProperty
    public List<PinotColumnHandle> getExpectedColumnHandles()
    {
        return expectedColumnHandles;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    public enum SplitType
    {
        SEGMENT,
        BROKER,
    }
}
