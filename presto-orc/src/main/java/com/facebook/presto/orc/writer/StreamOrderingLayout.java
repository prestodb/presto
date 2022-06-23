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
package com.facebook.presto.orc.writer;

import com.facebook.presto.orc.DwrfStreamOrderingConfig;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.DwrfSequenceEncoding;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class StreamOrderingLayout
        implements StreamLayout
{
    private final DwrfStreamOrderingConfig config;
    private final StreamLayout nonStreamOrderingLayout;

    public StreamOrderingLayout(
            DwrfStreamOrderingConfig config,
            StreamLayout layout)
    {
        this.config = requireNonNull(config, "config cannot be null");
        this.nonStreamOrderingLayout = requireNonNull(layout, "layout cannot be null");
    }

    private static class StreamMetadata
    {
        // <Column Id, Sequence ID> -> List<Streams>>
        private final Map<ColumnSequenceInfo, List<StreamDataOutput>> sequenceToStreams;
        // <Column Id, KeyId> -> SequenceId
        private final Map<ColumnKeyInfo, Integer> keyToSequence;

        public StreamMetadata(Map<ColumnSequenceInfo, List<StreamDataOutput>> sequenceToStreams, Map<ColumnKeyInfo, Integer> keyToSequence)
        {
            this.sequenceToStreams = requireNonNull(sequenceToStreams, "sequenceToStreams cannot be null");
            this.keyToSequence = requireNonNull(keyToSequence, "keyToSequence cannot be null");
        }
    }

    private static class ColumnKeyInfo
    {
        private final int column;
        private final DwrfProto.KeyInfo key;

        public ColumnKeyInfo(int column, DwrfProto.KeyInfo key)
        {
            this.column = column;
            this.key = requireNonNull(key, "key cannot be null");
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof ColumnKeyInfo)) {
                return false;
            }
            ColumnKeyInfo input = (ColumnKeyInfo) obj;
            return this.column == input.column && this.key.equals(input.key);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(column, key);
        }
    }

    private static class ColumnSequenceInfo
    {
        private final int column;
        private final int sequence;

        public ColumnSequenceInfo(int column, int sequence)
        {
            this.column = column;
            this.sequence = sequence;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof ColumnSequenceInfo)) {
                return false;
            }
            ColumnSequenceInfo input = (ColumnSequenceInfo) obj;
            return this.column == input.column && this.sequence == input.sequence;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(column, sequence);
        }
    }

    private StreamMetadata getStreamMetadata(
            Map<Integer, Integer> nodeIdToColumn,
            Map<Integer, ColumnEncoding> nodeIdToColumnEncodings,
            DwrfStreamOrderingConfig config)
    {
        ImmutableMap.Builder<ColumnKeyInfo, Integer> keyToSequenceBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<ColumnSequenceInfo, List<StreamDataOutput>> sequenceToStreamsBuilder = ImmutableMap.builder();
        Map<Integer, Set<DwrfProto.KeyInfo>> columnToKeySet = config.getStreamOrdering();
        // Adding a set to track which of the columns in the reorder list are already visited
        // For complex maps (complex values for the value)
        // there could be multiple nodeId(s) mapping to a single column ID
        // For example,  if the flat map column is map<int, list<int>> with node ids (1: <2, 3<4>>) and column id 0
        // There will be multiple entries in the nodeIdToColumnEncodings for each of the values
        // 1 -> DWRF_MAP_FLAT encoding
        // 3 -> DIRECT encoding  + SortedMap<Integer, DwrfSequenceEncoding> (sequence encodings)
        // 4 -> DIRECT encoding  + SortedMap<Integer, DwrfSequenceEncoding> (sequence encodings)
        Set<Integer> columnsVisited = new HashSet<>(columnToKeySet.size());

        // iterate through all the encodings and if the encoding has additional sequence encodings put it in the map
        for (Map.Entry<Integer, ColumnEncoding> entry : nodeIdToColumnEncodings.entrySet()) {
            int nodeId = entry.getKey();
            int column = nodeIdToColumn.get(nodeId);
            if (entry.getValue().getAdditionalSequenceEncodings().isPresent() && columnToKeySet.containsKey(column) && !columnsVisited.contains(column)) {
                // add entries only if stream ordering contains the column ID
                Set<DwrfProto.KeyInfo> keysPerColumn = columnToKeySet.get(column);
                for (Map.Entry<Integer, DwrfSequenceEncoding> sequenceToEncoding : entry.getValue().getAdditionalSequenceEncodings().get().entrySet()) {
                    Integer sequence = sequenceToEncoding.getKey();
                    DwrfProto.KeyInfo key = sequenceToEncoding.getValue().getKey();
                    // add the stream only if it is present in the stream ordering config
                    if (keysPerColumn.contains(key)) {
                        keyToSequenceBuilder.put(new ColumnKeyInfo(column, key), sequence);
                        sequenceToStreamsBuilder.put(new ColumnSequenceInfo(column, sequence), new ArrayList<>());
                    }
                }
                columnsVisited.add(column);
            }
        }
        return new StreamMetadata(sequenceToStreamsBuilder.build(), keyToSequenceBuilder.build());
    }

    @Override
    public void reorder(
            List<StreamDataOutput> dataStreams,
            Map<Integer, Integer> nodeIdToColumn,
            Map<Integer, ColumnEncoding> nodeIdToColumnEncodings)
    {
        List<StreamDataOutput> nonReorderStreams = new ArrayList<>();
        StreamMetadata metadata = getStreamMetadata(nodeIdToColumn, nodeIdToColumnEncodings, config);
        Map<ColumnSequenceInfo, List<StreamDataOutput>> sequenceToStreams = metadata.sequenceToStreams;
        for (StreamDataOutput dataOutput : dataStreams) {
            int nodeId = dataOutput.getStream().getColumn();
            int sequence = dataOutput.getStream().getSequence();
            int column = nodeIdToColumn.get(nodeId);
            // only if sequence ID > 0, we do a look up in sequenceToStreams
            if (sequence > 0) {
                List<StreamDataOutput> streams = sequenceToStreams.get(new ColumnSequenceInfo(column, sequence));
                if (streams == null) {
                    nonReorderStreams.add(dataOutput);
                }
                else {
                    streams.add(dataOutput);
                }
            }
            else {
                nonReorderStreams.add(dataOutput);
            }
        }

        // reorder everything in the input order
        List<StreamDataOutput> orderedStreams = new ArrayList<>();
        Map<ColumnKeyInfo, Integer> keyToSequence = metadata.keyToSequence;
        for (Map.Entry<Integer, Set<DwrfProto.KeyInfo>> columnToKeys : config.getStreamOrdering().entrySet()) {
            int column = columnToKeys.getKey();
            for (DwrfProto.KeyInfo key : columnToKeys.getValue()) {
                ColumnKeyInfo columnKeyInfo = new ColumnKeyInfo(column, key);
                Integer sequence = keyToSequence.get(columnKeyInfo);
                if (sequence != null) {
                    ColumnSequenceInfo columnSequenceInfo = new ColumnSequenceInfo(column, sequence);
                    List<StreamDataOutput> groupedDataStreams = sequenceToStreams.get(columnSequenceInfo);
                    checkState(groupedDataStreams != null, "list of streams for a sequence cannot be null");
                    checkState(groupedDataStreams.size() > 0, "There should be at least one stream for a sequence");
                    orderedStreams.addAll(groupedDataStreams);
                }
            }
        }

        // do actual reordering
        nonStreamOrderingLayout.reorder(nonReorderStreams, ImmutableMap.of(), ImmutableMap.of());

        // add all the streams
        checkState(orderedStreams.size() + nonReorderStreams.size() == dataStreams.size(),
                "Number ordered + non ordered streams should be equal to total number of data streams " +
                "orderedStreams: %s, nonReorderStreams: %s, dataStreams: %s",
                orderedStreams.size(),
                nonReorderStreams.size(),
                dataStreams.size());
        dataStreams.clear();
        dataStreams.addAll(orderedStreams);
        dataStreams.addAll(nonReorderStreams);
    }
}
