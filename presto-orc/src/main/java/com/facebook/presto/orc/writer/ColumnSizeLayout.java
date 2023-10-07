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

import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.stream.StreamDataOutput;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_MAP_FLAT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Order streams by total column size in the desc order preserving the type-tree
 * iteration order for complex types.
 * <p>
 * For flatmaps consider streams for the same flatmap key as a pseudo-column: do
 * additional grouping by sequence to keep streams for the same key together,
 * and then sort streams belonging to the same flatmap key by the total size in
 * desc order.
 */
public class ColumnSizeLayout
        implements StreamLayout
{
    @Override
    public void reorder(List<StreamDataOutput> dataStreams, Map<Integer, Integer> nodeToColumn, Map<Integer, ColumnEncoding> nodeIdToColumnEncodings)
    {
        requireNonNull(dataStreams, "dataStreams is null");
        requireNonNull(nodeToColumn, "nodeToColumn is null");
        requireNonNull(nodeIdToColumnEncodings, "nodeIdToColumnEncodings is null");

        if (dataStreams.isEmpty()) {
            return;
        }

        Set<Integer> flatMapColumns = getFlatMapColumns(nodeToColumn, nodeIdToColumnEncodings);

        // gather column sizes on the column and column+seq levels
        Map<Integer, long[]> columnSize = new HashMap<>();
        Map<ColumnSequenceKey, long[]> flatMapColumnSize = new HashMap<>();

        for (StreamDataOutput dataStream : dataStreams) {
            Stream stream = dataStream.getStream();
            int node = stream.getColumn();
            Integer column = nodeToColumn.get(node);

            long[] storedColumnSize = columnSize.computeIfAbsent(column, (n) -> new long[] {0});
            storedColumnSize[0] += dataStream.size();

            if (flatMapColumns.contains(column)) {
                ColumnSequenceKey key = new ColumnSequenceKey(column, stream.getSequence());
                long[] storedFlatMapColumnSize = flatMapColumnSize.computeIfAbsent(key, (n) -> new long[] {0});
                storedFlatMapColumnSize[0] += dataStream.size();
            }
        }

        // do the ordering
        dataStreams.sort((streamDataA, streamDataB) -> {
            Stream streamA = streamDataA.getStream();
            Stream streamB = streamDataB.getStream();

            int nodeA = streamA.getColumn();
            int nodeB = streamB.getColumn();

            int columnA = nodeToColumn.get(nodeA);
            int columnB = nodeToColumn.get(nodeB);

            boolean isFlatMapA = flatMapColumns.contains(columnA);
            boolean isFlatMapB = flatMapColumns.contains(columnB);

            // split non-flatmap and flatmap columns into separate groups
            if (isFlatMapA != isFlatMapB) {
                return Boolean.compare(isFlatMapA, isFlatMapB);
            }

            long columnSizeA = columnSize.get(columnA)[0];
            long columnSizeB = columnSize.get(columnB)[0];

            // order columns by total column size in desc order
            if (columnSizeA != columnSizeB) {
                return Long.compare(columnSizeB, columnSizeA);
            }

            // group streams by the column
            if (columnA != columnB) {
                return Integer.compare(columnA, columnB);
            }

            if (isFlatMapA) {
                int sequenceA = streamA.getSequence();
                int sequenceB = streamB.getSequence();

                // special handling for seq 0 before sorting by the col+seq size
                // to keep it on top of the group
                if (sequenceA != sequenceB) {
                    if (sequenceA == 0) {
                        return -1;
                    }
                    if (sequenceB == 0) {
                        return 1;
                    }
                }

                long columnSeqSizeA = flatMapColumnSize.get(new ColumnSequenceKey(columnA, sequenceA))[0];
                long columnSeqSizeB = flatMapColumnSize.get(new ColumnSequenceKey(columnB, sequenceB))[0];

                // order sequences by total column+seq size in desc order
                if (columnSeqSizeA != columnSeqSizeB) {
                    return Long.compare(columnSeqSizeB, columnSeqSizeA);
                }

                // group by the sequence
                if (sequenceA != sequenceB) {
                    return Integer.compare(sequenceA, sequenceB);
                }
            }

            // order by the node in asc order
            if (nodeA != nodeB) {
                return Integer.compare(nodeA, nodeB);
            }

            // sort by the stream kind, we don't really need it, but it makes testing easier
            return Integer.compare(streamA.getStreamKind().ordinal(), streamB.getStreamKind().ordinal());
        });
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .toString();
    }

    private static Set<Integer> getFlatMapColumns(Map<Integer, Integer> nodeIdToColumn, Map<Integer, ColumnEncoding> nodeIdToColumnEncodings)
    {
        Set<Integer> flatMapColumns = new HashSet<>();
        for (Map.Entry<Integer, ColumnEncoding> e : nodeIdToColumnEncodings.entrySet()) {
            Integer node = e.getKey();
            ColumnEncoding encoding = e.getValue();
            if (encoding.getColumnEncodingKind() == DWRF_MAP_FLAT) {
                flatMapColumns.add(nodeIdToColumn.get(node));
            }
        }
        return flatMapColumns;
    }
}
