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
package com.facebook.presto.orc;

import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.DwrfSequenceEncoding;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.stream.StreamDataOutput;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StreamSizeHelper
{
    private final List<OrcType> orcTypes;

    // flag indicating whether to collect flat map key size stats or not
    private final boolean collectKeyStats;

    // contains the node of the flattened column, it does not contain sub-nodes
    private final Set<Integer> flatMapNodes;

    // contains the mapping of all map value nodes to the top-level (column) map
    // node for flattened nodes
    private final Int2IntMap flatMapNodeTrees;

    // contains self node sizes (not rolled up)
    private final long[] nodeSizes;

    // key sizes by flat map node
    private final Int2ObjectMap<Object2LongMap<DwrfProto.KeyInfo>> keySizes = new Int2ObjectOpenHashMap<>();

    public StreamSizeHelper(List<OrcType> orcTypes, Set<Integer> flatMapNodes, boolean mapStatisticsEnabled)
    {
        this.orcTypes = requireNonNull(orcTypes, "orcTypes is null");
        this.flatMapNodes = requireNonNull(flatMapNodes, "flattenedNodes is null");
        this.collectKeyStats = mapStatisticsEnabled && !flatMapNodes.isEmpty();
        this.nodeSizes = new long[orcTypes.size()];
        this.flatMapNodeTrees = buildFlattenedNodeTrees();
    }

    private Int2IntMap buildFlattenedNodeTrees()
    {
        Int2IntMap flattenedNodeTrees = new Int2IntOpenHashMap();
        if (!collectKeyStats) {
            return flattenedNodeTrees;
        }

        // map all map values nodes to their top-level map node
        for (Integer mapNode : flatMapNodes) {
            OrcType mapType = orcTypes.get(mapNode);
            checkArgument(mapType.getOrcTypeKind() == OrcType.OrcTypeKind.MAP, "flat map node %s must be a map, but was %s", mapNode, mapType.getOrcTypeKind());
            checkArgument(mapType.getFieldCount() == 2, "flat map node %s must have exactly 2 sub-fields but had %s", mapNode, mapType.getFieldCount());
            int mapValueNode = mapType.getFieldTypeIndex(1);
            IntList deepValueNodes = collectDeepTreeNodes(orcTypes, mapValueNode);
            deepValueNodes.intStream().forEach(valueNode -> flattenedNodeTrees.put(valueNode, mapNode.intValue()));
        }

        return flattenedNodeTrees;
    }

    public void collectStreamSizes(Iterable<StreamDataOutput> streamDataOutputs, Map<Integer, ColumnEncoding> columnEncodings)
    {
        // collect node sizes first
        for (StreamDataOutput streamDataOutput : streamDataOutputs) {
            requireNonNull(streamDataOutput, "streamDataOutput is null");
            Stream stream = streamDataOutput.getStream();
            int node = stream.getColumn();
            nodeSizes[node] += streamDataOutput.size();
        }

        // collect map key sizes only if flat maps and map statistics are enabled
        if (collectKeyStats) {
            // flatMapNodeSizes contains total stream sizes by flat map node and sequence,
            // all value sub-nodes are mapped to the flat map node
            Int2ObjectMap<Int2LongMap> flatMapNodeSizes = new Int2ObjectOpenHashMap<>();

            // collect stream sizes aggregated by flat map node and sequence
            for (StreamDataOutput streamDataOutput : streamDataOutputs) {
                Stream stream = streamDataOutput.getStream();
                int node = stream.getColumn();

                // check if this node belongs to the flat map tree
                int flatMapNode = flatMapNodeTrees.getOrDefault(node, -1);
                if (flatMapNode != -1) {
                    Int2LongMap sequenceToSize = flatMapNodeSizes.computeIfAbsent(flatMapNode, Int2LongOpenHashMap::new);
                    sequenceToSize.mergeLong(stream.getSequence(), stream.getLength(), Long::sum);
                }
            }

            // merge stripe level sizes into the file level sizes
            for (int flatMapNode : flatMapNodeSizes.keySet()) {
                int flatMapValueNode = orcTypes.get(flatMapNode).getFieldTypeIndex(1);
                ColumnEncoding columnEncoding = columnEncodings.get(flatMapValueNode);
                checkArgument(columnEncoding != null, "columnEncoding for flat map node %s is null", flatMapNode);
                checkArgument(columnEncoding.getAdditionalSequenceEncodings().isPresent(), "columnEncoding for flat map node %s does not have keys", flatMapNode);

                SortedMap<Integer, DwrfSequenceEncoding> sequenceToKey = columnEncoding.getAdditionalSequenceEncodings().get();
                Int2LongMap sequenceToSize = flatMapNodeSizes.get(flatMapNode);
                Object2LongMap<DwrfProto.KeyInfo> keyToSize = keySizes.computeIfAbsent(flatMapNode, (ignore) -> new Object2LongOpenHashMap<>());

                // set the flat map node storage size in the map column statistics
                for (Map.Entry<Integer, DwrfSequenceEncoding> entry : sequenceToKey.entrySet()) {
                    int sequence = entry.getKey();
                    DwrfProto.KeyInfo key = entry.getValue().getKey();
                    long size = sequenceToSize.getOrDefault(sequence, 0);
                    keyToSize.mergeLong(key, size, Long::sum);
                }
            }
        }
    }

    /**
     * Returns flat map key sizes by flat map node.
     */
    public Int2ObjectMap<Object2LongMap<DwrfProto.KeyInfo>> getMapKeySizes()
    {
        return keySizes;
    }

    /**
     * Returns rolled up node sizes.
     */
    public Int2LongMap getNodeSizes()
    {
        Int2LongMap result = new Int2LongOpenHashMap(nodeSizes.length);
        rollupNodeSizes(result, 0);
        return result;
    }

    private long rollupNodeSizes(Int2LongMap result, int node)
    {
        long size = nodeSizes[node];
        List<Integer> subFieldIndexes = orcTypes.get(node).getFieldTypeIndexes();
        for (Integer subNode : subFieldIndexes) {
            size += rollupNodeSizes(result, subNode);
        }
        result.put(node, size);
        return size;
    }

    private static IntList collectDeepTreeNodes(List<OrcType> orcTypes, int startNode)
    {
        IntList result = new IntArrayList();
        result.add(startNode);

        for (int i = 0; i < result.size(); i++) {
            int node = result.getInt(i);
            OrcType orcType = orcTypes.get(node);
            for (int j = 0; j < orcType.getFieldCount(); j++) {
                result.add(orcType.getFieldTypeIndex(j));
            }
        }

        return result;
    }
}
