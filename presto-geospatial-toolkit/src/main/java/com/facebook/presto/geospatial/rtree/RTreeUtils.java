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
package com.facebook.presto.geospatial.rtree;

import com.facebook.presto.geospatial.Rectangle;
import com.facebook.presto.geospatial.rtree.StrRTree.Node;
import com.google.common.collect.ComparisonChain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class RTreeUtils
{
    private static final Comparator<StrRTree.Node> xComparator = (first, second) -> ComparisonChain.start()
            .compare(first.getExtent().getXMin(), second.getExtent().getXMin())
            .compare(first.getExtent().getXMax(), second.getExtent().getXMax())
            .result();
    private static final Comparator<StrRTree.Node> yComparator = (first, second) -> ComparisonChain.start()
            .compare(first.getExtent().getYMin(), second.getExtent().getYMin())
            .compare(first.getExtent().getYMax(), second.getExtent().getYMax())
            .result();

    public static Node bulkLoad(List<Node> leaves, int maxChildren)
    {
        checkArgument(leaves.size() > 0, "Must have at least one leaf node.");
        List<Node> nodes = leaves;
        while (nodes.size() > maxChildren) {
            nodes = tileOneLevel(nodes, maxChildren);
        }
        return Node.newInternal(mergeRectangles(nodes), nodes);
    }

    private static List<Node> tileOneLevel(List<Node> children, int maxChildren)
    {
        List<Node> parents = new ArrayList<Node>();
        Collections.sort(children, xComparator);
        // First we slice children into vertical slices
        int sliceSize = (int) Math.ceil(Math.sqrt(1.0 * children.size() / maxChildren));
        for (int sliceStart = 0; sliceStart < children.size(); sliceStart += sliceSize) {
            int sliceEnd = Math.min(sliceStart + sliceSize, children.size());
            List<Node> verticalSlice = children.subList(sliceStart, sliceEnd);
            verticalSlice.sort(yComparator);
            // Now slice them into blocks with horizontal slices
            for (int blockStart = 0; blockStart < verticalSlice.size(); blockStart += sliceSize) {
                int blockEnd = Math.min(blockStart + sliceSize, verticalSlice.size());
                List<Node> blockNodes = verticalSlice.subList(blockStart, blockEnd);
                parents.add(Node.newInternal(mergeRectangles(blockNodes), blockNodes));
            }
        }
        return parents;
    }

    private static Rectangle mergeRectangles(List<Node> nodes)
    {
        checkArgument(nodes.size() > 0, "Must have at least one node.");
        Rectangle rectangle = null;
        for (Node node : nodes) {
            rectangle = rectangle == null ? node.getExtent() : rectangle.merge(node.getExtent());
        }
        return rectangle;
    }
}
