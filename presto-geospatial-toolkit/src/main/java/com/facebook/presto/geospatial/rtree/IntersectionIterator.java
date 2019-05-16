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

import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.geospatial.Rectangle;
import com.facebook.presto.geospatial.rtree.StrRTree.Node;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Optional;

/**
 * Iterate over all entries in the RTree with envelope intersection.
 *
 * This only checks that the envelopes intersect; the caller must filter by
 * actual intersection if desired.  There is no guarantee of order.
 */
public class IntersectionIterator
        implements Iterator<OGCGeometry>
{
    private final Deque<Node> stack = new ArrayDeque<Node>();
    private final Rectangle queryEnvelope;
    private Optional<OGCGeometry> nextCandidate = Optional.empty();

    public IntersectionIterator(Rectangle queryEnvelope, Node root)
    {
        stack.addFirst(root);
        this.queryEnvelope = queryEnvelope;
        advance();
    }

    public boolean hasNext()
    {
        return nextCandidate.isPresent();
    }

    public OGCGeometry next()
    {
        OGCGeometry next = nextCandidate.get();
        nextCandidate = Optional.empty();
        advance();
        return next;
    }

    private void advance()
    {
        // ??? Check that nextCandidate is empty?
        while (stack.size() > 0) {
            Node node = stack.removeFirst();
            if (!queryEnvelope.intersects(node.getExtent())) {
                continue;
            }
            if (node.isLeaf()) {
                nextCandidate = node.getGeometry();
                break;
            }
            else {
                node.getChildren().get().stream().forEach(child -> stack.addFirst(child));
            }
        }
    }
}

