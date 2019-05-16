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
import com.facebook.presto.geospatial.GeometryUtils;
import com.facebook.presto.geospatial.Rectangle;
import com.facebook.presto.geospatial.rtree.StrRTree.Neighbor;
import com.facebook.presto.geospatial.rtree.StrRTree.Node;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.PriorityQueue;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Iterate over the entries in an RTree in order of increasing distance.
 *
 * All entries will be iterated over if the iterator is exhausted.
 *
 * It works by using a min-heap sorted by distance-squared.  The key
 * observation is that distance(a.envelope, b.envelope) <= distance(a, b).
 * For RTree nodes, we can calculate relatively cheap envelope-envelope
 * distances.  Each node is expanded into their children, which are re-inserted
 * into the heap.  When a Leaf node is pulled from the heap, the distance
 * between the actual query geometry and the geometry contained in the leaf
 * node is calculated; the `actualDistance` flag is set to indicate this the true
 * distance and not the envelope lower-bound.  Thus when an entry with
 * `actualDistance` is pulled, it must be the nearest geometry, and it can
 * be yielded.
 */
public class NeighborIterator
        implements Iterator<Neighbor>
{
    public static final Comparator<Entry> entryComparator = (first, second) -> Double.compare(first.distance2, second.distance2);
    private final PriorityQueue<Entry> heap = new PriorityQueue(1, entryComparator);
    private final Rectangle queryEnvelope;
    private final OGCGeometry queryGeometry;
    private Optional<Neighbor> nextCandidate = Optional.empty();

    public NeighborIterator(OGCGeometry geometry, Node root)
    {
        queryEnvelope = GeometryUtils.getRectangle(geometry);
        queryGeometry = geometry;
        heap.add(new Entry(queryEnvelope.distance2(root.getExtent()), root));
        advance();
    }

    public boolean hasNext()
    {
        return nextCandidate.isPresent();
    }

    public Neighbor next()
    {
        Neighbor neighbor = nextCandidate.get();
        nextCandidate = Optional.empty();
        advance();
        return neighbor;
    }

    private void advance()
    {
        while (!heap.isEmpty()) {
            Entry entry = heap.poll();
            Node node = entry.getNode();
            if (entry.isActualDistance()) {
                checkArgument(node.isLeaf(), "Non-leaf node has actual distance set.");
                nextCandidate = Optional.of(new Neighbor(Math.sqrt(entry.getDistance2()), node.getGeometry().get()));
                break;
            }
            if (node.isLeaf()) {
                double distance = queryGeometry.distance(node.getGeometry().get());
                entry.setDistance2(distance * distance);
                entry.setActualDistance(true);
                heap.add(entry);
            }
            else {
                node.getChildren().get().stream().forEach(c -> heap.add(new Entry(
                        queryEnvelope.distance2(c.getExtent()), c
                )));
            }
        }
    }

    public static final class Entry
    {
        private final Node node;
        private double distance2;
        // Leaf nodes will first be inserted with Envelope distance, then actual distance.
        private boolean actualDistance = false;

        public Entry(double distance2, Node node)
        {
            this.distance2 = distance2;
            this.node = node;
        }

        public double getDistance2()
        {
            return distance2;
        }

        public void setDistance2(double distance2)
        {
            this.distance2 = distance2;
        }

        public boolean isActualDistance()
        {
            return actualDistance;
        }

        public void setActualDistance(boolean actualDistance)
        {
            this.actualDistance = actualDistance;
        }

        public Node getNode()
        {
            return node;
        }
    }
}
