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
import com.google.common.collect.Streams;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * 2-dimensional Rtree created with STR bulk loading.
 * <p>
 * STR (Sort-Tile-Recurse) Rtrees are bulk-loaded query-only Rtrees that support very efficient
 * queries.  Intuitively, they "tile" the 2d space such that each tile contains
 * no more than max_children objects, and make a parent node for each tile.
 * This procedure is performed recursively on those parent nodes, and so on,
 * until a root node is reached.
 * <p>
 * Rtrees: https://en.wikipedia.org/wiki/R-tree
 * STR loading: https://www.researchgate.net/publication/3686660_STR_A_Simple_and_Efficient_Algorithm_for_R-Tree_Packing
 */
public class StrRTree
{

    private final int maxChildren;
    private final Node root;

    public StrRTree(List<OGCGeometry> geometries, int maxChildren)
    {
        this.maxChildren = maxChildren;
        List<Node> leaves = geometries.stream()
                .map(geom -> Node.newLeaf(GeometryUtils.getRectangle(geom), geom))
                .collect(Collectors.toList());
        this.root = RTreeUtils.bulkLoad(leaves, maxChildren);
    }

    public int getMaxChildren()
    {
        return maxChildren;
    }

    public Node getRoot()
    {
        return root;
    }

    public Stream<OGCGeometry> findEnvelopeIntersections(Rectangle envelope)
    {
        return Streams.stream(new IntersectionIterator(envelope, root));
    }

    public Stream<Neighbor> findNeighbors(OGCGeometry geometry)
    {
        return Streams.stream(new NeighborIterator(geometry, root));
    }

    public List<Neighbor> findNearestNeighbors(OGCGeometry geometry, int numNeighbors)
    {
        return findNeighbors(geometry).limit(numNeighbors).collect(Collectors.toList());
    }

    public List<Neighbor> findNeighborsWithin(OGCGeometry geometry, double distance)
    {
        // There's really no takeWhile function?
        List<Neighbor> neighborsWithin = new ArrayList<Neighbor>();
        Iterator<Neighbor> allNeighbors = new NeighborIterator(geometry, root);
        while (allNeighbors.hasNext()) {
            Neighbor neighbor = allNeighbors.next();
            if (neighbor.getDistance() > distance) {
                break;
            }
            neighborsWithin.add(neighbor);
        }
        return neighborsWithin;
    }

    public static final class Node
    {
        private final Rectangle extent;
        private final Optional<OGCGeometry> geometry;
        private final Optional<List<Node>> children;

        public Node(
                Rectangle extent,
                Optional<OGCGeometry> geometry,
                Optional<List<StrRTree.Node>> children)
        {
            this.extent = requireNonNull(extent, "extent is null");
            this.geometry = requireNonNull(geometry, "geometry is null");
            this.children = requireNonNull(children, "children is null");
            if (geometry.isPresent()) {
                checkArgument(!children.isPresent(), "Leaf node cannot have children");
            }
            else {
                checkArgument(children.isPresent(), "Internal node must have children");
            }
        }

        public static Node newLeaf(Rectangle extent, OGCGeometry geometry)
        {
            return new Node(extent, Optional.of(geometry), Optional.empty());
        }

        public static Node newInternal(Rectangle extent, List<Node> children)
        {
            return new Node(extent, Optional.empty(), Optional.of(children));
        }

        public Rectangle getExtent()
        {
            return extent;
        }

        public Optional<OGCGeometry> getGeometry()
        {
            return geometry;
        }

        public Optional<List<StrRTree.Node>> getChildren()
        {
            return children;
        }

        public boolean isLeaf()
        {
            return geometry.isPresent();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null) {
                return false;
            }

            if (!(obj instanceof StrRTree.Node)) {
                return false;
            }

            StrRTree.Node other = (StrRTree.Node) obj;
            return this.extent.equals(other.extent)
                    && Objects.equals(this.geometry, other.geometry)
                    && Objects.equals(this.children, other.children);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(extent, geometry, children);
        }
    }

    public static final class Neighbor
    {
        private final double distance;
        private final OGCGeometry geometry;

        public Neighbor(double distance, OGCGeometry geometry)
        {
            this.distance = distance;
            this.geometry = geometry;
        }

        public double getDistance()
        {
            return distance;
        }

        public OGCGeometry getGeometry()
        {
            return geometry;
        }
    }
}
