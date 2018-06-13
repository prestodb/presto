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
package com.facebook.presto.geospatial;

import com.esri.core.geometry.Envelope2D;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class KDBTree
{
    private final Envelope2D extent;
    private final int leafId;
    private final KDBTree[] children;

    public KDBTree(Envelope2D extent, int leafId)
    {
        checkState(leafId >= 0);
        this.extent = requireNonNull(extent, "extent is null");
        this.leafId = leafId;
        this.children = null;
    }

    public KDBTree(Envelope2D extent, KDBTree[] children)
    {
        this.extent = requireNonNull(extent, "extent is null");
        this.leafId = -1;
        this.children = requireNonNull(children, "children is null");
    }

    public Map<Integer, Envelope2D> getAllLeafs()
    {
        Map<Integer, Envelope2D> leafs = new HashMap<>();
        traverse(tree -> {
            if (tree.leafId >= 0) {
                leafs.put(tree.leafId, tree.extent);
            }
            return true;
        });
        return leafs;
    }

    public Map<Integer, Envelope2D> findLeafs(Envelope2D envelope)
    {
        Map<Integer, Envelope2D> matches = new HashMap<>();
        traverse(tree -> {
            if (!disjoint(tree.extent, envelope)) {
                if (tree.leafId >= 0) {
                    matches.put(tree.leafId, tree.extent);
                }
                return true;
            }
            else {
                return false;
            }
        });

        return matches;
    }

    public interface Visitor
    {
        /**
         * Visits a single node of the tree
         * @param tree Node to visit
         * @return true to continue traversing the tree; false to stop
         */
        boolean visit(KDBTree tree);
    }

    public void traverse(Visitor visitor)
    {
        if (!visitor.visit(this)) {
            return;
        }

        if (children != null) {
            for (KDBTree child : children) {
                child.traverse(visitor);
            }
        }
    }

    private static boolean disjoint(Envelope2D r1, Envelope2D r2)
    {
        return !r1.isIntersecting(r2) && !r1.contains(r2) && !r2.contains(r1);
    }

    public static KDBTree fromSpec(KDBTreeSpec spec)
    {
        return fromSpecNode(spec.getExtent(), spec.getRootNode());
    }

    private static KDBTree fromSpecNode(Envelope2D extent, KDBTreeSpec.KDBTreeNode node)
    {
        if (node.getLeafId() != null) {
            return new KDBTree(extent, node.getLeafId());
        }

        Envelope2D[] childExtents;
        if (node.getX() != null) {
            childExtents = splitAtX(extent, node.getX());
        }
        else {
            childExtents = splitAtY(extent, node.getY());
        }

        KDBTree[] children = new KDBTree[2];
        children[0] = fromSpecNode(childExtents[0], node.getChildren()[0]);
        children[1] = fromSpecNode(childExtents[1], node.getChildren()[1]);
        return new KDBTree(extent, children);
    }

    private static Envelope2D[] splitAtX(Envelope2D envelope, double x)
    {
        verify(envelope.xmin < x);
        verify(envelope.xmax > x);
        Envelope2D[] splits = new Envelope2D[2];
        splits[0] = new Envelope2D(envelope.xmin, envelope.ymin, x, envelope.ymax);
        splits[1] = new Envelope2D(x, envelope.ymin, envelope.xmax, envelope.ymax);
        return splits;
    }

    private static Envelope2D[] splitAtY(Envelope2D envelope, double y)
    {
        verify(envelope.ymin < y, format("%s <= %s", y, envelope.ymin));
        verify(envelope.ymax > y, format("%s >= %s", y, envelope.ymax));
        Envelope2D[] splits = new Envelope2D[2];
        splits[0] = new Envelope2D(envelope.xmin, envelope.ymin, envelope.xmax, y);
        splits[1] = new Envelope2D(envelope.xmin, y, envelope.xmax, envelope.ymax);
        return splits;
    }
}
