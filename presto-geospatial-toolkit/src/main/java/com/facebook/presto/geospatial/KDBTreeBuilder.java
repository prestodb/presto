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

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Point;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static java.lang.String.format;

/**
 * see https://en.wikipedia.org/wiki/K-D-B-tree
 */
public class KDBTreeBuilder
{
    private final int maxItemsPerNode;
    private final int maxLevels;
    private final Envelope extent;
    private final int level;
    private final List<Envelope> items = new ArrayList<>();
    private KDBTreeBuilder[] children;
    private boolean splitX;
    private double splitValue;
    private int leafId;

    public KDBTreeBuilder(int maxItemsPerNode, int maxLevels, Envelope extent)
    {
        this(maxItemsPerNode, maxLevels, 0, extent);
    }

    private KDBTreeBuilder(int maxItemsPerNode, int maxLevels, int level, Envelope extent)
    {
        this.maxItemsPerNode = maxItemsPerNode;
        this.maxLevels = maxLevels;
        this.level = level;
        this.extent = extent;
    }

    public KDBTreeSpec build()
    {
        assignLeafIds();
        return new KDBTreeSpec(extent.getXMin(), extent.getYMin(), extent.getXMax(), extent.getYMax(), buildNode());
    }

    private KDBTreeSpec.KDBTreeNode buildNode()
    {
        if (children == null) {
            return new KDBTreeSpec.KDBTreeNode(leafId);
        }

        KDBTreeSpec.KDBTreeNode[] childNodes = new KDBTreeSpec.KDBTreeNode[2];
        childNodes[0] = children[0].buildNode();
        childNodes[1] = children[1].buildNode();
        return new KDBTreeSpec.KDBTreeNode(splitX ? splitValue : null, splitX ? null : splitValue, childNodes, null);
    }

    public boolean isLeaf()
    {
        return children == null;
    }

    public void insert(Envelope envelope)
    {
        if (items.size() < maxItemsPerNode || level >= maxLevels) {
            items.add(envelope);
        }
        else {
            if (children == null) {
                // Split over longer side
                boolean splitX = extent.getWidth() > extent.getHeight();
                boolean ok = split(splitX);
                if (!ok) {
                    // Try spitting by the other side
                    ok = split(!splitX);
                }

                if (!ok) {
                    // This could happen if all envelopes are the same.
                    items.add(envelope);
                    return;
                }
            }

            for (KDBTreeBuilder child : children) {
                if (child.extent.contains(new Point(envelope.getXMin(), envelope.getYMin()))) {
                    child.insert(envelope);
                    break;
                }
            }
        }
    }

    public interface Visitor
    {
        /**
         * Visits a single node of the tree
         * @param tree Node to visit
         * @return true to continue traversing the tree; false to stop
         */
        boolean visit(KDBTreeBuilder tree);
    }

    /**
     * Traverses the tree top-down breadth-first and calls the visitor
     * for each node. Stops traversing if a call to Visitor.visit returns false.
     */
    public void traverse(KDBTreeBuilder.Visitor visitor)
    {
        if (!visitor.visit(this)) {
            return;
        }

        if (children != null) {
            for (KDBTreeBuilder child : children) {
                child.traverse(visitor);
            }
        }
    }

    public void assignLeafIds()
    {
        traverse(new KDBTreeBuilder.Visitor() {
            int id;

            @Override
            public boolean visit(KDBTreeBuilder tree)
            {
                if (tree.isLeaf()) {
                    tree.leafId = id;
                    id++;
                }
                return true;
            }
        });
    }

    private boolean split(boolean splitX)
    {
        final Comparator<Envelope> comparator = splitX ? new KDBTreeBuilder.XComparator() : new KDBTreeBuilder.YComparator();
        Collections.sort(items, comparator);

        final Envelope[] splits;
        final KDBTreeBuilder.Splitter splitter;
        Envelope middleItem = items.get((int) Math.floor(items.size() / 2));
        if (splitX) {
            double x = middleItem.getXMin();
            if (x > extent.getXMin() && x < extent.getXMax()) {
                splits = splitAtX(extent, x);
                splitter = new KDBTreeBuilder.XSplitter(x);
            }
            else {
                // Too many objects are crowded at the edge of the extent. Can't split.
                return false;
            }
        }
        else {
            double y = middleItem.getYMin();
            if (y > extent.getYMin() && y < extent.getYMax()) {
                splits = splitAtY(extent, y);
                splitter = new KDBTreeBuilder.YSplitter(y);
            }
            else {
                // Too many objects are crowded at the edge of the extent. Can't split.
                return false;
            }
        }

        if (splitter instanceof XSplitter) {
            this.splitX = true;
            this.splitValue = ((XSplitter) splitter).x;
        }
        else {
            this.splitX = false;
            this.splitValue = ((YSplitter) splitter).y;
        }

        children = new KDBTreeBuilder[2];
        children[0] = new KDBTreeBuilder(maxItemsPerNode, maxLevels, level + 1, splits[0]);
        children[1] = new KDBTreeBuilder(maxItemsPerNode, maxLevels, level + 1, splits[1]);

        // Move items
        splitItems(splitter);
        return true;
    }

    private static final class XComparator
            implements Comparator<Envelope>
    {
        @Override
        public int compare(Envelope o1, Envelope o2)
        {
            double deltaX = o1.getXMin() - o2.getXMin();
            return (int) Math.signum(deltaX != 0 ? deltaX : o1.getYMin() - o2.getYMin());
        }
    }

    private static final class YComparator
            implements Comparator<Envelope>
    {
        @Override
        public int compare(Envelope o1, Envelope o2)
        {
            double deltaY = o1.getYMin() - o2.getYMin();
            return (int) Math.signum(deltaY != 0 ? deltaY : o1.getXMin() - o2.getXMin());
        }
    }

    private interface Splitter
    {
        /**
         * @return true if the specified envelope belongs to the lower split
         */
        boolean split(Envelope envelope);
    }

    private static final class XSplitter
            implements KDBTreeBuilder.Splitter
    {
        private final double x;

        private XSplitter(double x)
        {
            this.x = x;
        }

        @Override
        public boolean split(Envelope envelope)
        {
            return envelope.getXMin() <= x;
        }
    }

    private static final class YSplitter
            implements KDBTreeBuilder.Splitter
    {
        private final double y;

        private YSplitter(double y)
        {
            this.y = y;
        }

        @Override
        public boolean split(Envelope envelope)
        {
            return envelope.getYMin() <= y;
        }
    }

    private void splitItems(KDBTreeBuilder.Splitter splitter)
    {
        for (Envelope item : items) {
            children[splitter.split(item) ? 0 : 1].insert(item);
        }
    }

    private static Envelope[] splitAtX(Envelope envelope, double x)
    {
        verify(envelope.getXMin() < x);
        verify(envelope.getXMax() > x);
        Envelope[] splits = new Envelope[2];
        splits[0] = new Envelope(envelope.getXMin(), envelope.getYMin(), x, envelope.getYMax());
        splits[1] = new Envelope(x, envelope.getYMin(), envelope.getXMax(), envelope.getYMax());
        return splits;
    }

    private static Envelope[] splitAtY(Envelope envelope, double y)
    {
        verify(envelope.getYMin() < y, format("%s <= %s", y, envelope.getYMin()));
        verify(envelope.getYMax() > y, format("%s >= %s", y, envelope.getYMax()));
        Envelope[] splits = new Envelope[2];
        splits[0] = new Envelope(envelope.getXMin(), envelope.getYMin(), envelope.getXMax(), y);
        splits[1] = new Envelope(envelope.getXMin(), y, envelope.getXMax(), envelope.getYMax());
        return splits;
    }
}
