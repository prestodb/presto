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

import com.facebook.presto.common.array.DoubleBigArray;
import com.facebook.presto.geospatial.Rectangle;
import com.google.common.annotations.VisibleForTesting;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/**
 * A fast, low memory footprint static RTree.
 * <p>
 * Packed Hilbert RTrees -- aka Flatbushes -- create very few objects, instead
 * storing the tree in a flat array.  They support the standard RTree queries,
 * but cannot be modified once built. It is quite possible to semi-efficiently
 * remove objects.
 * <p>
 * Consider an RTree with branching factor `b`.  Each non-leaf node has two
 * pieces of information:
 * 1. The minimum bounding box of all descendants, and
 * 2. Pointers to its children.
 * <p>
 * The former is simply four doubles.  The latter can be derived from the
 * node's level and which sibling it is.  This means we can actually flatten
 * the tree into a single array of doubles, four per node.  We can
 * programmatically find the indices of a node's children (it will be faster if
 * we pre-compute level offsets), and do envelope checks with just float
 * operations.  "padded" empty nodes will have NaN entries, which will naturally
 * return false for all comparison operators, thus being automatically not
 * selected.
 * <p>
 * A critical choice in RTree implementation is how to group leaf nodes as
 * children (and recursively, their parents).  One method that is very efficient
 * to construct and comparable to best lookup performance is sorting by an
 * object's Hilbert curve index.  Hilbert curves are naturally hierarchical, so
 * successively grouping children and their parents will give a naturally nested
 * structure.  This means we only need to sort the items once.
 * <p>
 * If sort time is a problem, we actually just need to "partition" into groups
 * of `degree`, since the order of the children of a single parent doesn't
 * matter.  This could be done with quicksort, stopping once all items at index
 * `n * degree` are correctly placed.
 * <p>
 * Original implementation in Javascript: https://github.com/mourner/flatbush
 */
public class Flatbush<T extends HasExtent>
{
    // Number of coordinates to define an envelope
    @VisibleForTesting
    static final int ENVELOPE_SIZE = 4;
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Flatbush.class).instanceSize();
    private static final int DEFAULT_DEGREE = 16;
    // Number of children per node
    private final int degree;
    // Offsets in tree for each level
    private final int[] levelOffsets;
    // Each node has four doubles: xMin, yMin, xMax, yMax
    private final DoubleBigArray tree;
    private final T[] items;

    /**
     * Build Flatbush RTree for `items`.
     *
     * @param items Items to index.
     * @param degree Number of children for each intermediate node.
     */
    public Flatbush(T[] items, int degree)
    {
        checkArgument(degree > 0, "degree must be positive");
        this.degree = degree;
        this.items = requireNonNull(items, "items is null");
        this.levelOffsets = calculateLevelOffsets(items.length, degree);
        this.tree = buildTree();
    }

    /**
     * Build Flatbush RTree for `items` with default number of children per node.
     * <p>
     * This will use the default degree.
     *
     * @param items Items to index.
     */
    public Flatbush(T[] items)
    {
        this(items, DEFAULT_DEGREE);
    }

    /**
     * Calculate the indices for each level.
     *
     * A given level contains a certain number of items.  We give it a capacity
     * equal to the next multiple of `degree`, so that each parent will have
     * an equal number (`degree`) of children.  This means the next level will
     * have `capacity/degree` nodes, which yields a capacity equal to the next
     * multiple, and so on, until the number of items is 1.
     *
     * Since we are storing each node as 4 doubles, we will actually multiply
     * every index by 4.
     */
    private static int[] calculateLevelOffsets(int numItems, int degree)
    {
        List<Integer> offsets = new ArrayList<>();
        // Leaf nodes start at 0, root is the last element.
        offsets.add(0);
        int level = 0;
        while (numItems > 1) {
            // The number of children will be the smallest multiple of degree >= numItems
            int numChildren = (int) Math.ceil(1.0 * numItems / degree) * degree;
            offsets.add(offsets.get(level) + ENVELOPE_SIZE * numChildren);
            numItems = numChildren / degree;
            level += 1;
        }
        return offsets.stream().mapToInt(Integer::intValue).toArray();
    }

    private DoubleBigArray buildTree()
    {
        // We initialize it to NaN, because all comparisons with NaN are false.
        // Thus the normal intersection logic will not select uninitialized
        // nodes.
        DoubleBigArray tree = new DoubleBigArray(Double.NaN);
        tree.ensureCapacity(levelOffsets[levelOffsets.length - 1] + ENVELOPE_SIZE);

        if (items.length > degree) {
            sortByHilbertIndex(items);
        }

        int writeOffset = 0;
        for (T item : items) {
            tree.set(writeOffset++, item.getExtent().getXMin());
            tree.set(writeOffset++, item.getExtent().getYMin());
            tree.set(writeOffset++, item.getExtent().getXMax());
            tree.set(writeOffset++, item.getExtent().getYMax());
        }

        int numChildren = items.length;
        for (int level = 0; level < levelOffsets.length - 1; level++) {
            int readOffset = levelOffsets[level];
            writeOffset = levelOffsets[level + 1];
            int numParents = 0;
            double xMin = Double.POSITIVE_INFINITY;
            double yMin = Double.POSITIVE_INFINITY;
            double xMax = Double.NEGATIVE_INFINITY;
            double yMax = Double.NEGATIVE_INFINITY;
            int child = 0;
            for (; child < numChildren; child++) {
                xMin = min(xMin, tree.get(readOffset++));
                yMin = min(yMin, tree.get(readOffset++));
                xMax = max(xMax, tree.get(readOffset++));
                yMax = max(yMax, tree.get(readOffset++));

                if ((child + 1) % degree == 0) {
                    numParents++;
                    tree.set(writeOffset++, xMin);
                    tree.set(writeOffset++, yMin);
                    tree.set(writeOffset++, xMax);
                    tree.set(writeOffset++, yMax);
                    xMin = Double.POSITIVE_INFINITY;
                    yMin = Double.POSITIVE_INFINITY;
                    xMax = Double.NEGATIVE_INFINITY;
                    yMax = Double.NEGATIVE_INFINITY;
                }
            }

            if (child % degree != 0) {
                numParents++;
                tree.set(writeOffset++, xMin);
                tree.set(writeOffset++, yMin);
                tree.set(writeOffset++, xMax);
                tree.set(writeOffset++, yMax);
            }
            numChildren = numParents;
        }

        return tree;
    }

    /**
     * Find intersection candidates for `query` rectangle.
     * <p>
     * This will feed to `consumer` each object in the rtree whose bounding
     * rectangle intersects the query rectangle.  The actual intersection
     * check will need to be performed by the caller.
     *
     * @param query Rectangle for which to search for intersection.
     * @param consumer Function to call for each intersection candidate.
     */
    public void findIntersections(Rectangle query, Consumer<T> consumer)
    {
        IntArrayList todoNodes = new IntArrayList(levelOffsets.length * degree);
        IntArrayList todoLevels = new IntArrayList(levelOffsets.length * degree);

        int rootLevel = levelOffsets.length - 1;
        int rootIndex = levelOffsets[rootLevel];
        if (doesIntersect(query, rootIndex)) {
            todoNodes.push(rootIndex);
            todoLevels.push(rootLevel);
        }

        while (!todoNodes.isEmpty()) {
            int nodeIndex = todoNodes.popInt();
            int level = todoLevels.popInt();

            if (level == 0) {
                // This is a leaf node
                consumer.accept(items[nodeIndex / ENVELOPE_SIZE]);
            }
            else {
                int childrenOffset = getChildrenOffset(nodeIndex, level);
                for (int i = 0; i < degree; i++) {
                    int childIndex = childrenOffset + ENVELOPE_SIZE * i;
                    if (doesIntersect(query, childIndex)) {
                        todoNodes.push(childIndex);
                        todoLevels.push(level - 1);
                    }
                }
            }
        }
    }

    private boolean doesIntersect(Rectangle query, int nodeIndex)
    {
        return query.getXMax() >= tree.get(nodeIndex) // xMin
                && query.getYMax() >= tree.get(nodeIndex + 1) // yMin
                && query.getXMin() <= tree.get(nodeIndex + 2) // xMax
                && query.getYMin() <= tree.get(nodeIndex + 3); // yMax
    }

    /**
     * Get the offset of the first child for the node.
     *
     * @param nodeIndex Index in tree of first entry for node
     * @param level Level of node
     */
    @VisibleForTesting
    int getChildrenOffset(int nodeIndex, int level)
    {
        int indexInLevel = nodeIndex - levelOffsets[level];
        return levelOffsets[level - 1] + degree * indexInLevel;
    }

    @VisibleForTesting
    int getHeight()
    {
        return levelOffsets.length;
    }

    /*
     * Sorts items in-place by the Hilbert index of the envelope center.
     */
    private void sortByHilbertIndex(T[] items)
    {
        if (items == null || items.length < 2) {
            return;
        }

        Rectangle totalExtent = items[0].getExtent();
        for (int i = 1; i < items.length; i++) {
            totalExtent = totalExtent.merge(items[i].getExtent());
        }

        HilbertIndex hilbert = new HilbertIndex(totalExtent);
        Arrays.parallelSort(items, Comparator.comparing(item ->
                hilbert.indexOf(
                        (item.getExtent().getXMin() + item.getExtent().getXMax()) / 2,
                        (item.getExtent().getYMin() + item.getExtent().getYMax()) / 2)));
    }

    public boolean isEmpty()
    {
        return items.length == 0;
    }

    public long getEstimatedSizeInBytes()
    {
        long result = INSTANCE_SIZE + sizeOf(levelOffsets) + tree.sizeOf();
        for (T item : items) {
            result += item.getEstimatedSizeInBytes();
        }
        return result;
    }
}
