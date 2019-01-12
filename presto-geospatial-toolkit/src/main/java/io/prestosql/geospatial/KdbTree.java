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
package io.prestosql.geospatial;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.geospatial.KdbTree.Node.newInternal;
import static io.prestosql.geospatial.KdbTree.Node.newLeaf;
import static java.util.Objects.requireNonNull;

/**
 * 2-dimensional K-D-B Tree
 * see https://en.wikipedia.org/wiki/K-D-B-tree
 */
public class KdbTree
{
    private static final int MAX_LEVELS = 10_000;

    private final Node root;

    public static final class Node
    {
        private final Rectangle extent;
        private final OptionalInt leafId;
        private final Optional<Node> left;
        private final Optional<Node> right;

        public static Node newLeaf(Rectangle extent, int leafId)
        {
            return new Node(extent, OptionalInt.of(leafId), Optional.empty(), Optional.empty());
        }

        public static Node newInternal(Rectangle extent, Node left, Node right)
        {
            return new Node(extent, OptionalInt.empty(), Optional.of(left), Optional.of(right));
        }

        @JsonCreator
        public Node(
                @JsonProperty("extent") Rectangle extent,
                @JsonProperty("leafId") OptionalInt leafId,
                @JsonProperty("left") Optional<Node> left,
                @JsonProperty("right") Optional<Node> right)
        {
            this.extent = requireNonNull(extent, "extent is null");
            this.leafId = requireNonNull(leafId, "leafId is null");
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
            if (leafId.isPresent()) {
                checkArgument(leafId.getAsInt() >= 0, "leafId must be >= 0");
                checkArgument(!left.isPresent(), "Leaf node cannot have left child");
                checkArgument(!right.isPresent(), "Leaf node cannot have right child");
            }
            else {
                checkArgument(left.isPresent(), "Intermediate node must have left child");
                checkArgument(right.isPresent(), "Intermediate node must have right child");
            }
        }

        @JsonProperty
        public Rectangle getExtent()
        {
            return extent;
        }

        @JsonProperty
        public OptionalInt getLeafId()
        {
            return leafId;
        }

        @JsonProperty
        public Optional<Node> getLeft()
        {
            return left;
        }

        @JsonProperty
        public Optional<Node> getRight()
        {
            return right;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null) {
                return false;
            }

            if (!(obj instanceof Node)) {
                return false;
            }

            Node other = (Node) obj;
            return this.extent.equals(other.extent)
                    && Objects.equals(this.leafId, other.leafId)
                    && Objects.equals(this.left, other.left)
                    && Objects.equals(this.right, other.right);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(extent, leafId, left, right);
        }
    }

    @JsonCreator
    public KdbTree(@JsonProperty("root") Node root)
    {
        this.root = requireNonNull(root, "root is null");
    }

    @JsonProperty
    public Node getRoot()
    {
        return root;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof KdbTree)) {
            return false;
        }

        KdbTree other = (KdbTree) obj;
        return this.root.equals(other.root);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(root);
    }

    public Map<Integer, Rectangle> getLeaves()
    {
        ImmutableMap.Builder<Integer, Rectangle> leaves = ImmutableMap.builder();
        addLeaves(root, leaves, node -> true);
        return leaves.build();
    }

    public Map<Integer, Rectangle> findIntersectingLeaves(Rectangle envelope)
    {
        ImmutableMap.Builder<Integer, Rectangle> leaves = ImmutableMap.builder();
        addLeaves(root, leaves, node -> node.extent.intersects(envelope));
        return leaves.build();
    }

    private static void addLeaves(Node node, ImmutableMap.Builder<Integer, Rectangle> leaves, Predicate<Node> predicate)
    {
        if (!predicate.apply(node)) {
            return;
        }

        if (node.leafId.isPresent()) {
            leaves.put(node.leafId.getAsInt(), node.extent);
        }
        else {
            addLeaves(node.left.get(), leaves, predicate);
            addLeaves(node.right.get(), leaves, predicate);
        }
    }

    private interface SplitDimension
    {
        Comparator<Rectangle> getComparator();

        double getValue(Rectangle rectangle);

        SplitResult<Rectangle> split(Rectangle rectangle, double value);
    }

    private static final SplitDimension BY_X = new SplitDimension()
    {
        private final Comparator<Rectangle> comparator = (first, second) -> ComparisonChain.start()
                .compare(first.getXMin(), second.getXMin())
                .compare(first.getYMin(), second.getYMin())
                .result();

        @Override
        public Comparator<Rectangle> getComparator()
        {
            return comparator;
        }

        @Override
        public double getValue(Rectangle rectangle)
        {
            return rectangle.getXMin();
        }

        @Override
        public SplitResult<Rectangle> split(Rectangle rectangle, double x)
        {
            checkArgument(rectangle.getXMin() < x && x < rectangle.getXMax());
            return new SplitResult<>(
                    new Rectangle(rectangle.getXMin(), rectangle.getYMin(), x, rectangle.getYMax()),
                    new Rectangle(x, rectangle.getYMin(), rectangle.getXMax(), rectangle.getYMax()));
        }
    };

    private static final SplitDimension BY_Y = new SplitDimension()
    {
        private final Comparator<Rectangle> comparator = (first, second) -> ComparisonChain.start()
                .compare(first.getYMin(), second.getYMin())
                .compare(first.getXMin(), second.getXMin())
                .result();

        @Override
        public Comparator<Rectangle> getComparator()
        {
            return comparator;
        }

        @Override
        public double getValue(Rectangle rectangle)
        {
            return rectangle.getYMin();
        }

        @Override
        public SplitResult<Rectangle> split(Rectangle rectangle, double y)
        {
            checkArgument(rectangle.getYMin() < y && y < rectangle.getYMax());
            return new SplitResult<>(
                    new Rectangle(rectangle.getXMin(), rectangle.getYMin(), rectangle.getXMax(), y),
                    new Rectangle(rectangle.getXMin(), y, rectangle.getXMax(), rectangle.getYMax()));
        }
    };

    private static final class LeafIdAllocator
    {
        private int nextId;

        public int next()
        {
            return nextId++;
        }
    }

    public static KdbTree buildKdbTree(int maxItemsPerNode, Rectangle extent, List<Rectangle> items)
    {
        checkArgument(maxItemsPerNode > 0, "maxItemsPerNode must be > 0");
        requireNonNull(extent, "extent is null");
        requireNonNull(items, "items is null");
        return new KdbTree(buildKdbTreeNode(maxItemsPerNode, 0, extent, items, new LeafIdAllocator()));
    }

    private static Node buildKdbTreeNode(int maxItemsPerNode, int level, Rectangle extent, List<Rectangle> items, LeafIdAllocator leafIdAllocator)
    {
        checkArgument(maxItemsPerNode > 0, "maxItemsPerNode must be > 0");
        checkArgument(level >= 0, "level must be >= 0");
        checkArgument(level <= MAX_LEVELS, "level must be <= 10,000");
        requireNonNull(extent, "extent is null");
        requireNonNull(items, "items is null");

        if (items.size() <= maxItemsPerNode || level == MAX_LEVELS) {
            return newLeaf(extent, leafIdAllocator.next());
        }

        // Split over longer side
        boolean splitVertically = extent.getWidth() >= extent.getHeight();
        Optional<SplitResult<Node>> splitResult = trySplit(splitVertically ? BY_X : BY_Y, maxItemsPerNode, level, extent, items, leafIdAllocator);
        if (!splitResult.isPresent()) {
            // Try spitting by the other side
            splitResult = trySplit(splitVertically ? BY_Y : BY_X, maxItemsPerNode, level, extent, items, leafIdAllocator);
        }

        if (!splitResult.isPresent()) {
            return newLeaf(extent, leafIdAllocator.next());
        }

        return newInternal(extent, splitResult.get().getLeft(), splitResult.get().getRight());
    }

    private static final class SplitResult<T>
    {
        private final T left;
        private final T right;

        private SplitResult(T left, T right)
        {
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
        }

        public T getLeft()
        {
            return left;
        }

        public T getRight()
        {
            return right;
        }
    }

    private static Optional<SplitResult<Node>> trySplit(SplitDimension splitDimension, int maxItemsPerNode, int level, Rectangle extent, List<Rectangle> items, LeafIdAllocator leafIdAllocator)
    {
        checkArgument(items.size() > 1, "Number of items to split must be > 1");

        // Sort envelopes by xMin or yMin
        List<Rectangle> sortedItems = ImmutableList.sortedCopyOf(splitDimension.getComparator(), items);

        // Find a mid-point
        int middleIndex = (sortedItems.size() - 1) / 2;
        Rectangle middleEnvelope = sortedItems.get(middleIndex);
        double splitValue = splitDimension.getValue(middleEnvelope);
        int splitIndex = middleIndex;

        // skip over duplicate values
        while (splitIndex < sortedItems.size() && splitDimension.getValue(sortedItems.get(splitIndex)) == splitValue) {
            splitIndex++;
        }

        // all values between left-of-middle and the end are the same, so can't split
        if (splitIndex == sortedItems.size()) {
            return Optional.empty();
        }

        // about half of the objects are <= splitValue, the rest are >= next value
        // assuming the input set of objects is a sample from a much larger set,
        // let's split in the middle; this way objects from the larger set with values
        // between splitValue and next value will get split somewhat evenly into left
        // and right partitions
        splitValue = (splitValue + splitDimension.getValue(sortedItems.get(splitIndex))) / 2;

        SplitResult<Rectangle> childExtents = splitDimension.split(extent, splitValue);

        return Optional.of(new SplitResult(
                buildKdbTreeNode(maxItemsPerNode, level + 1, childExtents.getLeft(), sortedItems.subList(0, splitIndex), leafIdAllocator),
                buildKdbTreeNode(maxItemsPerNode, level + 1, childExtents.getRight(), sortedItems.subList(splitIndex, sortedItems.size()), leafIdAllocator)));
    }
}
