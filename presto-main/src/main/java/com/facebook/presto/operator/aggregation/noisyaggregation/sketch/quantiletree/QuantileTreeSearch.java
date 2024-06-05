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
package com.facebook.presto.operator.aggregation.noisyaggregation.sketch.quantiletree;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.math.IntMath;

import java.util.Arrays;

/**
 * A class used for traversing and querying QuantileTrees, including denoising and quantile estimation
 */
public class QuantileTreeSearch
{
    /**
     * Largest number of leaf nodes of subtree to traverse to denoise any single node's value
     */
    private static final int MAX_DENOISING_LEAVES = 64;

    private final QuantileTree tree;
    private final int maxDenoisingTraversalDepth;

    public QuantileTreeSearch(QuantileTree tree)
    {
        this.tree = tree;
        maxDenoisingTraversalDepth = findMaxTraversalDepth(tree.getBranchingFactor());
    }

    @VisibleForTesting
    static int findMaxTraversalDepth(int branchingFactor)
    {
        int depth = 1;
        int nodes = branchingFactor;
        while (nodes <= MAX_DENOISING_LEAVES) {
            depth++;
            nodes *= branchingFactor;
        }
        return depth - 1;
    }

    /**
     * Returns an estimate of the total number of items in the tree, using the denoised root node value
     */
    public double cardinality()
    {
        return query(rootNode());
    }

    @VisibleForTesting
    int getBranchingFactor()
    {
        return tree.getBranchingFactor();
    }

    /**
     * Return the raw estimate from the node (no denoising)
     */
    @VisibleForTesting
    double queryDirect(NodeAddress node)
    {
        return tree.getLevel(node.level()).query(node.position());
    }

    /**
     * Estimate node value, traversing through some descendants to apply inverse-variance weighting.
     * The inverse-variance weighting is performed as described in "Understanding Hierarchical
     * Methods for Differentially Private Histograms" by Qardaji et al.
     * (https://www.vldb.org/pvldb/vol6/p1954-qardaji.pdf).
     * @return a pair of floats: estimate and relative variance
     */
    @VisibleForTesting
    double[] queryDown(NodeAddress node, int depth)
    {
        double localEst = queryDirect(node);

        // Return the node value with a relative variance of 1 if depth == 0 or if we're on the bottom level.
        if (depth == 0 || node.isLeaf()) {
            return new double[] {localEst, 1};
        }

        // Else return a weighted average of the node value and the estimates derived from its children.
        double estimateSum = 0;
        double childVariance = 0;
        for (NodeAddress child : node.children()) {
            double[] est = queryDown(child, depth - 1);
            estimateSum += est[0];
            childVariance = est[1];
        }
        double variance = tree.getBranchingFactor() * childVariance / (tree.getBranchingFactor() * childVariance + 1);
        double weight = 1 / (tree.getBranchingFactor() * childVariance);
        double estimate = (localEst + weight * estimateSum) / (1 + weight);
        return new double[] {estimate, variance};
    }

    /**
     * Return a denoised estimate of the node value based on the node and some of its descendants
     */
    @VisibleForTesting
    double query(NodeAddress node)
    {
        return queryDown(node, maxDenoisingTraversalDepth)[0];
    }

    /**
     * Traverse the tree to find the leaf node corresponding to the p-th quantile
     * @return Canonical bin corresponding to the p-th quantile
     */
    public long findQuantile(double p)
    {
        // Start at root, where the left bound is the p=0-th quantile,
        // and the right bound is the p=1-th (100%-th) quantile.
        NodeAddress node = rootNode();
        double left = 0;
        double right = 1;
        double nodeCount = cardinality();

        while (!node.isLeaf()) {
            NodeAddress[] children = node.children();
            double[] splits = findSplits(children, left, right, nodeCount);

            boolean foundSplit = false;
            for (int j = 0; j < splits.length; j++) {
                // find first split for which p < split, traverse down that node
                if (p < splits[j]) {
                    node = children[j];
                    if (j > 0) {
                        left = splits[j - 1];
                    }
                    right = splits[j];
                    foundSplit = true;
                    break;
                }
            }

            // if no split is < p, take the right-most child
            if (!foundSplit) {
                node = children[children.length - 1];
                left = splits[splits.length - 1];
            }
        }

        return node.position();
    }

    /**
     * Given a set of nodes representing an interval of cumulative frequencies
     * from leftBound to rightBound, find the values that separate the child nodes.
     * In other words, picturing the interval [leftBound, rightBound] on a number line,
     * find the points that separate the subintervals corresponding to each node.
     * Note: Because nodes have noisy values, the results returned here could be non-monotonic.
     */
    private double[] findSplits(NodeAddress[] children, double leftBound, double rightBound, double totalCardinality)
    {
        // Finding the splits boils down to finding the cumulative sum over the nodes.
        // In other words, we have to answer (nodes.length - 1) range queries, finding
        // the sum from the start to each node. Because nodes are noisy, there
        // are several approaches to doing this, differing most notably in their variance
        // and potential bias. The approach taken here is that of "mean consistency" as described
        // in "Understanding Hierarchical Methods for Differentially Private Histograms" by
        // Qardaji et al. (https://www.vldb.org/pvldb/vol6/p1954-qardaji.pdf).
        // Essentially, we add up the values of the children, and compare that sum to the parent.
        // (Since we're working with subsets of the unit interval here, we'll be doing all these
        // calculations normalized relative to totalCardinality.)
        // We divide the difference equally over the children so that in place of each child node's
        // value, we use a modified value that incorporates a share of this difference.

        // Store normalized but unadjusted child values in childValues.
        // At the end of the for loop, residual will equal the (normalized) difference between parent and child-sum.
        double[] childValues = new double[children.length];
        double residual = rightBound - leftBound;
        for (int i = 0; i < children.length; i++) {
            childValues[i] = query(children[i]) / totalCardinality;
            residual -= childValues[i];
        }

        // Find the points that separate the subintervals
        // split[i] is the point separating the i-th child branch from the (i+1)-th child branch
        double[] splits = new double[children.length - 1];

        // To calculate split[i], we can either:
        // - add the adjusted childValues from 0 through i to leftBound
        // - subtract the adjusted childValues from i+i through (end) from rightBound
        // The way that produces the least noisy estimates is to go from the left when we're closer to the left
        // or from the right when we're closer to the right.
        // Starting from the left, we'll calculate splits 0 through floor(children.length / 2) - 1
        // Letting C = children.length, w = rightBound - leftBound,
        // the formula for these is:
        // leftBound + sum(childValues[0:i]) + (i+1)/C * residual
        double leftSum = 0;
        for (int i = 0; i < Math.floorDiv(children.length, 2); i++) {
            leftSum += childValues[i];
            double fraction = (i + 1.0) / children.length;
            splits[i] = leftBound + leftSum + fraction * residual;
        }

        // We're left to calculate the remaining splits from the right.
        // We'll iterate from children.length - 1 through floor(children.length / 2), and calculate
        // rightBound - sum(childValues[i+1:]) + (1 - (i+1)/C) * residual
        double rightSum = 0;
        for (int i = children.length - 2; i >= Math.floorDiv(children.length, 2); i--) {
            rightSum += childValues[i + 1];
            double fraction = (i + 1.0) / children.length;
            splits[i] = rightBound - rightSum + (1 - fraction) * residual;
        }

        return splits;
    }

    @VisibleForTesting
    NodeAddress rootNode()
    {
        return new NodeAddress();
    }

    /**
     * We reference any node in the tree by the index at each level. For example, in the following tree:
     * <pre>
     *             (0)
     *         1         2
     *       3   4     5   6
     * </pre>
     * node 4 can be addressed as (0, 1) (0-th child in first level, 1-th child in second level),
     * node 5 can be address as (1, 0),
     * and so on...
     */
    @VisibleForTesting
    class NodeAddress
    {
        @VisibleForTesting
        int[] indices;

        public NodeAddress(int... indices)
        {
            this.indices = indices;
        }

        public NodeAddress[] children()
        {
            NodeAddress[] children = new NodeAddress[tree.getBranchingFactor()];
            for (int i = 0; i < tree.getBranchingFactor(); i++) {
                int[] child = Arrays.copyOf(indices, indices.length + 1);
                child[indices.length] = i;
                children[i] = new NodeAddress(child);
            }
            return children;
        }

        public boolean isLeaf()
        {
            return level() >= tree.totalLevels() - 1;
        }

        public int level()
        {
            return indices.length;
        }

        public int position()
        {
            int position = 0;
            for (int i = 0; i < indices.length; i++) {
                position += indices[indices.length - i - 1] * IntMath.pow(tree.getBranchingFactor(), i);
            }
            return position;
        }
    }
}
