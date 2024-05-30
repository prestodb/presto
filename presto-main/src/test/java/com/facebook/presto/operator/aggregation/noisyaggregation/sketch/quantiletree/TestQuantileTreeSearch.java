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

import com.google.common.math.IntMath;
import org.testng.annotations.Test;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestQuantileTreeSearch
{
    @Test
    public void testAddressingBinary()
    {
        // A binary tree is indexed like this:
        //                  (root)
        //          0                   1
        //   00          01      10            11
        // 000 001    010 011  100 101       110  111
        //                   ...
        QuantileTree tree = new QuantileTree(0, 1, 128, 2, 1, 100_000);
        QuantileTreeSearch search = new QuantileTreeSearch(tree);
        QuantileTreeSearch.NodeAddress root = search.rootNode();

        // first level
        QuantileTreeSearch.NodeAddress node0 = root.children()[0];
        assertEquals(node0.indices, new int[]{0});
        assertEquals(node0.level(), 1);
        assertEquals(node0.position(), 0);

        QuantileTreeSearch.NodeAddress node1 = root.children()[1];
        assertEquals(node1.indices, new int[]{1});
        assertEquals(node1.level(), 1);
        assertEquals(node1.position(), 1);

        // second level
        QuantileTreeSearch.NodeAddress node00 = node0.children()[0];
        assertEquals(node00.indices, new int[]{0, 0});
        assertEquals(node00.level(), 2);
        assertEquals(node00.position(), 0);

        QuantileTreeSearch.NodeAddress node01 = node0.children()[1];
        assertEquals(node01.indices, new int[]{0, 1});
        assertEquals(node01.level(), 2);
        assertEquals(node01.position(), 1);

        QuantileTreeSearch.NodeAddress node10 = node1.children()[0];
        assertEquals(node10.indices, new int[]{1, 0});
        assertEquals(node10.level(), 2);
        assertEquals(node10.position(), 2);

        QuantileTreeSearch.NodeAddress node11 = node1.children()[1];
        assertEquals(node11.indices, new int[]{1, 1});
        assertEquals(node11.level(), 2);
        assertEquals(node11.position(), 3);

        // third level
        QuantileTreeSearch.NodeAddress node000 = node00.children()[0];
        assertEquals(node000.indices, new int[]{0, 0, 0});
        assertEquals(node000.level(), 3);
        assertEquals(node000.position(), 0);

        QuantileTreeSearch.NodeAddress node011 = node01.children()[1];
        assertEquals(node011.indices, new int[]{0, 1, 1});
        assertEquals(node011.level(), 3);
        assertEquals(node011.position(), 0b11);

        QuantileTreeSearch.NodeAddress node100 = node10.children()[0];
        assertEquals(node100.indices, new int[]{1, 0, 0});
        assertEquals(node100.level(), 3);
        assertEquals(node100.position(), 0b100);

        QuantileTreeSearch.NodeAddress node111 = node11.children()[1];
        assertEquals(node111.indices, new int[]{1, 1, 1});
        assertEquals(node111.level(), 3);
        assertEquals(node111.position(), 0b111);

        // fourth level
        QuantileTreeSearch.NodeAddress node1001 = node100.children()[1];
        assertEquals(node1001.indices, new int[]{1, 0, 0, 1});
        assertEquals(node1001.level(), 4);
        assertEquals(node1001.position(), 0b1001);

        QuantileTreeSearch.NodeAddress node1110 = node111.children()[0];
        assertEquals(node1110.indices, new int[]{1, 1, 1, 0});
        assertEquals(node1110.level(), 4);
        assertEquals(node1110.position(), 0b1110);

        // ...seventh level
        QuantileTreeSearch.NodeAddress node0101010 = root.children()[0]
                .children()[1]
                .children()[0]
                .children()[1]
                .children()[0]
                .children()[1]
                .children()[0];
        assertEquals(node0101010.indices, new int[]{0, 1, 0, 1, 0, 1, 0});
        assertEquals(node0101010.level(), 7);
        assertEquals(node0101010.position(), 0b0101010);
    }

    @Test
    public void testAddressingTernary()
    {
        // A ternary tree is indexed like this:
        //                            (root)
        //          0                   1                   2
        //   00     01     02    10    11     12     20    21     22
        //                             ...
        QuantileTree tree = new QuantileTree(0, 1, 81, 3, 1, 100_000);
        QuantileTreeSearch search = new QuantileTreeSearch(tree);
        QuantileTreeSearch.NodeAddress root = search.rootNode();

        // first level
        QuantileTreeSearch.NodeAddress node0 = root.children()[0];
        assertEquals(node0.indices, new int[]{0});
        assertEquals(node0.level(), 1);
        assertEquals(node0.position(), 0);

        QuantileTreeSearch.NodeAddress node1 = root.children()[1];
        assertEquals(node1.indices, new int[]{1});
        assertEquals(node1.level(), 1);
        assertEquals(node1.position(), 1);

        QuantileTreeSearch.NodeAddress node2 = root.children()[2];
        assertEquals(node2.indices, new int[]{2});
        assertEquals(node2.level(), 1);
        assertEquals(node2.position(), 2);

        // second level
        QuantileTreeSearch.NodeAddress node00 = node0.children()[0];
        assertEquals(node00.indices, new int[]{0, 0});
        assertEquals(node00.level(), 2);
        assertEquals(node00.position(), 0);

        QuantileTreeSearch.NodeAddress node01 = node0.children()[1];
        assertEquals(node01.indices, new int[]{0, 1});
        assertEquals(node01.level(), 2);
        assertEquals(node01.position(), 1);

        QuantileTreeSearch.NodeAddress node02 = node0.children()[2];
        assertEquals(node02.indices, new int[]{0, 2});
        assertEquals(node02.level(), 2);
        assertEquals(node02.position(), 2);

        QuantileTreeSearch.NodeAddress node10 = node1.children()[0];
        assertEquals(node10.indices, new int[]{1, 0});
        assertEquals(node10.level(), 2);
        assertEquals(node10.position(), 3);

        QuantileTreeSearch.NodeAddress node11 = node1.children()[1];
        assertEquals(node11.indices, new int[]{1, 1});
        assertEquals(node11.level(), 2);
        assertEquals(node11.position(), 4);

        QuantileTreeSearch.NodeAddress node12 = node1.children()[2];
        assertEquals(node12.indices, new int[]{1, 2});
        assertEquals(node12.level(), 2);
        assertEquals(node12.position(), 5);

        QuantileTreeSearch.NodeAddress node20 = node2.children()[0];
        assertEquals(node20.indices, new int[]{2, 0});
        assertEquals(node20.level(), 2);
        assertEquals(node20.position(), 6);

        QuantileTreeSearch.NodeAddress node21 = node2.children()[1];
        assertEquals(node21.indices, new int[]{2, 1});
        assertEquals(node21.level(), 2);
        assertEquals(node21.position(), 7);

        QuantileTreeSearch.NodeAddress node22 = node2.children()[2];
        assertEquals(node22.indices, new int[]{2, 2});
        assertEquals(node22.level(), 2);
        assertEquals(node22.position(), 8);

        // third level
        QuantileTreeSearch.NodeAddress node210 = node21.children()[0];
        assertEquals(node210.indices, new int[]{2, 1, 0});
        assertEquals(node210.level(), 3);
        assertEquals(node210.position(), 2 * 9 + 1 * 3 + 0);

        // fourth level
        QuantileTreeSearch.NodeAddress node2101 = node210.children()[1];
        assertEquals(node2101.indices, new int[]{2, 1, 0, 1});
        assertEquals(node2101.level(), 4);
        assertEquals(node2101.position(), 2 * 27 + 1 * 9 + 0 * 3 + 1);
    }

    @Test
    public void testAddressingBase10()
    {
        // It's no longer practical to draw out the tree. :)
        QuantileTree tree = new QuantileTree(0, 1, 10_000, 10, 1, 100_000);
        QuantileTreeSearch search = new QuantileTreeSearch(tree);
        QuantileTreeSearch.NodeAddress root = search.rootNode();

        // first level
        QuantileTreeSearch.NodeAddress node5 = root.children()[5];
        assertEquals(node5.indices, new int[]{5});
        assertEquals(node5.level(), 1);
        assertEquals(node5.position(), 5);

        // second level
        QuantileTreeSearch.NodeAddress node54 = node5.children()[4];
        assertEquals(node54.indices, new int[]{5, 4});
        assertEquals(node54.level(), 2);
        assertEquals(node54.position(), 54);

        // third level
        QuantileTreeSearch.NodeAddress node543 = node54.children()[3];
        assertEquals(node543.indices, new int[]{5, 4, 3});
        assertEquals(node543.level(), 3);
        assertEquals(node543.position(), 543);

        // fourth level
        QuantileTreeSearch.NodeAddress node5432 = node543.children()[2];
        assertEquals(node5432.indices, new int[]{5, 4, 3, 2});
        assertEquals(node5432.level(), 4);
        assertEquals(node5432.position(), 5432);

        // fifth level
        QuantileTreeSearch.NodeAddress node54321 = node5432.children()[1];
        assertEquals(node54321.indices, new int[]{5, 4, 3, 2, 1});
        assertEquals(node54321.level(), 5);
        assertEquals(node54321.position(), 54321);
    }

    @Test
    public void testQueryDirect()
    {
        int bins = 3 * 3 * 3; // a tree with branching factor = 3 and three levels
        QuantileTree tree = generateQuantileTree(bins, 3);
        QuantileTreeSearch search = new QuantileTreeSearch(tree);

        eachNode(search, node -> {
            if (node.isLeaf()) {
                // leaf nodes should be the exact bin value, which is equal to one plus their position (by definition of generateQuantileTree)
                assertEquals(search.queryDirect(node), node.position() + 1);
            }
            else {
                // non-leaf nodes should exactly equal the sum of their children (since this tree has no noise)
                assertEquals(search.queryDirect(node), Arrays.stream(node.children()).mapToDouble(search::queryDirect).sum(), 0.0001);
            }
        });
    }

    @Test
    public void testQueryDownBinary()
    {
        for (int branchingFactor : new int[] {2, 3, 4}) {
            // this will create a complete tree with at least 200 bins
            // for branchingFactor = 2, that's 2^6 = 256 bins
            // for                 = 3, that's 3^5 = 243 bins
            // for                 = 4, that's 4^4 = 256 bins
            QuantileTree refTree = generateQuantileTree(200, branchingFactor);
            QuantileTreeSearch refSearch = new QuantileTreeSearch(refTree);

            // Make a noisy copy of the tree (refTree will be our "ground truth" tree)
            QuantileTree noisyTree = QuantileTree.deserialize(refTree.serialize());
            noisyTree.enablePrivacy(1.0, new TestQuantileTreeSeededRandomizationStrategy(1));
            QuantileTreeSearch noisySearch = new QuantileTreeSearch(noisyTree);

            QuantileTreeSearch.NodeAddress refNode0 = refSearch.rootNode().children()[0];
            QuantileTreeSearch.NodeAddress noisyNode0 = noisySearch.rootNode().children()[0];

            assertEquals(refSearch.queryDirect(refNode0), noisySearch.queryDirect(noisyNode0), 10.0, "noisyTree counts should vary slightly from refTree");
            double[] denoise1 = noisySearch.queryDown(noisyNode0, 1);
            assertEquals(denoise1[0], noisySearch.queryDirect(noisyNode0), 10.0, "de-noising one level should change estimate slightly");
            assertTrue(denoise1[1] < 1, "de-noising one level should reduce variance");
            double relativeVariance1 = (branchingFactor - 1) / (branchingFactor - Math.pow(branchingFactor, -1));
            assertEquals(denoise1[1], relativeVariance1, 0.001, "de-noising one level yield relative variance of relativeVariance1");

            double[] denoise2 = noisySearch.queryDown(noisyNode0, 2);
            assertEquals(denoise2[0], noisySearch.queryDirect(noisyNode0), 10.0, "de-noising two levels should change estimate slightly");
            assertTrue(denoise2[1] < denoise1[1], "de-noising two level should reduce variance more than de-noising one level");
            double relativeVariance2 = (branchingFactor - 1) / (branchingFactor - Math.pow(branchingFactor, -2));
            assertEquals(denoise2[1], relativeVariance2, 0.001, "de-noising one level yield relative variance of relativeVariance2");
        }
    }

    @Test
    public void testQueryDownZeroLevels()
    {
        QuantileTree tree = generateQuantileTree(1296, 6); // 6^4
        tree.enablePrivacy(1.0, new TestQuantileTreeSeededRandomizationStrategy(1));
        QuantileTreeSearch search = new QuantileTreeSearch(tree);
        // Picking a non-leaf node at random, queryDown(x, 0) is equivalent to queryDirect(x)
        QuantileTreeSearch.NodeAddress arbitraryNode = search.rootNode().children()[2].children()[3];
        assertEquals(search.queryDown(arbitraryNode, 0), new double[] {search.queryDirect(arbitraryNode), 1});
    }

    @Test
    public void testMaxTraversalDepth()
    {
        assertEquals(QuantileTreeSearch.findMaxTraversalDepth(2), 6);
        assertEquals(QuantileTreeSearch.findMaxTraversalDepth(3), 3);
        assertEquals(QuantileTreeSearch.findMaxTraversalDepth(4), 3);
        assertEquals(QuantileTreeSearch.findMaxTraversalDepth(5), 2);
        assertEquals(QuantileTreeSearch.findMaxTraversalDepth(6), 2);
        assertEquals(QuantileTreeSearch.findMaxTraversalDepth(7), 2);
        assertEquals(QuantileTreeSearch.findMaxTraversalDepth(8), 2);

        // branching factors 9 through 64 may traverse one level
        for (int i = 9; i <= 64; i++) {
            assertEquals(QuantileTreeSearch.findMaxTraversalDepth(i), 1);
        }

        // >64 should not traverse down at all (queryDirect only)
        for (int i = 65; i < 1000; i++) {
            assertEquals(QuantileTreeSearch.findMaxTraversalDepth(i), 0);
        }
    }

    @Test
    public void testIsLeaf()
    {
        for (int branchingFactor : new int[] {2, 3, 4}) {
            int depth = 4;
            int bins = IntMath.pow(branchingFactor, depth);
            QuantileTree tree = new QuantileTree(0, 1, bins, branchingFactor, 1, 100_000);
            QuantileTreeSearch search = new QuantileTreeSearch(tree);

            eachNode(search, node -> {
                assertEquals(node.isLeaf(), node.level() == depth);
            });
        }
    }

    @Test
    public void testQuery()
    {
        for (int branchingFactor : new int[] {2, 3, 4}) {
            int depth = 4;
            int bins = IntMath.pow(branchingFactor, depth);
            QuantileTree tree = generateQuantileTree(bins, branchingFactor);
            tree.enablePrivacy(1.0, new TestQuantileTreeSeededRandomizationStrategy(1));
            QuantileTreeSearch search = new QuantileTreeSearch(tree);

            eachNode(search, node -> {
                // Leaf nodes:
                // Their values should approximately equal the number of items in the bin.
                // query() and queryDirect() should return the same value here, since there are no children to use for variance-reduction.
                if (node.isLeaf()) {
                    double directValue = search.queryDirect(node);
                    double queryValue = search.query(node);
                    assertEquals(directValue, node.position() + 1, 10.0);
                    assertEquals(queryValue, node.position() + 1, 10.0);
                    assertEquals(directValue, queryValue);
                }
                // Non-leaf nodes:
                // Since this tree is noisy, their values should approximately equal the sum of their children.
                // Both query() and queryDirect() should return similar estimates, but we should get different estimates
                // using query() vs. queryDirect(), since query() performs a variance-reduction step.
                else {
                    QuantileTreeSearch.NodeAddress[] children = node.children();
                    double directValue = search.queryDirect(node);
                    double queryValue = search.query(node);
                    double childSum = Arrays.stream(children).mapToDouble(search::queryDirect).sum();
                    assertEquals(directValue, childSum, 10.0);
                    assertEquals(queryValue, childSum, 10.0);
                    assertNotEquals(directValue, queryValue);
                }
            });
        }
    }

    @Test
    public void testFindQuantile()
    {
        for (int branchingFactor : new int[] {2, 3, 4, 5}) {
            for (int depth : new int[] {1, 3, 5}) {
                int bins = IntMath.pow(branchingFactor, depth);
                QuantileTree tree = generateQuantileTree(bins, branchingFactor);
                QuantileTreeSearch search = new QuantileTreeSearch(tree);
                int total = IntStream.rangeClosed(1, bins).sum();
                int cumulativeSum = 0;
                for (int i = 1; i < bins; i++) {
                    // findQuantile() returns 0-indexed bins
                    // look for the bin with slightly less weight than the cumulativeSum
                    cumulativeSum += i;
                    assertEquals(search.findQuantile((cumulativeSum - 0.1) / total), i - 1);
                }
            }
        }
    }

    private static QuantileTree generateQuantileTree(int bins, int branchingFactor)
    {
        // Create a tree with B bins, corresponding to the ranges [0, 1), [1, 2), ..., [B-2, B-1), [B-1, B]
        // Using enormous sketch dimensions ensures we will only use non-sketched levels.
        QuantileTree tree = new QuantileTree(0, bins, bins, branchingFactor, 100_000_000, 1);

        // in each bin, i (1-indexed)
        for (int i = 1; i <= bins; i++) {
            // insert i items
            for (int j = 0; j < i; j++) {
                tree.add(i - 0.5);
            }
        }

        return tree;
    }

    private static void eachNode(QuantileTreeSearch search, Consumer<QuantileTreeSearch.NodeAddress> assertion)
    {
        ArrayDeque<QuantileTreeSearch.NodeAddress> nodes = new ArrayDeque<QuantileTreeSearch.NodeAddress>(Arrays.asList(search.rootNode().children()));
        while (!nodes.isEmpty()) {
            QuantileTreeSearch.NodeAddress node = nodes.pop();
            assertion.accept(node);
        }
    }
}
