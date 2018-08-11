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
package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Lateral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import static com.facebook.presto.sql.AbbreviatorUtil.isAllowedToBePruned;
import static com.facebook.presto.sql.SqlFormatter.SqlFormatterType.PRUNE_AWARE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class QueryAbbreviator
{
    private static Logger log = Logger.get(QueryAbbreviator.class);
    private static final String PRUNED_MARKER = "...";

    private QueryAbbreviator() {}

    public static String abbreviate(String query, Node root, Optional<List<Expression>> parameters, int threshold)
    {
        requireNonNull(root, "root is null");
        checkArgument(threshold >= 0, "threshold is < 0");

        // Don't try pruning if threshold is smaller than the shortest possible string after abbreviation
        if (threshold < PRUNED_MARKER.length()) {
            return PRUNED_MARKER.substring(0, threshold);
        }

        // Compute priorities and generate an order of candidates to prune
        Queue<NodeInfo> pruningOrder = generatePruningOrder(root);

        // Prune the query tree.
        try {
            pruneQueryTree(root, parameters, pruningOrder, threshold);
        }
        catch (Exception e) {
            log.warn("Couldn't prune the query: " + query);
            return query.substring(0, threshold);
        }

        // construct and return formatted string for pruned tree. Truncate if still not done
        String prunedTreeSql = SqlFormatter.formatSql(root, parameters, PRUNE_AWARE);
        if (prunedTreeSql.length() > threshold) {
            return prunedTreeSql.substring(0, threshold);
        }
        return prunedTreeSql;
    }

    /**
     * Given a tree, generate a queue of nodes to prune. Less important nodes are put ahead of
     * the more important nodes.
     * @param root
     * @return
     */
    static Queue<NodeInfo> generatePruningOrder(Node root)
    {
        // Initialize the context for traversal
        PruningContext pruningContext = new PruningContext(new ArrayList<>(), 1, 1, 0);

        // Traverse the tree using PriorityGenerator visitor class, generate unordered list of nodes to prune
        new PriorityGenerator().process(root, pruningContext);

        // Sort the list to order nodes according to priority
        List<NodeInfo> pruningCandidatesUnordered = pruningContext.getNodesToPrune();
        pruningCandidatesUnordered.sort((o1, o2) -> {
            if (o1.cpVal < o2.cpVal) {
                return -1;
            }

            if (o1.cpVal == o2.cpVal) {
                if (o1.level > o2.level) {
                    return -1;
                }

                if (o1.level < o2.level) {
                    return 1;
                }

                return ((Integer) o1.getNode().hashCode()).compareTo(o2.getNode().hashCode());
            }
            return 1;
        });

        // Create a queue of nodes from the sorted list
        Queue<NodeInfo> pruningOrder = new LinkedList<>();
        for (int i = 0; i < pruningCandidatesUnordered.size(); i++) {
            pruningOrder.add(pruningCandidatesUnordered.get(i));
        }

        return pruningOrder;
    }

    /**
     * This method keeps pruning the nodes from the query tree one-by-one until
     *      (1) we achieve the required query size OR
     *      (2) All the nodes in queue are pruned
     * it is possible that generated sql from the pruned tree has length greater
     * than the threshold. This is because not all nodes are added to the pruned list.
     *
     * @param root - root of the query tree
     * @param pruningOrder - queue of candidate nodes to prune, ordered according to their priorities.
     * @param threshold - target length for abbreviation
     */
    private static void pruneQueryTree(Node root, Optional<List<Expression>> parameters, Queue<NodeInfo> pruningOrder, int threshold)
    {
        String originalQuery = SqlFormatter.formatSql(root, parameters, PRUNE_AWARE);
        int currentSize = originalQuery.length();

        while (currentSize > threshold) {
            try {
                int reduction = pruneOneNode(pruningOrder, parameters);
                currentSize -= reduction;
            }
            catch (NoSuchElementException e) {
                break;
            }
        }
    }

    /**
     * This method prunes the node in front of the queue.
     * Throws NoSuchElementException if the queue is empty.
     *
     * @param pruningOrder - queue of candidate nodes to prune, ordered according to their priorities.
     * @return change in the query length after pruning one node.
     */
    static int pruneOneNode(Queue<NodeInfo> pruningOrder, Optional<List<Expression>> parameters)
    {
        NodeInfo nodeInfo = pruningOrder.remove();
        return prune(nodeInfo.getNode(), nodeInfo.getIndent(), parameters);
    }

    private static int prune(Node node, int indent, Optional<List<Expression>> parameters)
    {
        // Formatted Sql for unpruned node
        String currentNodeSql = SqlFormatter.formatSql(node, parameters, indent, PRUNE_AWARE);

        // Sql after pruning the node
        String prunedNodeSql = PRUNED_MARKER;

        // Change in query length
        int changeInQueryLength = currentNodeSql.length() - prunedNodeSql.length();

        // Mark the node pruned and set PRUNED_MARKER
        node.setPruned(prunedNodeSql);
        return changeInQueryLength;
    }

    /**
     * This visitor class traverses the tree and
     * - keeps adding nodes to PruningContext::nodesToPrune.
     * - propagates the values for pVal and level.
     */
    private static class PriorityGenerator
            extends AstVisitor<Void, PruningContext>
    {
        /**
         * Common implementation for node objects
         * @param node
         * @param context
         * @return
         */
        @Override
        protected Void visitNode(Node node, PruningContext context)
        {
            double pVal = context.getCurrentPVal();
            double cpVal = pVal / Math.max(1, node.getChildren().size());

            // Add NodeInfo object to the queue if it is allowed to be pruned
            NodeInfo nodeInfo = new NodeInfo(
                                        node,
                                        cpVal,
                                        context.getCurrentLevel(),
                                        context.getCurrentIndent());

            if (isAllowedToBePruned(node)) {
                context.getNodesToPrune().add(nodeInfo);
            }

            // compute indent value for children
            int childIndent = getChildIndent(context.getCurrentIndent(), node);

            // Generate child context
            PruningContext childContext = new PruningContext(
                                                    context.getNodesToPrune(),
                                                    cpVal,
                                                    context.getCurrentLevel() + 1,
                                                    childIndent);

            // Process children
            for (Node child : node.getChildren()) {
                process(child, childContext);
            }

            return null;
        }

        private static int getChildIndent(int indent, Node node)
        {
            Set<Class> indentIncrementors = new HashSet<>(Arrays.asList(Prepare.class, TableSubquery.class, Lateral.class));

            if (indentIncrementors.contains(node.getClass())) {
                return indent + 1;
            }
            return indent;
        }

        /**
         * Special handling for query nodes
         * @param node Query instance
         * @param context pruning context
         * @return
         */
        @Override
        protected Void visitQuery(Query node, PruningContext context)
        {
            double pVal = context.getCurrentPVal();
            double cpVal = pVal / Math.max(1, node.getChildren().size());

            // compute indent value for children
            int childIndent = getChildIndent(context.getCurrentIndent(), node);

            // Add NodeInfo object to the priority queue
            NodeInfo nodeInfo = new NodeInfo(
                    node,
                    cpVal,
                    context.getCurrentLevel(),
                    context.getCurrentIndent());

            if (isAllowedToBePruned(node)) {
                context.getNodesToPrune().add(nodeInfo);
            }

            // Generate child context
            PruningContext childContext = new PruningContext(
                    context.getNodesToPrune(),
                    cpVal,
                    context.getCurrentLevel() + 1,
                    childIndent);

            if (node.getWith().isPresent()) {
                With with = node.getWith().get();
                processWith(with, childContext);
            }

            List<Node> childrenExceptWith = new ArrayList<>();
            childrenExceptWith.add(node.getQueryBody());

            if (node.getOrderBy().isPresent()) {
                childrenExceptWith.add(node.getOrderBy().get());
            }

            // Process children except With
            for (Node child : childrenExceptWith) {
                process(child, childContext);
            }

            return null;
        }

        private void processWith(With with, PruningContext context)
        {
            Iterator<WithQuery> queries = with.getQueries().iterator();
            int numWithQueries = with.getQueries().size();

            double pVal = context.getCurrentPVal();
            double cpVal = pVal / Math.max(numWithQueries, 1);

            int childIndent = getChildIndent(context.getCurrentIndent(), with);

            PruningContext childContext = new PruningContext(context.getNodesToPrune(),
                    cpVal,
                    context.getCurrentLevel() + 1,
                    childIndent);

            while (queries.hasNext()) {
                WithQuery query = queries.next();
                process(new TableSubquery(query.getQuery()), childContext);
            }
        }
    }

    static class NodeInfo
    {
        private final Node node;
        private final double cpVal;
        private final int level;
        private final int indent;

        public Node getNode()
        {
            return node;
        }

        public double getCPVal()
        {
            return cpVal;
        }

        public int getLevel()
        {
            return level;
        }

        public int getIndent()
        {
            return indent;
        }

        public NodeInfo(Node node, double cpVal, int level, int indent)
        {
            this.node = node;
            this.level = level;
            this.cpVal = cpVal;
            this.indent = indent;
        }
    }

    static class PruningContext
    {
        private final List<NodeInfo> nodesToPrune;
        private double currentPVal;
        private int currentLevel;
        private int currentIndent;

        public PruningContext(List<NodeInfo> nodesToPrune, double currentPVal, int currentLevel, int currentIndent)
        {
            this.nodesToPrune = nodesToPrune;
            this.currentPVal = currentPVal;
            this.currentLevel = currentLevel;
            this.currentIndent = currentIndent;
        }

        public List<NodeInfo> getNodesToPrune()
        {
            return this.nodesToPrune;
        }

        public double getCurrentPVal()
        {
            return this.currentPVal;
        }

        public int getCurrentLevel()
        {
            return this.currentLevel;
        }

        public int getCurrentIndent()
        {
            return this.currentIndent;
        }
    }
}
