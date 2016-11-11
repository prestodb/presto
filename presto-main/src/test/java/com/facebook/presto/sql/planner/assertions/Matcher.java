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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.plan.PlanNode;

public interface Matcher
{
    /**
     * Verifies that the PlanNode passes basic matching tests that can done with only
     * the information contained in the node itself. Typically, these should be limited to
     * tests that validate the type of the node or attributes of that type.
     *
     * Matchers that can be applied to any node should return true from downMatches and do
     * the rest of their work in upMatches.
     *
     * @param node  The node to apply the matching tests to
     * @return      true if all matching tests pass, false otherwise
     */
    boolean downMatches(PlanNode node);

    /**
     * Verifies that the Plan node passes in-depth matching tests. Matching tests that
     * check detailed information in a node's internals should be in upMatches.
     * In particular, matching tests that need to reference symbols from source nodes
     * must be in a Matcher's upMatches method.
     *
     * The upMatches method may add symbols to the ExpressionAliases that is passed to it.
     * This allows Matchers further up the tree to reference these symbols in their upMatches
     * method in turn.
     *
     * In general, adding symbols to the ExpressionAliases map should be done with the
     * special Alias Matcher. In cases where a node produces a symbol not from an Expression,
     * the symbol should be added with a node-specific Matcher. For example, SemiJoinNodes
     * produce a semiJoinOutput symbol, and SemiJoinMatcher adds an alias for that to
     * ExpressionAliases.
     *
     * Matchers that don't need to validate anything about the internals of a node should
     * return true from upMatches and do all of their work in downMatches.
     *
     * The plan testing framework should not call a Matcher's upMatches on a node if downMatches
     * didn't return true for the same node.
     *
     * @param node      The node to apply the matching tests to
     * @param session   The session information for the query
     * @param metadata  The metadata for the query
     * @param expressionAliases     The ExpressionAliases containing aliases from the nodes sources
     * @return      true if all matching tests pass, false otherwise
     */
    boolean upMatches(PlanNode node, Session session, Metadata metadata, ExpressionAliases expressionAliases);
}
