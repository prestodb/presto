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
     * Verifies that the PlanNode passes basic matching tests that can done
     * with only the information contained in the node itself. Typically, these
     * should be limited to tests that validate the type of the node or
     * attributes of that type.
     * <p>
     * Matchers that can be applied to nodes of any typeshould return true from
     * shapeMatches and do the rest of their work in detailMatches.
     *
     * @param node The node to apply the matching tests to
     * @return true if all matching tests pass, false otherwise
     */
    boolean shapeMatches(PlanNode node);

    /**
     * Verifies that the Plan node passes in-depth matching tests. Matching
     * tests that check detailed information in a node's internals should be in
     * detailMatches.  In particular, matching tests that need to reference symbol
     * aliases from source nodes must be in a Matcher's detailMatches method.
     * <p>
     * The detailMatches method may collect Symbol aliases from the node that it is
     * being applied to, and return them in the MatchResult it returns.
     * detailMatches must ONLY collect SymbolAliases that are new to the node it is
     * being applied to.  Specifically, the MatchResult returned by
     * detailMatches MUST NOT contain any of the aliases contained in the
     * SymbolAliases that was passed in to detailMatches().
     * <p>
     * This is because the caller of detailMatches is responsible for calling
     * detailMatches for all of the source nodes/patterns, and then returning the
     * union of all of they symbols they returned to be used when applying the
     * parent nodes Matchers. If two Matchers each added their source aliases
     * to their results, the caller would see duplicate aliases while computing
     * the union of the returned aliases.
     * <p>
     * Matchers that don't need to validate anything about the internals of a
     * node should return a MatchResult with true and an empty
     * SymbolAliases object from detailMatches and do all of their work in
     * shapeMatches.
     * <p>
     * The plan testing framework should not call a Matcher's detailMatches on a
     * node if shapeMatches didn't return true for the same node.
     *
     * @param node The node to apply the matching tests to
     * @param session The session information for the query
     * @param metadata The metadata for the query
     * @param symbolAliases The SymbolAliases containing aliases from the nodes sources
     * @return a MatchResult with information about the success of the match
     */
    MatchResult detailMatches(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases);
}
