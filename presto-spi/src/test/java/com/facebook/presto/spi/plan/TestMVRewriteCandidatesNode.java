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
package com.facebook.presto.spi.plan;

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestMVRewriteCandidatesNode
{
    private static final VariableReferenceExpression COL_A =
            new VariableReferenceExpression(Optional.empty(), "col_a", BIGINT);
    private static final VariableReferenceExpression COL_B =
            new VariableReferenceExpression(Optional.empty(), "col_b", VARCHAR);

    @Test
    public void testGetSources()
    {
        PlanNode original = valuesNode("original", COL_A);
        PlanNode mv1Plan = valuesNode("mv1", COL_A);
        PlanNode mv2Plan = valuesNode("mv2", COL_A);

        MVRewriteCandidatesNode node = new MVRewriteCandidatesNode(
                Optional.empty(),
                new PlanNodeId("mvnode"),
                original,
                ImmutableList.of(
                        new MVRewriteCandidatesNode.MVRewriteCandidate(mv1Plan, "cat", "sch", "mv1"),
                        new MVRewriteCandidatesNode.MVRewriteCandidate(mv2Plan, "cat", "sch", "mv2")),
                ImmutableList.of(COL_A));

        List<PlanNode> sources = node.getSources();
        assertEquals(sources.size(), 3);
        assertEquals(sources.get(0), original);
        assertEquals(sources.get(1), mv1Plan);
        assertEquals(sources.get(2), mv2Plan);
    }

    @Test
    public void testOutputVariables()
    {
        PlanNode original = valuesNode("original", COL_A, COL_B);

        MVRewriteCandidatesNode node = new MVRewriteCandidatesNode(
                Optional.empty(),
                new PlanNodeId("mvnode"),
                original,
                ImmutableList.of(),
                ImmutableList.of(COL_A, COL_B));

        assertEquals(node.getOutputVariables(), ImmutableList.of(COL_A, COL_B));
    }

    @Test
    public void testReplaceChildren()
    {
        PlanNode original = valuesNode("original", COL_A);
        PlanNode mv1Plan = valuesNode("mv1", COL_A);
        PlanNode mv2Plan = valuesNode("mv2", COL_A);

        MVRewriteCandidatesNode node = new MVRewriteCandidatesNode(
                Optional.empty(),
                new PlanNodeId("mvnode"),
                original,
                ImmutableList.of(
                        new MVRewriteCandidatesNode.MVRewriteCandidate(mv1Plan, "cat", "sch", "mv1"),
                        new MVRewriteCandidatesNode.MVRewriteCandidate(mv2Plan, "cat", "sch", "mv2")),
                ImmutableList.of(COL_A));

        PlanNode newOriginal = valuesNode("newOriginal", COL_A);
        PlanNode newMv1Plan = valuesNode("newMv1", COL_A);
        PlanNode newMv2Plan = valuesNode("newMv2", COL_A);

        MVRewriteCandidatesNode replaced = (MVRewriteCandidatesNode) node.replaceChildren(
                ImmutableList.of(newOriginal, newMv1Plan, newMv2Plan));

        assertEquals(replaced.getOriginalPlan(), newOriginal);
        assertEquals(replaced.getCandidates().size(), 2);
        assertEquals(replaced.getCandidates().get(0).getPlan(), newMv1Plan);
        assertEquals(replaced.getCandidates().get(0).getMaterializedViewCatalog(), "cat");
        assertEquals(replaced.getCandidates().get(0).getMaterializedViewSchema(), "sch");
        assertEquals(replaced.getCandidates().get(0).getMaterializedViewName(), "mv1");
        assertEquals(replaced.getCandidates().get(1).getPlan(), newMv2Plan);
        assertEquals(replaced.getCandidates().get(1).getMaterializedViewName(), "mv2");
        assertEquals(replaced.getOutputVariables(), ImmutableList.of(COL_A));
    }

    @Test
    public void testReplaceChildrenWrongCount()
    {
        PlanNode original = valuesNode("original", COL_A);
        PlanNode mv1Plan = valuesNode("mv1", COL_A);

        MVRewriteCandidatesNode node = new MVRewriteCandidatesNode(
                Optional.empty(),
                new PlanNodeId("mvnode"),
                original,
                ImmutableList.of(
                        new MVRewriteCandidatesNode.MVRewriteCandidate(mv1Plan, "cat", "sch", "mv1")),
                ImmutableList.of(COL_A));

        try {
            node.replaceChildren(ImmutableList.of(valuesNode("only_one", COL_A)));
            fail("Expected IllegalArgumentException for wrong children count");
        }
        catch (IllegalArgumentException e) {
            // expected: need 2 children (1 original + 1 candidate) but got 1
        }
    }

    @Test
    public void testCandidateFullyQualifiedName()
    {
        MVRewriteCandidatesNode.MVRewriteCandidate candidate =
                new MVRewriteCandidatesNode.MVRewriteCandidate(
                        valuesNode("v1", COL_A),
                        "my_catalog",
                        "my_schema",
                        "my_view");

        assertEquals(candidate.getFullyQualifiedName(), "my_catalog.my_schema.my_view");
    }

    @Test
    public void testGetOriginalPlanAndCandidates()
    {
        PlanNode original = valuesNode("original", COL_A);
        PlanNode mv1Plan = valuesNode("mv1", COL_A);

        MVRewriteCandidatesNode.MVRewriteCandidate candidate =
                new MVRewriteCandidatesNode.MVRewriteCandidate(mv1Plan, "cat", "sch", "mv1");

        MVRewriteCandidatesNode node = new MVRewriteCandidatesNode(
                Optional.empty(),
                new PlanNodeId("mvnode"),
                original,
                ImmutableList.of(candidate),
                ImmutableList.of(COL_A));

        assertEquals(node.getOriginalPlan(), original);
        assertEquals(node.getCandidates().size(), 1);
        assertEquals(node.getCandidates().get(0).getPlan(), mv1Plan);
        assertEquals(node.getCandidates().get(0).getMaterializedViewCatalog(), "cat");
        assertEquals(node.getCandidates().get(0).getMaterializedViewSchema(), "sch");
        assertEquals(node.getCandidates().get(0).getMaterializedViewName(), "mv1");
    }

    @Test
    public void testAssignStatsEquivalentPlanNode()
    {
        PlanNode original = valuesNode("original", COL_A);
        PlanNode mv1Plan = valuesNode("mv1", COL_A);
        PlanNode statsEquivalent = valuesNode("stats", COL_A);

        MVRewriteCandidatesNode node = new MVRewriteCandidatesNode(
                Optional.empty(),
                new PlanNodeId("mvnode"),
                original,
                ImmutableList.of(
                        new MVRewriteCandidatesNode.MVRewriteCandidate(mv1Plan, "cat", "sch", "mv1")),
                ImmutableList.of(COL_A));

        MVRewriteCandidatesNode withStats = (MVRewriteCandidatesNode) node.assignStatsEquivalentPlanNode(
                Optional.of(statsEquivalent));

        assertEquals(withStats.getStatsEquivalentPlanNode(), Optional.of(statsEquivalent));
        assertEquals(withStats.getOriginalPlan(), original);
        assertEquals(withStats.getCandidates().size(), 1);
    }

    private static ValuesNode valuesNode(String id, VariableReferenceExpression... variables)
    {
        return new ValuesNode(
                Optional.empty(),
                new PlanNodeId(id),
                ImmutableList.copyOf(variables),
                ImmutableList.of(),
                Optional.empty());
    }
}
