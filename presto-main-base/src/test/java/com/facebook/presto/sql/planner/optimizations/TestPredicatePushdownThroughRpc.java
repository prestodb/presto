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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.InMemoryExpressionOptimizerProvider;
import com.facebook.presto.sql.planner.assertions.OptimizerAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.RPCNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

/**
 * An RPC node produces one output row per input row and adds one column, so a predicate that does
 * not read that column selects the same rows above the node and below it. Below is the better place:
 * the call is not made for a row the query goes on to discard, and the optimizer can carry the
 * predicate further down towards the scan.
 */
public class TestPredicatePushdownThroughRpc
{
    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (tester != null) {
            tester.close();
            tester = null;
        }
    }

    @Test
    public void testPredicateNotReadingTheCallIsPushedBelowIt()
    {
        assertThatOptimizer()
                .on(p -> filterOverRpc(p, "x < 10"))
                .matches(node(
                        RPCNode.class,
                        filter("x < 10", values(ImmutableMap.of("x", 0, "s", 1)))));
    }

    @Test
    public void testPredicateReadingTheCallStaysAboveIt()
    {
        assertThatOptimizer()
                .on(p -> filterOverRpc(p, "score = 'yes'"))
                .matches(filter(
                        "score = 'yes'",
                        rpc(values(ImmutableMap.of("x", 0, "s", 1)))));
    }

    /**
     * The conjunct on {@code x} moves below the call and the conjunct on the call's own output stays
     * in the filter above it.
     */
    @Test
    public void testPredicateIsSplitAroundTheCall()
    {
        assertThatOptimizer()
                .on(p -> filterOverRpc(p, "x < 10 AND score = 'yes'"))
                .matches(filter(
                        "score = 'yes'",
                        rpc(
                                filter(
                                        "x < 10",
                                        values(ImmutableMap.of("x", 0, "s", 1))))));
    }

    /**
     * A predicate that reads both the call's output and a source column cannot be split, so it
     * stays above the call in one piece: the values node is still directly below.
     */
    @Test
    public void testPredicateReadingBothStaysAboveIt()
    {
        assertThatOptimizer()
                .on(p -> filterOverRpc(p, "score = 'yes' OR x < 10"))
                .matches(filter(
                        "score = 'yes' OR x < 10",
                        rpc(values(ImmutableMap.of("x", 0, "s", 1)))));
    }

    /**
     * A non-deterministic predicate is left above the call even though it reads none of its output,
     * which is how every other node in this rule treats one.
     */
    @Test
    public void testNonDeterministicPredicateStaysAboveIt()
    {
        assertThatOptimizer()
                .on(p -> filterOverRpc(p, "random() < 5e-1"))
                .matches(filter(
                        "random() < 5e-1",
                        node(RPCNode.class, values(ImmutableMap.of("x", 0, "s", 1)))));
    }

    /**
     * The RPC node over {@code source}, with its output bound to the alias {@code score} so that a
     * filter above it can name the column in its condition.
     */
    private static PlanMatchPattern rpc(PlanMatchPattern source)
    {
        return node(RPCNode.class, source)
                .withAlias("score", (node, session, metadata, aliases) -> node instanceof RPCNode
                        ? Optional.of(((RPCNode) node).getOutputVariable())
                        : Optional.empty());
    }

    private OptimizerAssert assertThatOptimizer()
    {
        return tester.assertThat(new PredicatePushDown(
                tester.getMetadata(),
                tester.getSqlParser(),
                new InMemoryExpressionOptimizerProvider(tester.getMetadata()),
                false));
    }

    /**
     * {@code FILTER(predicate) -> RPC(score = f(s)) -> VALUES(x, s)} — the shape a query whose remote
     * call sits in a subquery and whose predicate sits on the outer query plans as.
     */
    private static PlanNode filterOverRpc(PlanBuilder p, String predicate)
    {
        p.variable("x", INTEGER);
        p.variable("s", VARCHAR);
        p.variable("score", VARCHAR);

        RowExpression argument = p.variable("s", VARCHAR);
        PlanNode rpc = new RPCNode(
                Optional.empty(),
                p.getIdAllocator().getNextId(),
                p.values(p.variable("x", INTEGER), p.variable("s", VARCHAR)),
                "test_rpc_function",
                ImmutableList.of(argument),
                ImmutableList.of("arg0"),
                p.variable("score", VARCHAR),
                RPCNode.StreamingMode.PER_ROW,
                0);
        return p.filter(p.rowExpression(predicate), rpc);
    }
}
