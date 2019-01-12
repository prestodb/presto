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
package io.prestosql.sql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.function.Consumer;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static org.testng.Assert.assertEquals;

public class TestSubqueries
{
    private static final String UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG = "line .*: Given correlated subquery is not supported";

    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testCorrelatedExistsSubqueriesWithOrPredicateAndNull()
    {
        assertExistsRewrittenToAggregationAboveJoin(
                "SELECT EXISTS(SELECT 1 FROM (VALUES null, 10) t(x) WHERE y > x OR y + 10 > x) FROM (values (11)) t2(y)",
                "VALUES true",
                false);
        assertExistsRewrittenToAggregationAboveJoin(
                "SELECT EXISTS(SELECT 1 FROM (VALUES null) t(x) WHERE y > x OR y + 10 > x) FROM (values (11)) t2(y)",
                "VALUES false",
                false);
    }

    @Test
    public void testUnsupportedSubqueriesWithCoercions()
    {
        // coercion from subquery symbol type to correlation type
        assertions.assertFails(
                "select (select count(*) from (values 1) t(a) where t.a=t2.b limit 1) from (values 1.0) t2(b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        // coercion from t.a (null) to integer
        assertions.assertFails(
                "select EXISTS(select 1 from (values (null, null)) t(a, b) where t.a=t2.b GROUP BY t.b) from (values 1, 2) t2(b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
    }

    @Test
    public void testCorrelatedSubqueriesWithLimit()
    {
        assertions.assertQuery(
                "select (select t.a from (values 1, 2) t(a) where t.a=t2.b limit 1) from (values 1) t2(b)",
                "VALUES 1");
        // cannot enforce limit 2 on correlated subquery
        assertions.assertFails(
                "select (select t.a from (values 1, 2) t(a) where t.a=t2.b limit 2) from (values 1) t2(b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertions.assertQuery(
                "select (select sum(t.a) from (values 1, 2) t(a) where t.a=t2.b group by t.a limit 2) from (values 1) t2(b)",
                "VALUES BIGINT '1'");
        assertions.assertQuery(
                "select (select count(*) from (select t.a from (values 1, 1, null, 3) t(a) limit 1) t where t.a=t2.b) from (values 1, 2) t2(b)",
                "VALUES BIGINT '1', BIGINT '0'");
        assertExistsRewrittenToAggregationBelowJoin(
                "select EXISTS(select 1 from (values 1, 1, 3) t(a) where t.a=t2.b limit 1) from (values 1, 2) t2(b)",
                "VALUES true, false",
                false);
        // TransformCorrelatedScalarAggregationToJoin does not fire since limit is above aggregation node
        assertions.assertFails(
                "select (select count(*) from (values 1, 1, 3) t(a) where t.a=t2.b limit 1) from (values 1) t2(b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertExistsRewrittenToAggregationBelowJoin(
                "SELECT EXISTS(SELECT 1 FROM (values ('x', 1)) u(x, cid) WHERE x = 'x' AND t.cid = cid LIMIT 1) " +
                        "FROM (values 1) t(cid)",
                "VALUES true",
                false);
    }

    @Test
    public void testCorrelatedSubqueriesWithGroupBy()
    {
        // t.a is not a "constant" column, group by does not guarantee single row per correlated subquery
        assertions.assertFails(
                "select (select count(*) from (values 1, 2, 3, null) t(a) where t.a<t2.b GROUP BY t.a) from (values 1, 2, 3) t2(b)",
                "Scalar sub-query has returned multiple rows");
        assertions.assertQuery(
                "select (select count(*) from (values 1, 1, 2, 3, null) t(a) where t.a<t2.b GROUP BY t.a HAVING count(*) > 1) from (values 1, 2) t2(b)",
                "VALUES null, BIGINT '2'");
        assertExistsRewrittenToAggregationBelowJoin(
                "select EXISTS(select 1 from (values 1, 1, 3) t(a) where t.a=t2.b GROUP BY t.a) from (values 1, 2) t2(b)",
                "VALUES true, false",
                false);
        assertExistsRewrittenToAggregationBelowJoin(
                "select EXISTS(select 1 from (values (1, 2), (1, 2), (null, null), (3, 3)) t(a, b) where t.a=t2.b GROUP BY t.a, t.b) from (values 1, 2) t2(b)",
                "VALUES true, false",
                true);
        assertExistsRewrittenToAggregationAboveJoin(
                "select EXISTS(select 1 from (values (1, 2), (1, 2), (null, null), (3, 3)) t(a, b) where t.a<t2.b GROUP BY t.a, t.b) from (values 1, 2) t2(b)",
                "VALUES false, true",
                true);
        // t.b is not a "constant" column, cannot be pushed above aggregation
        assertions.assertFails(
                "select EXISTS(select 1 from (values (1, 1), (1, 1), (null, null), (3, 3)) t(a, b) where t.a+t.b<t2.b GROUP BY t.a) from (values 1, 2) t2(b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertExistsRewrittenToAggregationAboveJoin(
                "select EXISTS(select 1 from (values (1, 1), (1, 1), (null, null), (3, 3)) t(a, b) where t.a+t.b<t2.b GROUP BY t.a, t.b) from (values 1, 4) t2(b)",
                "VALUES false, true",
                true);
        assertExistsRewrittenToAggregationBelowJoin(
                "select EXISTS(select 1 from (values (1, 2), (1, 2), (null, null), (3, 3)) t(a, b) where t.a=t2.b GROUP BY t.b) from (values 1, 2) t2(b)",
                "VALUES true, false",
                true);
        assertExistsRewrittenToAggregationBelowJoin(
                "select EXISTS(select * from (values 1, 1, 2, 3) t(a) where t.a=t2.b GROUP BY t.a HAVING count(*) > 1) from (values 1, 2) t2(b)",
                "VALUES true, false",
                false);
        assertions.assertQuery(
                "select EXISTS(select * from (select t.a from (values (1, 1), (1, 1), (1, 2), (1, 2), (3, 3)) t(a, b) where t.b=t2.b GROUP BY t.a HAVING count(*) > 1) t where t.a=t2.b)" +
                        " from (values 1, 2) t2(b)",
                "VALUES true, false");
        assertExistsRewrittenToAggregationBelowJoin(
                "select EXISTS(select * from (values 1, 1, 2, 3) t(a) where t.a=t2.b GROUP BY (t.a) HAVING count(*) > 1) from (values 1, 2) t2(b)",
                "VALUES true, false",
                false);
    }

    @Test
    public void testCorrelatedLateralWithGroupBy()
    {
        assertions.assertQuery(
                "select * from (values 1, 2) t2(b), LATERAL (select t.a from (values 1, 1, 3) t(a) where t.a=t2.b GROUP BY t.a)",
                "VALUES (1, 1)");
        assertions.assertQuery(
                "select * from (values 1, 2) t2(b), LATERAL (select count(*) from (values 1, 1, 2, 3) t(a) where t.a=t2.b GROUP BY t.a HAVING count(*) > 1)",
                "VALUES (1, BIGINT '2')");
        // correlated subqueries with grouping sets are not supported
        assertions.assertFails(
                "select * from (values 1, 2) t2(b), LATERAL (select t.a, t.b, count(*) from (values (1, 1), (1, 2), (2, 2), (3, 3)) t(a, b) where t.a=t2.b GROUP BY GROUPING SETS ((t.a, t.b), (t.a)))",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
    }

    @Test
    public void testLateralWithUnnest()
    {
        assertions.assertFails(
                "SELECT * FROM (VALUES ARRAY[1]) t(x), LATERAL (SELECT * FROM UNNEST(x))",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
    }

    @Test
    public void testCorrelatedScalarSubquery()
    {
        assertions.assertQuery(
                "SELECT * FROM (VALUES 1, 2) t2(b) WHERE (SELECT b) = 2",
                "VALUES 2");
    }

    @Test
    public void testCorrelatedSubqueryWithExplicitCoercion()
    {
        assertions.assertQuery(
                "SELECT 1 FROM (VALUES 1, 2) t1(b) WHERE 1 = (SELECT cast(b as decimal(7,2)))",
                "VALUES 1");
    }

    private void assertExistsRewrittenToAggregationBelowJoin(@Language("SQL") String actual, @Language("SQL") String expected, boolean extraAggregation)
    {
        PlanMatchPattern source = node(ValuesNode.class);
        if (extraAggregation) {
            source = aggregation(ImmutableMap.of(),
                    exchange(LOCAL, REPARTITION,
                            aggregation(ImmutableMap.of(),
                                    anyTree(
                                            node(ValuesNode.class)))));
        }
        assertions.assertQueryAndPlan(actual, expected,
                anyTree(
                        node(JoinNode.class,
                                anyTree(
                                        node(ValuesNode.class)),
                                anyTree(
                                        aggregation(ImmutableMap.of(), FINAL,
                                                exchange(LOCAL, REPARTITION,
                                                        aggregation(ImmutableMap.of(), PARTIAL,
                                                                anyTree(source))))))),
                plan -> assertEquals(countFinalAggregationNodes(plan), extraAggregation ? 2 : 1));
    }

    private void assertExistsRewrittenToAggregationAboveJoin(@Language("SQL") String actual, @Language("SQL") String expected, boolean extraAggregation)
    {
        Consumer<Plan> singleStreamingAggregationValidator = plan -> assertEquals(countSingleStreamingAggregations(plan), 1);
        Consumer<Plan> finalAggregationValidator = plan -> assertEquals(countFinalAggregationNodes(plan), extraAggregation ? 1 : 0);

        assertions.assertQueryAndPlan(actual, expected,
                anyTree(
                        aggregation(
                                ImmutableMap.of("COUNT", functionCall("count", ImmutableList.of("NON_NULL"))),
                                SINGLE,
                                node(JoinNode.class,
                                        anyTree(
                                                node(ValuesNode.class)),
                                        anyTree(
                                                node(ProjectNode.class,
                                                        anyTree(
                                                                node(ValuesNode.class)))
                                                        .withAlias("NON_NULL", expression("true")))))),
                singleStreamingAggregationValidator.andThen(finalAggregationValidator));
    }

    private static int countFinalAggregationNodes(Plan plan)
    {
        return searchFrom(plan.getRoot())
                .where(node -> node instanceof AggregationNode && ((AggregationNode) node).getStep() == FINAL)
                .count();
    }

    private static int countSingleStreamingAggregations(Plan plan)
    {
        return searchFrom(plan.getRoot())
                .where(node -> node instanceof AggregationNode && ((AggregationNode) node).getStep() == SINGLE && ((AggregationNode) node).isStreamable())
                .count();
    }
}
