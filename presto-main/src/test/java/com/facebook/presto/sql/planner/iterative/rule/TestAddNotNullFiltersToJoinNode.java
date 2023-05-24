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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinNotNullInferenceStrategy;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.AddNotNullFiltersToJoinNode.ExtractInferredNotNullVariablesVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.JOINS_NOT_NULL_INFERENCE_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_NULLS_IN_JOINS;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinNotNullInferenceStrategy.INFER_FROM_STANDARD_OPERATORS;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinNotNullInferenceStrategy.USE_FUNCTION_METADATA;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestAddNotNullFiltersToJoinNode
        extends BasePlanTest
{
    private static final Map<String, Type> testVariableTypeMap;
    private final TestingRowExpressionTranslator rowExpressionTranslator;
    private final FunctionAndTypeManager functionAndTypeManager;

    public TestAddNotNullFiltersToJoinNode()
    {
        super(ImmutableMap.of(OPTIMIZE_NULLS_IN_JOINS, Boolean.toString(false),
                JOINS_NOT_NULL_INFERENCE_STRATEGY, USE_FUNCTION_METADATA.toString()));

        functionAndTypeManager = createTestFunctionAndTypeManager();
        Metadata metadata = MetadataManager.createTestMetadataManager();
        rowExpressionTranslator = new TestingRowExpressionTranslator(metadata);
    }

    @DataProvider
    public static Object[][] getExistingNotNullVarsTestCases()
    {
        return new Object[][] {
                {"a IS NOT NULL AND b IS NOT NULL", new String[] {"a", "b"}},
                {"a > 10 AND b IS NOT NULL AND c is NOT NULL", new String[] {"b", "c"}},
                {"a is NULL AND b IS NOT NULL", new String[] {"b"}},
                {"NOT(a is NULL)", new String[] {"a"}},
                {"NOT(a is NULL OR b is NULL)", new String[] {}},
                {"a is NOT NULL OR b is NOT NULL", new String[] {}}
        };
    }

    @DataProvider
    public static Object[][] standardOperatorTestCases()
    {
        return new Object[][] {
                {"a + b > 10", new String[] {"a", "b"}},
                {"a != 10 - b", new String[] {"a", "b"}},
                {"a > b", new String[] {"a", "b"}},
                {"a + NULL > b", new String[] {"a", "b"}},
                // We can infer NOT NULL predicates on arguments of an AND expression
                {"a > b and c = d", new String[] {"a", "b", "c", "d"}},
                {"a IS NULL and b > c", new String[] {"b", "c"}},
                // We cannot infer NOT NULL predicates on arguments of an OR expression
                {"a > b OR c = d", new String[] {}},
                // COALESCE can operate on NULL arguments, so cant infer predicates on its arguments
                {"COALESCE(a,b)", new String[] {}},
                // IN can operate on NULL arguments, so cant infer predicates on its arguments
                {"a IN (b,10,NULL)", new String[] {}},
                // arr[3] = 10 translates to EQUAL(SUBSCRIPT(arr, 3), 10). SUBSCRIPT is a standard Operator, so we can infer that 'arr' is NOT NULL
                {"arr[3] = 10", new String[] {"arr"}},
                // c_struct.a = 10 translates to EQUAL(DEREFERENCE(c_struct, 0), 10).
                // We chose to not make any inferences for DEREFERENCE clauses, hence we don't add any NOT NULL clauses for 'c_struct' or 'c_struct.a'
                {"c_struct.a = 10", new String[] {}},
                // NULLs are only inferred from standard operators
                {"NOT (b + 10 > c)", new String[] {}},
                {"abs(b + c) > 10", new String[] {}},
                {"random(b) = ceil(c)", new String[] {}},
                {"d > e and abs(b + c) > 10", new String[] {"d", "e"}},
        };
    }

    @DataProvider
    public static Object[][] nonStandardOperatorTestCases()
    {
        return new Object[][] {
                {"NOT (b + 10 > c)", new String[] {"b", "c"}},
                {"abs(b + c) > 10", new String[] {"b", "c"}},
                {"random(b) = ceil(c)", new String[] {"b", "c"}},
        };
    }

    @Test
    public void testNotNullPredicatesAddedForSingleEquiJoinClause()
    {
        String query = "select 1 from lineitem l join orders o on l.orderkey = o.orderkey";
        assertPlan(query,
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("LINE_ORDER_KEY", "ORDERS_ORDER_KEY")),
                                anyTree(
                                        filter("LINE_ORDER_KEY IS NOT NULL",
                                                tableScan("lineitem", ImmutableMap.of("LINE_ORDER_KEY", "orderkey")))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY IS NOT NULL",
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @Test
    public void testNotNullPredicatesAddedForCrossJoinReducedToInnerJoin()
    {
        String query = "select 1 from lineitem l, orders o where l.orderkey = o.orderkey";
        assertPlan(query,
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("LINE_ORDER_KEY", "ORDERS_ORDER_KEY")),
                                anyTree(
                                        filter("LINE_ORDER_KEY IS NOT NULL",
                                                tableScan("lineitem", ImmutableMap.of("LINE_ORDER_KEY", "orderkey")))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY IS NOT NULL",
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));

        query = "select 1 from lineitem l join orders o on l.orderkey = o.orderkey, customer c where c.custkey = o.custkey";
        assertPlan(query,
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("LINE_ORDER_KEY", "ORDERS_ORDER_KEY")),
                                anyTree(
                                        filter("LINE_ORDER_KEY IS NOT NULL",
                                                tableScan("lineitem", ImmutableMap.of("LINE_ORDER_KEY", "orderkey")))),
                                anyTree(join(INNER,
                                        ImmutableList.of(equiJoinClause("ORDERS_CUSTOMER_KEY", "CUSTOMER_CUSTOMER_KEY")),
                                        anyTree(
                                                filter("ORDERS_CUSTOMER_KEY IS NOT NULL AND ORDERS_ORDER_KEY IS NOT NULL",
                                                        tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey",
                                                                "ORDERS_CUSTOMER_KEY", "custkey")))),
                                        anyTree(
                                                filter("CUSTOMER_CUSTOMER_KEY IS NOT NULL",
                                                        tableScan("customer", ImmutableMap.of("CUSTOMER_CUSTOMER_KEY", "custkey")))))))));
    }

    @Test
    public void testMultipleNotNullsAddedForMultipleEquiJoinClause()
    {
        String query = "select 1 from lineitem l join orders o on l.orderkey = o.orderkey and l.partkey = o.custkey";
        assertPlan(query,
                anyTree(
                        join(INNER,
                                ImmutableList.of(equiJoinClause("LINE_ORDER_KEY", "ORDERS_ORDER_KEY"),
                                        equiJoinClause("partkey", "custkey")),
                                anyTree(
                                        filter("LINE_ORDER_KEY IS NOT NULL AND partkey IS NOT NULL",
                                                tableScan("lineitem",
                                                        ImmutableMap.of(
                                                                "LINE_ORDER_KEY", "orderkey",
                                                                "partkey", "partkey")))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY IS NOT NULL AND custkey IS NOT NULL",
                                                tableScan("orders",
                                                        ImmutableMap.of(
                                                                "ORDERS_ORDER_KEY", "orderkey",
                                                                "custkey", "custkey")))))));
    }

    @Test
    public void testNotNullInferredForJoinFilter()
    {
        String query = "select 1 from lineitem l join orders o on l.orderkey = o.orderkey and partkey + custkey > 10";
        assertPlan(query,
                anyTree(
                        join(INNER,
                                // Only single equi join clause in this case
                                ImmutableList.of(equiJoinClause("LINE_ORDER_KEY", "ORDERS_ORDER_KEY")),
                                // Extra join filter is passed unchanged
                                Optional.of("partkey + custkey > 10"),
                                // We can infer NOT NULL filters on partkey and custkey since the ADD function cannot operate on NULL arguments
                                anyTree(
                                        filter("LINE_ORDER_KEY IS NOT NULL AND partkey IS NOT NULL",
                                                tableScan("lineitem",
                                                        ImmutableMap.of(
                                                                "LINE_ORDER_KEY", "orderkey",
                                                                "partkey", "partkey")))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY IS NOT NULL AND custkey IS NOT NULL",
                                                tableScan("orders",
                                                        ImmutableMap.of(
                                                                "ORDERS_ORDER_KEY", "orderkey",
                                                                "custkey", "custkey")))))));
    }

    @Test
    public void testNotNullPredicatesAddedOnlyForInnerSideTablesVariableReferences()
    {
        String query = "select 1 from lineitem l left join orders o on l.orderkey = o.orderkey and partkey - custkey > 10";
        assertPlan(query,
                anyTree(
                        join(LEFT,
                                ImmutableList.of(equiJoinClause("LINE_ORDER_KEY", "ORDERS_ORDER_KEY")),
                                Optional.of("partkey - custkey > 10"),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of(
                                                "LINE_ORDER_KEY", "orderkey",
                                                "partkey", "partkey"))),
                                anyTree(
                                        filter("ORDERS_ORDER_KEY IS NOT NULL and custkey IS NOT NULL",
                                                tableScan("orders",
                                                        ImmutableMap.of(
                                                                "ORDERS_ORDER_KEY", "orderkey",
                                                                "custkey", "custkey")))))));

        query = "select 1 from lineitem l right join orders o on l.orderkey = o.orderkey and custkey > partkey";
        assertPlan(query,
                anyTree(
                        join(RIGHT,
                                ImmutableList.of(equiJoinClause("LINE_ORDER_KEY", "ORDERS_ORDER_KEY")),
                                Optional.of("custkey > partkey"),
                                anyTree(
                                        filter("LINE_ORDER_KEY IS NOT NULL and partkey IS NOT NULL",
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey",
                                                        "partkey", "partkey")))),
                                anyTree(
                                        tableScan("orders",
                                                ImmutableMap.of(
                                                        "ORDERS_ORDER_KEY", "orderkey",
                                                        "custkey", "custkey"))))));
    }

    @Test(dataProvider = "standardOperatorTestCases")
    public void testNotNullInferenceForInferFromStandardOperatorsStrategy(String filterSql, String[] expectedInferredNotNullVariables)
    {
        assertInferredNotNullVariableRefsListMatch(INFER_FROM_STANDARD_OPERATORS, filterSql, buildVariableReferencesList(expectedInferredNotNullVariables));
    }

    @Test(dataProvider = "nonStandardOperatorTestCases")
    public void testNotNullInferenceForUseFunctionMetadataStrategy(String filterSql, String[] expectedInferredNotNullVariables)
    {
        assertInferredNotNullVariableRefsListMatch(USE_FUNCTION_METADATA, filterSql, buildVariableReferencesList(expectedInferredNotNullVariables));
    }

    @Test(dataProvider = "getExistingNotNullVarsTestCases")
    public void testGetExistingNotNullVars(String filterSql, String[] expectedNotNullVars)
    {
        Set<VariableReferenceExpression> actual = new AddNotNullFiltersToJoinNode(functionAndTypeManager).getExistingNotNullVariables(
                Optional.of(rowExpressionTranslator.translate(filterSql, testVariableTypeMap)));

        assertEquals(actual, buildVariableReferencesList(expectedNotNullVars));
    }

    private List<VariableReferenceExpression> buildVariableReferencesList(String... var)
    {
        return Arrays.stream(var).map(x -> variable(x, testVariableTypeMap.get(x))).collect(Collectors.toList());
    }

    private void assertInferredNotNullVariableRefsListMatch(JoinNotNullInferenceStrategy notNullInferenceStrategy,
            String filterSql, List<VariableReferenceExpression> expectedInferredNotNullVariables)
    {
        ExtractInferredNotNullVariablesVisitor visitor = new ExtractInferredNotNullVariablesVisitor(functionAndTypeManager, notNullInferenceStrategy);

        RowExpression rowExpression = rowExpressionTranslator.translate(filterSql, testVariableTypeMap);
        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
        rowExpression.accept(visitor, builder);
        assertEquals(builder.build(), expectedInferredNotNullVariables);
    }

    static {
        ImmutableMap.Builder<String, Type> builder = ImmutableMap.builder();
        builder.put("a", BIGINT);
        builder.put("b", BIGINT);
        builder.put("c", BIGINT);
        builder.put("d", BIGINT);
        builder.put("e", BIGINT);
        builder.put("arr", new ArrayType(BIGINT));
        builder.put("c_struct", RowType.from(ImmutableList.of(RowType.field("a", BIGINT))));
        testVariableTypeMap = builder.build();
    }
}
