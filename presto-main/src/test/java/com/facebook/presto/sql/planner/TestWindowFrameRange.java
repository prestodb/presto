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
package com.facebook.presto.sql.planner;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.ShortDecimalType;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.LogicalPlanner.Stage.CREATED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.specification;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.window;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.windowFrame;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.CURRENT_ROW;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.FOLLOWING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.PRECEDING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.WindowType.RANGE;

public class TestWindowFrameRange
        extends BasePlanTest
{
    @Test
    public void testFramePrecedingWithSortKeyCoercions()
    {
        @Language("SQL") String sql = "SELECT array_agg(key) OVER(ORDER BY key RANGE x PRECEDING) " +
                "FROM (VALUES (1, 1.1), (2, 2.2)) T(key, x)";

        PlanMatchPattern pattern =
                anyTree(
                        window(
                                windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(
                                                ImmutableList.of(),
                                                ImmutableList.of("key"),
                                                ImmutableMap.of("key", SortOrder.ASC_NULLS_LAST)))
                                        .addFunction(
                                                "array_agg_result",
                                                functionCall("array_agg", ImmutableList.of("key")),
                                                createTestFunctionAndTypeManager().resolveFunction(Optional.empty(), Optional.empty(), QualifiedObjectName.valueOf("presto.default.array_agg"), fromTypes(INTEGER)),
                                                windowFrame(
                                                        RANGE,
                                                        PRECEDING,
                                                        Optional.of("frame_start_value"),
                                                        Optional.of(new ShortDecimalType(12, 1)),
                                                        Optional.of("key_for_frame_start_comparison"),
                                                        Optional.of(new ShortDecimalType(12, 1)),
                                                        CURRENT_ROW,
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        Optional.empty())),
                                project(// coerce sort key to compare sort key values with frame start values
                                        ImmutableMap.of("key_for_frame_start_comparison", expression("CAST(key AS decimal(12, 1))")),
                                        project(// calculate frame start value (sort key - frame offset)
                                                ImmutableMap.of("frame_start_value", expression(new FunctionCall(QualifiedName.of("presto", "default", "$operator$subtract"), ImmutableList.of(new SymbolReference("key_for_frame_start_calculation"), new SymbolReference("x"))))),
                                                project(// coerce sort key to calculate frame start values
                                                        ImmutableMap.of("key_for_frame_start_calculation", expression("CAST(key AS decimal(10, 0))")),
                                                        filter(// validate offset values
                                                                "IF((x >= CAST(0 AS DECIMAL(2,1))), " +
                                                                        "true, " +
                                                                        "CAST(presto.default.fail(CAST('Window frame offset value must not be negative or null' AS varchar)) AS boolean))",
                                                                anyTree(
                                                                        values(
                                                                                ImmutableList.of("key", "x"),
                                                                                ImmutableList.of(
                                                                                        ImmutableList.of(new LongLiteral("1"), new DecimalLiteral("1.1")),
                                                                                        ImmutableList.of(new LongLiteral("2"), new DecimalLiteral("2.2")))))))))));

        assertPlan(sql, CREATED, pattern);
    }

    @Test
    public void testFrameFollowingWithOffsetCoercion()
    {
        @Language("SQL") String sql = "SELECT array_agg(key) OVER(ORDER BY key RANGE BETWEEN CURRENT ROW AND x FOLLOWING) " +
                "FROM (VALUES (1.1, 1), (2.2, 2)) T(key, x)";

        PlanMatchPattern pattern =
                anyTree(
                        window(
                                windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(
                                                ImmutableList.of(),
                                                ImmutableList.of("key"),
                                                ImmutableMap.of("key", SortOrder.ASC_NULLS_LAST)))
                                        .addFunction(
                                                "array_agg_result",
                                                functionCall("array_agg", ImmutableList.of("key")),
                                                createTestFunctionAndTypeManager().resolveFunction(Optional.empty(), Optional.empty(),
                                                        QualifiedObjectName.valueOf("presto.default.array_agg"), fromTypes(createDecimalType(2, 1))),
                                                windowFrame(
                                                        RANGE,
                                                        CURRENT_ROW,
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        FOLLOWING,
                                                        Optional.of("frame_end_value"),
                                                        Optional.of(new ShortDecimalType(12, 1)),
                                                        Optional.of("key_for_frame_end_comparison"),
                                                        Optional.of(new ShortDecimalType(12, 1)))),
                                project(// coerce sort key to compare sort key values with frame end values
                                        ImmutableMap.of("key_for_frame_end_comparison", expression("CAST(key AS decimal(12, 1))")),
                                        project(// calculate frame end value (sort key + frame offset)
                                                ImmutableMap.of("frame_end_value", expression(new FunctionCall(QualifiedName.of("presto", "default", "$operator$add"), ImmutableList.of(new SymbolReference("key"), new SymbolReference("offset"))))),
                                                filter(// validate offset values
                                                        "IF((offset >= CAST(0 AS DECIMAL(10, 0))), " +
                                                                "true, " +
                                                                "CAST(presto.default.fail(CAST('Window frame offset value must not be negative or null' AS varchar)) AS boolean))",
                                                        project(// coerce offset value to calculate frame end values
                                                                ImmutableMap.of("offset", expression("CAST(x AS decimal(10, 0))")),
                                                                anyTree(
                                                                        values(
                                                                                ImmutableList.of("key", "x"),
                                                                                ImmutableList.of(
                                                                                        ImmutableList.of(new DecimalLiteral("1.1"), new LongLiteral("1")),
                                                                                        ImmutableList.of(new DecimalLiteral("2.2"), new LongLiteral("2")))))))))));

        assertPlan(sql, CREATED, pattern);
    }

    @Test
    public void testFramePrecedingFollowingNoCoercions()
    {
        @Language("SQL") String sql = "SELECT array_agg(key) OVER(ORDER BY key RANGE BETWEEN x PRECEDING AND y FOLLOWING) " +
                "FROM (VALUES (1, 1, 1), (2, 2, 2)) T(key, x, y)";

        PlanMatchPattern pattern =
                anyTree(
                        window(
                                windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(
                                                ImmutableList.of(),
                                                ImmutableList.of("key"),
                                                ImmutableMap.of("key", SortOrder.ASC_NULLS_LAST)))
                                        .addFunction(
                                                "array_agg_result",
                                                functionCall("array_agg", ImmutableList.of("key")),
                                                createTestFunctionAndTypeManager().resolveFunction(Optional.empty(), Optional.empty(), QualifiedObjectName.valueOf("presto.default.array_agg"), fromTypes(INTEGER)),
                                                windowFrame(
                                                        RANGE,
                                                        PRECEDING,
                                                        Optional.of("frame_start_value"),
                                                        Optional.of(INTEGER),
                                                        Optional.of("key"),
                                                        Optional.of(INTEGER),
                                                        FOLLOWING,
                                                        Optional.of("frame_end_value"),
                                                        Optional.of(INTEGER),
                                                        Optional.of("key"),
                                                        Optional.of(INTEGER))),
                                project(// calculate frame end value (sort key + frame end offset)
                                        ImmutableMap.of("frame_end_value", expression(new FunctionCall(QualifiedName.of("presto", "default", "$operator$add"), ImmutableList.of(new SymbolReference("key"), new SymbolReference("y"))))),
                                        filter(// validate frame end offset values
                                                "IF((y >= CAST(0 AS INTEGER)), " +
                                                        "true, " +
                                                        "CAST(presto.default.fail(CAST('Window frame offset value must not be negative or null' AS varchar)) AS boolean))",
                                                project(// calculate frame start value (sort key - frame start offset)pattern = {PlanMatchPattern@4789} "- anyTree\n    - node(WindowNode)\n        WindowMatcher{specification=SpecificationProvider{partitionBy=[], orderBy=[key], orderings={key=ASC_NULLS_LAST}}}\n        bind array_agg_result -> WindowFunctionMatcher{callMaker=array_agg (key) , functionHandle=presto.default.array_agg(integer):array(integer), frameMaker=WindowFrameProvider{type=RANGE, startType=PRECEDING, startValue=Optional[frame_start_value], endType=FOLLOWING, endValue=Optional[frame_end_value]}}\n        - node(ProjectNode)\n            bind frame_end_value -> "$operator$add"("key", "y")\n            - node(FilterNode)\n                FilterMatcher{predicate=IF(("y" >= CAST(0 AS integer)), true, CAST("fail"(CAST('Window frame offset value must not be negative or null' AS varchar)) AS boolean))}\n                - node(ProjectNode)\n                    bind frame_start_value -> "$operator$subtract"("key", "x")\n                    - node(FilterNode)\n                        FilterMatcher{predicate=IF(("x" >= CAST(0 AS integer)), t"… View
                                                        ImmutableMap.of("frame_start_value", expression(new FunctionCall(QualifiedName.of("presto", "default", "$operator$subtract"), ImmutableList.of(new SymbolReference("key"), new SymbolReference("x"))))),
                                                        filter(// validate frame start offset values
                                                                "IF((x >= CAST(0 AS INTEGER)), " +
                                                                        "true, " +
                                                                        "CAST(presto.default.fail(CAST('Window frame offset value must not be negative or null' AS varchar)) AS boolean))",
                                                                anyTree(
                                                                        values(
                                                                                ImmutableList.of("key", "x", "y"),
                                                                                ImmutableList.of(
                                                                                        ImmutableList.of(new LongLiteral("1"), new LongLiteral("1"), new LongLiteral("1")),
                                                                                        ImmutableList.of(new LongLiteral("2"), new LongLiteral("2"), new LongLiteral("2")))))))))));

        assertPlan(sql, CREATED, pattern);
    }
}
