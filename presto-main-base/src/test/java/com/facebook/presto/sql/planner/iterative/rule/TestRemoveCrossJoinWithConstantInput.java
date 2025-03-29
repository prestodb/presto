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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT;
import static com.facebook.presto.common.block.MapBlock.fromKeyValueBlock;
import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.testing.TestingEnvironment.getOperatorMethodHandle;

public class TestRemoveCrossJoinWithConstantInput
        extends BaseRuleTest
{
    private static final MethodHandle KEY_NATIVE_EQUALS = getOperatorMethodHandle(OperatorType.EQUAL, BIGINT, BIGINT);
    private static final MethodHandle KEY_BLOCK_EQUALS = compose(KEY_NATIVE_EQUALS, nativeValueGetter(BIGINT), nativeValueGetter(BIGINT));
    private static final MethodHandle KEY_NATIVE_HASH_CODE = getOperatorMethodHandle(OperatorType.HASH_CODE, BIGINT);
    private static final MethodHandle KEY_BLOCK_HASH_CODE = compose(KEY_NATIVE_HASH_CODE, nativeValueGetter(BIGINT));

    @Test
    public void testOneColumnValuesNode()
    {
        tester().assertThat(new RemoveCrossJoinWithConstantInput(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_k1", new ArrayType(BIGINT));
                    p.variable("right_k1", BIGINT);
                    return p.join(JoinType.INNER,
                            p.tableScan(ImmutableList.of(leftKey), ImmutableMap.of(leftKey, new TestingColumnHandle("col"))),
                            p.values(ImmutableList.of(p.variable("right_k1")), ImmutableList.of(ImmutableList.of(constant(1L, BIGINT)))));
                })
                .matches(
                        node(ProjectNode.class,
                                node(TableScanNode.class)));
    }

    @Test
    public void testOneColumnValuesNodeArray()
    {
        tester().assertThat(new RemoveCrossJoinWithConstantInput(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_k1", new ArrayType(BIGINT));
                    VariableReferenceExpression rightKey = p.variable("right_k1", new ArrayType(BIGINT));
                    return p.join(JoinType.INNER,
                            p.tableScan(ImmutableList.of(leftKey), ImmutableMap.of(leftKey, new TestingColumnHandle("col"))),
                            p.values(ImmutableList.of(rightKey), ImmutableList.of(ImmutableList.of(constant(new LongArrayBlock(2, Optional.empty(), new long[2]), new ArrayType(BIGINT))))));
                })
                .matches(
                        node(ProjectNode.class,
                                node(TableScanNode.class)));
    }

    @Test
    public void testOneColumnValuesNodeMap()
    {
        long[] keys = {1, 2};
        LongArrayBlock longArrayBlock = new LongArrayBlock(2, Optional.empty(), keys);
        int[] offsets = {0, 2, 4, 6};
        Block mapBlock = fromKeyValueBlock(2, Optional.ofNullable(null), offsets, longArrayBlock, longArrayBlock);

        tester().assertThat(new RemoveCrossJoinWithConstantInput(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_k1", new ArrayType(BIGINT));
                    VariableReferenceExpression rightKey = p.variable("right_k1", new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE));
                    return p.join(JoinType.INNER,
                            p.tableScan(ImmutableList.of(leftKey), ImmutableMap.of(leftKey, new TestingColumnHandle("col"))),
                            p.values(ImmutableList.of(rightKey), ImmutableList.of(ImmutableList.of(constant(mapBlock, new MapType(BIGINT, BIGINT, KEY_BLOCK_EQUALS, KEY_BLOCK_HASH_CODE))))));
                })
                .matches(
                        node(ProjectNode.class,
                                node(TableScanNode.class)));
    }

    @Test
    public void testOneColumnValuesNodeNonDeterministic()
    {
        tester().assertThat(new RemoveCrossJoinWithConstantInput(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_k1", new ArrayType(BIGINT));
                    p.variable("right_k1", BIGINT);
                    return p.join(JoinType.INNER,
                            p.tableScan(ImmutableList.of(leftKey), ImmutableMap.of(leftKey, new TestingColumnHandle("col"))),
                            p.values(ImmutableList.of(p.variable("right_k1")), ImmutableList.of(ImmutableList.of(p.rowExpression("random()")))));
                }).doesNotFire();
    }

    @Test
    public void testOneColumnValuesNodeWithJoinFilter()
    {
        tester().assertThat(new RemoveCrossJoinWithConstantInput(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_k1", BIGINT);
                    p.variable("right_k1", BIGINT);
                    return p.join(JoinType.INNER,
                            p.tableScan(ImmutableList.of(leftKey), ImmutableMap.of(leftKey, new TestingColumnHandle("col"))),
                            p.values(ImmutableList.of(p.variable("right_k1")), ImmutableList.of(ImmutableList.of(constant(1L, BIGINT)))),
                            p.rowExpression("left_k1+right_k1 > 2"));
                })
                .matches(
                        node(FilterNode.class,
                                node(ProjectNode.class,
                                        node(TableScanNode.class))));
    }

    @Test
    public void testMultipleColumnsValuesNode()
    {
        tester().assertThat(new RemoveCrossJoinWithConstantInput(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_k1", new ArrayType(BIGINT));
                    VariableReferenceExpression rightK1 = p.variable("right_k1", BIGINT);
                    VariableReferenceExpression rightK2 = p.variable("right_k2", BIGINT);
                    return p.join(JoinType.INNER,
                            p.tableScan(ImmutableList.of(leftKey), ImmutableMap.of(leftKey, new TestingColumnHandle("col"))),
                            p.values(ImmutableList.of(rightK1, rightK2), ImmutableList.of(ImmutableList.of(constant(1L, BIGINT), constant(2L, BIGINT)))));
                })
                .matches(
                        node(ProjectNode.class,
                                node(TableScanNode.class)));
    }

    @Test
    public void testMultipleRowsValuesNode()
    {
        tester().assertThat(new RemoveCrossJoinWithConstantInput(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_k1", new ArrayType(BIGINT));
                    p.variable("right_k1", BIGINT);
                    return p.join(JoinType.INNER,
                            p.tableScan(ImmutableList.of(leftKey), ImmutableMap.of(leftKey, new TestingColumnHandle("col"))),
                            p.values(ImmutableList.of(p.variable("right_k1")), ImmutableList.of(ImmutableList.of(constant(1L, BIGINT)), ImmutableList.of(constant(2L, BIGINT)))));
                })
                .doesNotFire();
    }

    @Test
    public void testEmptyValuesNode()
    {
        tester().assertThat(new RemoveCrossJoinWithConstantInput(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_k1", new ArrayType(BIGINT));
                    p.variable("right_k1", BIGINT);
                    return p.join(JoinType.INNER,
                            p.tableScan(ImmutableList.of(leftKey), ImmutableMap.of(leftKey, new TestingColumnHandle("col"))),
                            p.values(p.variable("right_k1")));
                })
                .doesNotFire();
    }

    @Test
    public void testProjectConstant()
    {
        tester().assertThat(new RemoveCrossJoinWithConstantInput(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_k1", new ArrayType(BIGINT));
                    VariableReferenceExpression rightKey = p.variable("right_k", new ArrayType(BIGINT));
                    p.variable("right_k1", BIGINT);
                    return p.join(JoinType.INNER,
                            p.tableScan(ImmutableList.of(leftKey), ImmutableMap.of(leftKey, new TestingColumnHandle("col"))),
                            p.project(
                                    assignment(rightKey, p.rowExpression("1")),
                                    p.values(1)));
                })
                .matches(
                        node(ProjectNode.class,
                                node(TableScanNode.class)));
    }

    @Test
    public void testProjectConstantMultipleRows()
    {
        tester().assertThat(new RemoveCrossJoinWithConstantInput(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_k1", new ArrayType(BIGINT));
                    VariableReferenceExpression rightKey = p.variable("right_k", new ArrayType(BIGINT));
                    p.variable("right_k1", BIGINT);
                    return p.join(JoinType.INNER,
                            p.tableScan(ImmutableList.of(leftKey), ImmutableMap.of(leftKey, new TestingColumnHandle("col"))),
                            p.project(
                                    assignment(rightKey, p.rowExpression("1")),
                                    p.values(2)));
                })
                .doesNotFire();
    }

    @Test
    public void testMultipleProjects()
    {
        tester().assertThat(new RemoveCrossJoinWithConstantInput(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_k1", new ArrayType(BIGINT));
                    VariableReferenceExpression rightKey = p.variable("right_k", new ArrayType(BIGINT));
                    VariableReferenceExpression rightKey2 = p.variable("right_k2", new ArrayType(BIGINT));
                    p.variable("right_k1", BIGINT);
                    return p.join(JoinType.INNER,
                            p.tableScan(ImmutableList.of(leftKey), ImmutableMap.of(leftKey, new TestingColumnHandle("col"))),
                            p.project(
                                    assignment(rightKey, rightKey2),
                                    p.project(
                                            assignment(rightKey2, p.rowExpression("1")),
                                            p.values(1))));
                })
                .matches(
                        node(ProjectNode.class,
                                node(TableScanNode.class)));
    }

    @Test
    public void testOneColumnValuesNodeOnLeft()
    {
        tester().assertThat(new RemoveCrossJoinWithConstantInput(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_k1", new ArrayType(BIGINT));
                    p.variable("right_k1", BIGINT);
                    return p.join(JoinType.INNER,
                            p.values(ImmutableList.of(p.variable("right_k1")), ImmutableList.of(ImmutableList.of(constant(1L, BIGINT)))),
                            p.tableScan(ImmutableList.of(leftKey), ImmutableMap.of(leftKey, new TestingColumnHandle("col"))));
                })
                .matches(
                        node(ProjectNode.class,
                                node(TableScanNode.class)));
    }

    @Test
    public void testOneColumnValuesNodeExpression()
    {
        tester().assertThat(new RemoveCrossJoinWithConstantInput(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT, "true")
                .on(p ->
                {
                    VariableReferenceExpression leftKey = p.variable("left_k1", new ArrayType(BIGINT));
                    VariableReferenceExpression rightKey1 = p.variable("right_k1", BIGINT);
                    VariableReferenceExpression rightKey2 = p.variable("right_k2", VARCHAR);
                    p.variable("right_k1", BIGINT);
                    return p.join(JoinType.INNER,
                            p.values(ImmutableList.of(leftKey), ImmutableList.of(ImmutableList.of(constant(1L, BIGINT)), ImmutableList.of(constant(2L, BIGINT)))),
                            p.project(
                                    assignment(rightKey2, p.rowExpression("cast(right_k1 as varchar)")),
                            p.values(ImmutableList.of(rightKey1), ImmutableList.of(ImmutableList.of(constant(1L, BIGINT))))));
                })
                .matches(
                        project(ImmutableMap.of("left_k1", expression("left_k1"), "right_k2", expression("cast(1 as varchar)")),
                                values("left_k1")));
    }
}
