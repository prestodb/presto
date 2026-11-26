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

import com.facebook.presto.Session;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.MergeProcessorNode;
import com.facebook.presto.sql.planner.plan.MergeWriterNode;
import com.facebook.presto.sql.planner.plan.RPCNode;
import com.facebook.presto.sql.planner.sanity.ValidateDependenciesChecker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PUSHDOWN_SUBFIELDS_ENABLED;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.plan.RPCNode.ExecutionPhase.COLLECT;
import static com.facebook.presto.sql.planner.plan.RPCNode.StreamingMode.BATCH;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;

/**
 * Regression tests for {@link PushdownSubfields#visitMergeWriter} and
 * {@link PushdownSubfields#visitMergeProcessor}.
 *
 * Without these visitor overrides, walking a plan that contains a
 * {@code MergeWriterNode} or {@code MergeProcessorNode} sitting on top of a
 * (Project → Values) source that re-emits an identity-assigned row-typed
 * variable (e.g. {@code $target_table_row_id}) throws
 * {@code PrestoException("Missing variable: ...")} from
 * {@code Context.addAssignment} because the merge variables are never seeded
 * into {@code context.variables} before the descent.
 */
public class TestPushdownSubfields
        extends BaseRuleTest
{
    private static final SchemaTableName TARGET_TABLE = new SchemaTableName("schema", "table");

    @BeforeClass
    @Override
    public void setUp()
    {
        tester = new RuleTester(emptyList(), ImmutableMap.of(PUSHDOWN_SUBFIELDS_ENABLED, "true"));
    }

    private PushdownSubfields newOptimizer()
    {
        PushdownSubfields optimizer = new PushdownSubfields(tester().getMetadata(), tester().getExpressionManager());
        return optimizer;
    }

    @Test
    public void testDeferredRpcGroupingOnlySubfieldSurvivesPruning()
    {
        Session session = tester().getSession();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, ids, getMetadata());
        VariableReferenceExpression ready = builder.variable("ready", VARCHAR);
        VariableReferenceExpression keyRow = builder.variable("key_row", RowType.from(ImmutableList.of(
                RowType.field("job_key", VARCHAR), RowType.field("unused", BIGINT))));
        VariableReferenceExpression unused = builder.variable("unused", VARCHAR);
        VariableReferenceExpression result = builder.variable("result", VARCHAR);
        PlanNode source = builder.project(
                Assignments.builder().put(ready, ready).put(keyRow, keyRow).put(unused, unused).build(),
                builder.values(ready, keyRow, unused));
        SpecialFormExpression grouping = new SpecialFormExpression(DEREFERENCE, VARCHAR,
                ImmutableList.of(keyRow, new ConstantExpression(0L, INTEGER)));
        RPCNode collect = new RPCNode(Optional.empty(), ids.getNextId(), Optional.empty(), source, "test_collect",
                ImmutableList.of(ready), ImmutableList.of(ready.getName()), result,
                BATCH, 2, 2, COLLECT, ImmutableList.of(grouping));
        OutputNode output = builder.output(ImmutableList.of("result"), ImmutableList.of(result), collect);
        VariableAllocator variables = new VariableAllocator(builder.getTypes().allVariables());

        PlanNode pruned = new PruneUnreferencedOutputs()
                .optimize(output, session, builder.getTypes(), variables, ids, WarningCollector.NOOP, false).getPlanNode();
        PlanNode optimized = newOptimizer()
                .optimize(pruned, session, builder.getTypes(), variables, ids, WarningCollector.NOOP, false).getPlanNode();

        assertEquals(optimized.getOutputVariables(), ImmutableList.of(result));
        RPCNode plannedCollect = (RPCNode) ((OutputNode) optimized).getSource();
        assertEquals(plannedCollect.getExecutionPhase(), COLLECT);
        assertEquals(plannedCollect.getArguments(), ImmutableList.of(ready));
        assertEquals(plannedCollect.getInputGroupingExpressions(), ImmutableList.of(grouping));
        assertEquals(plannedCollect.getSource().getOutputVariables(), ImmutableList.of(ready, keyRow));
        new ValidateDependenciesChecker().validate(optimized);
    }

    @Test
    public void testMergeWriterPreservesProjectedVariables()
    {
        tester().assertThat(newOptimizer())
                .on(p -> {
                    // Use a row-typed row-id (mirrors IcebergMetadataColumn.MERGE_TARGET_ROW_ID_DATA),
                    // which is exactly what makes PushdownSubfields consider the variable for
                    // subfield pruning in the original failure mode.
                    RowType rowIdType = RowType.anonymous(ImmutableList.of(UNKNOWN));
                    VariableReferenceExpression rowId = p.variable("target_table_row_id", rowIdType);
                    VariableReferenceExpression mergeRow = p.variable("merge_row");
                    VariableReferenceExpression partialRows = p.variable("partial_rows");
                    VariableReferenceExpression fragment = p.variable("fragment");

                    // Identity Project re-emitting the merge variables (this is the project
                    // that throws "Missing variable" without the visitor override).
                    PlanNode source = p.project(
                            Assignments.builder()
                                    .put(rowId, rowId)
                                    .put(mergeRow, mergeRow)
                                    .build(),
                            p.values(rowId, mergeRow));

                    List<VariableReferenceExpression> mergeProcessorProjectedSymbols =
                            ImmutableList.of(rowId, mergeRow);
                    return p.merge(
                            TARGET_TABLE,
                            source,
                            mergeProcessorProjectedSymbols,
                            ImmutableList.of(partialRows, fragment));
                })
                .matches(
                        node(
                                MergeWriterNode.class,
                                node(ProjectNode.class, node(ValuesNode.class))));
    }

    @Test
    public void testMergeProcessorPreservesRowIdAndMergeRowAndTargetColumns()
    {
        tester().assertThat(newOptimizer())
                .on(p -> {
                    RowType rowIdType = RowType.anonymous(ImmutableList.of(UNKNOWN));
                    VariableReferenceExpression rowId = p.variable("target_table_row_id", rowIdType);
                    VariableReferenceExpression mergeRow = p.variable("merge_row");
                    VariableReferenceExpression col1 = p.variable("col1");
                    VariableReferenceExpression col2 = p.variable("col2");

                    // Identity Project re-emitting all merge variables (this is the project
                    // that throws "Missing variable" without the visitor override).
                    PlanNode source = p.project(
                            Assignments.builder()
                                    .put(rowId, rowId)
                                    .put(mergeRow, mergeRow)
                                    .put(col1, col1)
                                    .put(col2, col2)
                                    .build(),
                            p.values(rowId, mergeRow, col1, col2));

                    return p.mergeProcessor(
                            TARGET_TABLE,
                            source,
                            rowId,
                            mergeRow,
                            ImmutableList.of(col1, col2),
                            ImmutableList.of(rowId, mergeRow, col1, col2));
                })
                .matches(
                        node(
                                MergeProcessorNode.class,
                                node(ProjectNode.class, node(ValuesNode.class))));
    }

    /**
     * Integration test exercising the full plan shape that triggered the original
     * "Missing variable: $target_table_row_id" failure on the iceberg MERGE path:
     *
     *   MergeWriter
     *     └── MergeProcessor
     *           └── Project(identity row-id, identity merge-row, identity columns)
     *                 └── Values
     *
     * Both {@code visitMergeWriter} AND {@code visitMergeProcessor} must seed
     * their merge variables into {@code context.variables} as the rewrite
     * descends, otherwise the identity {@code Project} below
     * {@code MergeProcessor} fails its {@code addAssignment} lookup for the
     * row-typed {@code target_table_row_id}.
     */
    @Test
    public void testMergeWriterAtopMergeProcessorAtopIdentityProject()
    {
        tester().assertThat(newOptimizer())
                .on(p -> {
                    RowType rowIdType = RowType.anonymous(ImmutableList.of(UNKNOWN));
                    VariableReferenceExpression rowId = p.variable("target_table_row_id", rowIdType);
                    VariableReferenceExpression mergeRow = p.variable("merge_row");
                    VariableReferenceExpression col1 = p.variable("col1");
                    VariableReferenceExpression col2 = p.variable("col2");
                    VariableReferenceExpression partialRows = p.variable("partial_rows");
                    VariableReferenceExpression fragment = p.variable("fragment");

                    // Identity Project — same shape produced by QueryPlanner.plan(Merge)
                    // re-emitting the row-id from a TableScan below.
                    PlanNode identityProject = p.project(
                            Assignments.builder()
                                    .put(rowId, rowId)
                                    .put(mergeRow, mergeRow)
                                    .put(col1, col1)
                                    .put(col2, col2)
                                    .build(),
                            p.values(rowId, mergeRow, col1, col2));

                    PlanNode mergeProcessor = p.mergeProcessor(
                            TARGET_TABLE,
                            identityProject,
                            rowId,
                            mergeRow,
                            ImmutableList.of(col1, col2),
                            ImmutableList.of(rowId, mergeRow, col1, col2));

                    List<VariableReferenceExpression> mergeProcessorProjectedSymbols =
                            ImmutableList.of(rowId, mergeRow, col1, col2);
                    return p.merge(
                            TARGET_TABLE,
                            mergeProcessor,
                            mergeProcessorProjectedSymbols,
                            ImmutableList.of(partialRows, fragment));
                })
                .matches(
                        node(
                                MergeWriterNode.class,
                                node(
                                        MergeProcessorNode.class,
                                        node(
                                                ProjectNode.class,
                                                node(ValuesNode.class)))));
    }
}
