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

import com.facebook.presto.common.type.RowType;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.MergeProcessorNode;
import com.facebook.presto.sql.planner.plan.MergeWriterNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;

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

    private PushdownSubfields newOptimizer()
    {
        PushdownSubfields optimizer = new PushdownSubfields(tester().getMetadata(), tester().getExpressionManager());
        optimizer.setEnabledForTesting(true);
        return optimizer;
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
