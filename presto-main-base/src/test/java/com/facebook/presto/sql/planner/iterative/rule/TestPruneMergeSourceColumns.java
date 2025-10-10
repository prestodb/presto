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

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.MergeWriterNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneMergeSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testPruneInputColumn()
    {
        tester().assertThat(new PruneMergeSourceColumns())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression mergeRow = p.variable("merge_row");
                    VariableReferenceExpression rowId = p.variable("row_id");
                    VariableReferenceExpression partialRows = p.variable("partial_rows");
                    VariableReferenceExpression fragment = p.variable("fragment");
                    List<VariableReferenceExpression> mergeProcessorProjectedSymbols = ImmutableList.of(mergeRow, rowId);
                    return p.merge(
                            new SchemaTableName("schema", "table"),
                            p.values(a, mergeRow, rowId),
                            mergeProcessorProjectedSymbols,
                            ImmutableList.of(partialRows, fragment));
                })
                .matches(
                        node(
                                MergeWriterNode.class,
                                strictProject(
                                        ImmutableMap.of(
                                                "row_id", PlanMatchPattern.expression("row_id"),
                                                "merge_row", PlanMatchPattern.expression("merge_row")),
                                        values("a", "merge_row", "row_id"))));
    }

    @Test
    public void testDoNotPruneRowId()
    {
        tester().assertThat(new PruneMergeSourceColumns())
                .on(p -> {
                    VariableReferenceExpression mergeRow = p.variable("merge_row");
                    VariableReferenceExpression rowId = p.variable("row_id");
                    VariableReferenceExpression partialRows = p.variable("partial_rows");
                    VariableReferenceExpression fragment = p.variable("fragment");
                    List<VariableReferenceExpression> mergeProcessorProjectedSymbols = ImmutableList.of(mergeRow, rowId);
                    return p.merge(
                            new SchemaTableName("schema", "table"),
                            p.values(mergeRow, rowId),
                            mergeProcessorProjectedSymbols,
                            ImmutableList.of(partialRows, fragment));
                })
                .doesNotFire();
    }
}
