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

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;

public class RemoveArgumentsAfterNotNullFromCoalesce
    implements Rule<ProjectNode>
{
    private static final Capture<JoinNode> JOIN = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(join().capturedAs(JOIN)));

    @Override
    public Pattern<ProjectNode> getPattern() {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context) {
        // TODO: check if this is actually a coalesce node or another project, also define result
        JoinNode join = captures.get(JOIN);

        if (!join.getFilter().isPresent()) {
            return Result.empty();
        }

        List<RowExpression> notNulls = join.getFilter().get().getChildren();
        Map<VariableReferenceExpression, RowExpression> oldAssignmentsMap = projectNode.getAssignments().getMap();

        Map<VariableReferenceExpression, RowExpression> newAssignmentsMap = oldAssignmentsMap.keySet().stream()
                .filter(notNulls::contains)
                .collect(Collectors.toMap(Function.identity(), oldAssignmentsMap::get));
        Assignments newAssignments = Assignments.builder().putAll(newAssignmentsMap).build();

        ProjectNode rewrittenCoalesce = new ProjectNode(
                projectNode.getId(),
                projectNode.getSource(),
                newAssignments);

        return Result.empty();
    }
}
