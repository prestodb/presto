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
import com.facebook.presto.matching.PropertyPattern;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.iterative.rule.Util.restrictOutputs;
import static com.facebook.presto.sql.planner.iterative.rule.Util.transpose;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.isIdentity;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.window;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class GatherAndMergeWindows
{
    private GatherAndMergeWindows() {}

    public static Set<Rule<?>> rules()
    {
        // TODO convert to a pattern that allows for a sequence of ProjectNode, instead
        // of a canned number, once the pattern system supports it.
        return IntStream.range(0, 5)
                .boxed()
                .flatMap(numProjects ->
                        Stream.of(
                                new MergeAdjacentWindowsOverProjects(numProjects),
                                new SwapAdjacentWindowsBySpecifications(numProjects)))
                .collect(toImmutableSet());
    }

    private abstract static class ManipulateAdjacentWindowsOverProjects
            implements Rule<WindowNode>
    {
        private final Capture<WindowNode> childCapture = newCapture();
        private final List<Capture<ProjectNode>> projectCaptures;
        private final Pattern<WindowNode> pattern;

        protected ManipulateAdjacentWindowsOverProjects(int numProjects)
        {
            PropertyPattern<PlanNode, ?> childPattern = source().matching(window().capturedAs(childCapture));
            ImmutableList.Builder<Capture<ProjectNode>> projectCapturesBuilder = ImmutableList.builder();
            for (int i = 0; i < numProjects; ++i) {
                Capture<ProjectNode> projectCapture = newCapture();
                projectCapturesBuilder.add(projectCapture);
                childPattern = source().matching(project().capturedAs(projectCapture).with(childPattern));
            }
            this.projectCaptures = projectCapturesBuilder.build();
            this.pattern = window().with(childPattern);
        }

        @Override
        public Pattern<WindowNode> getPattern()
        {
            return pattern;
        }

        @Override
        public Result apply(WindowNode parent, Captures captures, Context context)
        {
            // Pulling the descendant WindowNode above projects is done as a part of this rule, as opposed in a
            // separate rule, because that pullup is not useful on its own, and could be undone by other rules.
            // For example, a rule could insert a project-off node between adjacent WindowNodes that use different
            // input symbols.
            List<ProjectNode> projects = projectCaptures.stream()
                    .map(captures::get)
                    .collect(toImmutableList());

            return pullWindowNodeAboveProjects(captures.get(childCapture), projects)
                    .flatMap(newChild -> manipulateAdjacentWindowNodes(parent, newChild, context))
                    .map(Result::ofPlanNode)
                    .orElse(Result.empty());
        }

        protected abstract Optional<PlanNode> manipulateAdjacentWindowNodes(WindowNode parent, WindowNode child, Context context);

        /**
         * Looks for the pattern (ProjectNode*)WindowNode, and rewrites it to WindowNode(ProjectNode*),
         * returning an empty option if it can't rewrite the projects, for example because they rely on
         * the output of the WindowNode.
         *
         * @param projects the nodes above the target, bottom first.
         */
        protected static Optional<WindowNode> pullWindowNodeAboveProjects(WindowNode target, List<ProjectNode> projects)
        {
            if (projects.isEmpty()) {
                return Optional.of(target);
            }

            PlanNode targetChild = target.getSource();

            Set<VariableReferenceExpression> targetInputs = ImmutableSet.copyOf(targetChild.getOutputVariables());
            Set<VariableReferenceExpression> targetOutputs = ImmutableSet.copyOf(target.getOutputVariables());

            PlanNode newTargetChild = targetChild;

            for (ProjectNode project : projects) {
                Set<VariableReferenceExpression> newTargetChildOutputs = ImmutableSet.copyOf(newTargetChild.getOutputVariables());

                // The only kind of use of the output of the target that we can safely ignore is a simple identity propagation.
                // The target node, when hoisted above the projections, will provide the symbols directly.
                Map<VariableReferenceExpression, RowExpression> assignmentsWithoutTargetOutputIdentities = Maps.filterKeys(
                        project.getAssignments().getMap(),
                        output -> !(isIdentity(project.getAssignments(), output) && targetOutputs.contains(output)));

                if (targetInputs.stream().anyMatch(assignmentsWithoutTargetOutputIdentities::containsKey)) {
                    // Redefinition of an input to the target -- can't handle this case.
                    return Optional.empty();
                }

                Assignments newAssignments = Assignments.builder()
                        .putAll(assignmentsWithoutTargetOutputIdentities)
                        .putAll(identityAssignments(targetInputs))
                        .build();

                if (!newTargetChildOutputs.containsAll(extractUnique(newAssignments))) {
                    // Projection uses an output of the target -- can't move the target above this projection.
                    return Optional.empty();
                }

                newTargetChild = new ProjectNode(project.getId(), newTargetChild, newAssignments, project.getLocality());
            }

            WindowNode newTarget = (WindowNode) target.replaceChildren(ImmutableList.of(newTargetChild));
            Set<VariableReferenceExpression> newTargetOutputs = ImmutableSet.copyOf(newTarget.getOutputVariables());
            if (!newTargetOutputs.containsAll(projects.get(projects.size() - 1).getOutputVariables())) {
                // The new target node is hiding some of the projections, which makes this rewrite incorrect.
                return Optional.empty();
            }
            return Optional.of(newTarget);
        }
    }

    private static Set<VariableReferenceExpression> extractUnique(Assignments assignments)
    {
        Collection<RowExpression> expressions = assignments.getExpressions();
        return VariablesExtractor.extractUnique(expressions);
    }

    private static boolean dependsOn(WindowNode parent, WindowNode child)
    {
        return parent.getPartitionBy().stream().anyMatch(child.getCreatedVariable()::contains)
                || (parent.getOrderingScheme().isPresent() && parent.getOrderingScheme().get().getOrderByVariables().stream()
                .anyMatch(child.getCreatedVariable()::contains))
                || parent.getWindowFunctions().values().stream()
                .map(function -> VariablesExtractor.extractUnique(function.getFunctionCall().getArguments()))
                .flatMap(Collection::stream)
                .anyMatch(child.getCreatedVariable()::contains);
    }

    public static class MergeAdjacentWindowsOverProjects
            extends ManipulateAdjacentWindowsOverProjects
    {
        public MergeAdjacentWindowsOverProjects(int numProjects)
        {
            super(numProjects);
        }

        @Override
        protected Optional<PlanNode> manipulateAdjacentWindowNodes(WindowNode parent, WindowNode child, Context context)
        {
            if (!child.getSpecification().equals(parent.getSpecification()) || dependsOn(parent, child)) {
                return Optional.empty();
            }

            ImmutableMap.Builder<VariableReferenceExpression, WindowNode.Function> functionsBuilder = ImmutableMap.builder();
            functionsBuilder.putAll(parent.getWindowFunctions());
            functionsBuilder.putAll(child.getWindowFunctions());

            WindowNode mergedWindowNode = new WindowNode(
                    parent.getId(),
                    child.getSource(),
                    parent.getSpecification(),
                    functionsBuilder.build(),
                    parent.getHashVariable(),
                    parent.getPrePartitionedInputs(),
                    parent.getPreSortedOrderPrefix());

            return Optional.of(
                    restrictOutputs(context.getIdAllocator(), mergedWindowNode, ImmutableSet.copyOf(parent.getOutputVariables()), true)
                            .orElse(mergedWindowNode));
        }
    }

    public static class SwapAdjacentWindowsBySpecifications
            extends ManipulateAdjacentWindowsOverProjects
    {
        public SwapAdjacentWindowsBySpecifications(int numProjects)
        {
            super(numProjects);
        }

        @Override
        protected Optional<PlanNode> manipulateAdjacentWindowNodes(WindowNode parent, WindowNode child, Context context)
        {
            if ((compare(parent, child) < 0) && (!dependsOn(parent, child))) {
                PlanNode transposedWindows = transpose(parent, child);
                return Optional.of(
                        restrictOutputs(context.getIdAllocator(), transposedWindows, ImmutableSet.copyOf(parent.getOutputVariables()), true)
                                .orElse(transposedWindows));
            }
            else {
                return Optional.empty();
            }
        }

        private static int compare(WindowNode o1, WindowNode o2)
        {
            int comparison = comparePartitionBy(o1, o2);
            if (comparison != 0) {
                return comparison;
            }

            comparison = compareOrderBy(o1, o2);
            if (comparison != 0) {
                return comparison;
            }

            // If PartitionBy and OrderBy clauses are identical, let's establish an arbitrary order to prevent non-deterministic results of swapping WindowNodes in such a case
            return o1.getId().toString().compareTo(o2.getId().toString());
        }

        private static int comparePartitionBy(WindowNode o1, WindowNode o2)
        {
            Iterator<VariableReferenceExpression> iterator1 = o1.getPartitionBy().iterator();
            Iterator<VariableReferenceExpression> iterator2 = o2.getPartitionBy().iterator();

            while (iterator1.hasNext() && iterator2.hasNext()) {
                VariableReferenceExpression variable1 = iterator1.next();
                VariableReferenceExpression variable2 = iterator2.next();

                int partitionByComparison = variable1.compareTo(variable2);
                if (partitionByComparison != 0) {
                    return partitionByComparison;
                }
            }

            if (iterator1.hasNext()) {
                return 1;
            }
            if (iterator2.hasNext()) {
                return -1;
            }
            return 0;
        }

        private static int compareOrderBy(WindowNode o1, WindowNode o2)
        {
            if (!o1.getOrderingScheme().isPresent() && !o2.getOrderingScheme().isPresent()) {
                return 0;
            }
            else if (o1.getOrderingScheme().isPresent() && !o2.getOrderingScheme().isPresent()) {
                return 1;
            }
            else if (!o1.getOrderingScheme().isPresent() && o2.getOrderingScheme().isPresent()) {
                return -1;
            }

            OrderingScheme o1OrderingScheme = o1.getOrderingScheme().get();
            OrderingScheme o2OrderingScheme = o2.getOrderingScheme().get();
            Iterator<VariableReferenceExpression> iterator1 = o1OrderingScheme.getOrderByVariables().iterator();
            Iterator<VariableReferenceExpression> iterator2 = o2OrderingScheme.getOrderByVariables().iterator();

            while (iterator1.hasNext() && iterator2.hasNext()) {
                VariableReferenceExpression variable1 = iterator1.next();
                VariableReferenceExpression variable2 = iterator2.next();

                int orderByComparison = variable1.compareTo(variable2);
                if (orderByComparison != 0) {
                    return orderByComparison;
                }
                else {
                    int sortOrderComparison = o1OrderingScheme.getOrdering(variable1).compareTo(o2OrderingScheme.getOrdering(variable2));
                    if (sortOrderComparison != 0) {
                        return sortOrderComparison;
                    }
                }
            }

            if (iterator1.hasNext()) {
                return 1;
            }
            if (iterator2.hasNext()) {
                return -1;
            }
            return 0;
        }
    }
}
