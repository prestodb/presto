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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.ExternalCallExpressionChecker;
import com.facebook.presto.sql.planner.optimizations.SymbolMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isRemoteFunctionsEnabled;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.REMOTE;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.UNKNOWN;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class PlanRemotePojections
        implements Rule<ProjectNode>
{
    private static final Pattern<ProjectNode> PATTERN = project();

    private final FunctionAndTypeManager functionAndTypeManager;

    public PlanRemotePojections(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Rule.Context context)
    {
        if (!node.getLocality().equals(UNKNOWN)) {
            // Already planned
            return Result.empty();
        }
        // Fast check for remote functions
        if (node.getAssignments().getExpressions().stream().noneMatch(expression -> expression.accept(new ExternalCallExpressionChecker(functionAndTypeManager), null))) {
            // No remote function
            return Result.ofPlanNode(new ProjectNode(node.getId(), node.getSource(), node.getAssignments(), LOCAL));
        }
        if (!isRemoteFunctionsEnabled(context.getSession())) {
            throw new PrestoException(GENERIC_USER_ERROR, "Remote functions are not enabled");
        }
        List<ProjectionContext> projectionContexts = planRemoteAssignments(node.getAssignments(), context.getVariableAllocator());
        checkState(!projectionContexts.isEmpty(), "Expect non-empty projectionContexts");
        PlanNode rewritten = node.getSource();
        for (ProjectionContext projectionContext : projectionContexts) {
            rewritten = new ProjectNode(context.getIdAllocator().getNextId(), rewritten, Assignments.builder().putAll(projectionContext.getProjections()).build(), projectionContext.remote ? REMOTE : LOCAL);
        }
        return Result.ofPlanNode(rewritten);
    }

    @VisibleForTesting
    public List<ProjectionContext> planRemoteAssignments(Assignments assignments, PlanVariableAllocator variableAllocator)
    {
        ImmutableList.Builder<List<ProjectionContext>> assignmentProjections = ImmutableList.builder();
        for (Map.Entry<VariableReferenceExpression, RowExpression> entry : assignments.getMap().entrySet()) {
            List<ProjectionContext> rewritten = entry.getValue().accept(new Visitor(functionAndTypeManager, variableAllocator), null);
            if (rewritten.isEmpty()) {
                assignmentProjections.add(ImmutableList.of(new ProjectionContext(ImmutableMap.of(entry.getKey(), entry.getValue()), false)));
            }
            else {
                checkState(rewritten.get(rewritten.size() - 1).getProjections().size() == 1, "Expect at most 1 assignment from last projection in rewrite");
                ProjectionContext last = rewritten.get(rewritten.size() - 1);
                ImmutableList.Builder<ProjectionContext> projectionContextBuilder = ImmutableList.builder();
                projectionContextBuilder.addAll(rewritten.subList(0, rewritten.size() - 1));
                projectionContextBuilder.add(new ProjectionContext(ImmutableMap.of(entry.getKey(), getOnlyElement(last.getProjections().values())), last.isRemote()));
                assignmentProjections.add(projectionContextBuilder.build());
            }
        }
        List<ProjectionContext> mergedProjectionContexts = mergeProjectionContexts(assignmentProjections.build());
        return dedupVariables(mergedProjectionContexts);
    }

    private static List<ProjectionContext> dedupVariables(List<ProjectionContext> projectionContexts)
    {
        ImmutableList.Builder<ProjectionContext> deduppedProjectionContexts = ImmutableList.builder();
        Set<VariableReferenceExpression> originalVariable = projectionContexts.get(projectionContexts.size() - 1).getProjections().keySet();
        SymbolMapper mapper = null;
        for (int i = 0; i < projectionContexts.size(); i++) {
            Map<VariableReferenceExpression, RowExpression> projections = projectionContexts.get(i).getProjections();
            // Apply mapping from previous projection
            if (mapper != null) {
                ImmutableMap.Builder<VariableReferenceExpression, RowExpression> newProjections = ImmutableMap.builder();
                for (Map.Entry<VariableReferenceExpression, RowExpression> entry : projections.entrySet()) {
                    newProjections.put(entry.getKey(), mapper.map(entry.getValue()));
                }
                projections = newProjections.build();
            }

            // Dedup
            ImmutableMultimap.Builder<RowExpression, VariableReferenceExpression> reverseProjectionsBuilder = ImmutableMultimap.builder();
            projections.forEach((key, value) -> reverseProjectionsBuilder.put(value, key));
            ImmutableMultimap<RowExpression, VariableReferenceExpression> reverseProjections = reverseProjectionsBuilder.build();
            if (reverseProjections.keySet().size() == projectionContexts.get(i).getProjections().size() && reverseProjections.keySet().stream().noneMatch(VariableReferenceExpression.class::isInstance)) {
                // No duplication
                deduppedProjectionContexts.add(new ProjectionContext(projections, projectionContexts.get(i).isRemote()));
                mapper = null;
            }
            else {
                SymbolMapper.Builder mapperBuilder = SymbolMapper.builder();
                ImmutableMap.Builder<VariableReferenceExpression, RowExpression> dedupedProjectionsBuilder = ImmutableMap.builder();
                for (RowExpression key : reverseProjections.keySet()) {
                    List<VariableReferenceExpression> values = ImmutableList.copyOf(reverseProjections.get(key));
                    if (key instanceof VariableReferenceExpression) {
                        values.forEach(variable -> mapperBuilder.put(variable, (VariableReferenceExpression) key));
                        dedupedProjectionsBuilder.put((VariableReferenceExpression) key, key);
                    }
                    else if (values.size() > 1) {
                        // Consolidate to one variable, prefer variables from original plan
                        List<VariableReferenceExpression> fromOriginal = originalVariable.stream().filter(values::contains).collect(toImmutableList());
                        VariableReferenceExpression variable = fromOriginal.isEmpty() ? values.get(0) : getOnlyElement(fromOriginal);
                        for (int j = 0; j < values.size(); j++) {
                            if (!values.get(j).equals(variable)) {
                                mapperBuilder.put(values.get(j), variable);
                            }
                        }
                        dedupedProjectionsBuilder.put(variable, key);
                    }
                    else {
                        checkState(values.size() == 1, "Expect only 1 value");
                        dedupedProjectionsBuilder.put(values.get(0), key);
                    }
                }
                deduppedProjectionContexts.add(new ProjectionContext(dedupedProjectionsBuilder.build(), projectionContexts.get(i).isRemote()));
                mapper = mapperBuilder.build();
            }
        }
        return deduppedProjectionContexts.build();
    }

    private static List<ProjectionContext> mergeProjectionContexts(List<List<ProjectionContext>> projectionContexts)
    {
        int assignmentsCount = projectionContexts.size();
        int[] indices = new int[assignmentsCount];
        ImmutableList.Builder<ProjectionContext> mergedAssignments = ImmutableList.builder();
        boolean remote = false;
        while (true) {
            boolean finished = true;
            for (int i = 0; i < projectionContexts.size(); i++) {
                if (projectionContexts.get(i).size() > indices[i]) {
                    finished = false;
                    break;
                }
            }
            if (finished) {
                break;
            }

            ImmutableMap.Builder<VariableReferenceExpression, RowExpression> projectionBuilder = ImmutableMap.builder();
            boolean hasNonIdentityProjection = false;
            for (int i = 0; i < projectionContexts.size(); i++) {
                if (projectionContexts.get(i).size() > indices[i]) {
                    ProjectionContext projectionContext = projectionContexts.get(i).get(indices[i]);
                    if (projectionContexts.get(i).get(indices[i]).isRemote() == remote) {
                        projectionBuilder.putAll(projectionContext.getProjections());
                        indices[i]++;
                        hasNonIdentityProjection = true;
                    }
                    else if (remote && !projectionContext.isRemote()) {
                        // For remote stage, pass identity projection for local parameters
                        projectionBuilder.putAll(projectionContext.getProjections().keySet().stream().collect(toImmutableMap(identity(), identity())));
                    }
                }
                else {
                    // Pass identity projection for shorter assignment chains
                    Map<VariableReferenceExpression, RowExpression> projections = projectionContexts.get(i).get(projectionContexts.get(i).size() - 1).getProjections();
                    projectionBuilder.putAll(projections.keySet().stream().collect(toImmutableMap(identity(), identity())));
                }
            }
            ImmutableMap<VariableReferenceExpression, RowExpression> merged = projectionBuilder.build();
            if (hasNonIdentityProjection) {
                // Have non-identity assignments
                mergedAssignments.add(new ProjectionContext(merged, remote));
            }
            remote = !remote;
        }
        return mergedAssignments.build();
    }

    private static VariableReferenceExpression getAssignedArgument(List<ProjectionContext> projectionContexts)
    {
        checkState(projectionContexts.get(projectionContexts.size() - 1).getProjections().size() == 1, "Expect only 1 projection for argument");
        return getOnlyElement(projectionContexts.get(projectionContexts.size() - 1).getProjections().keySet());
    }

    private static class Visitor
            implements RowExpressionVisitor<List<ProjectionContext>, Void>
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private final PlanVariableAllocator variableAllocator;

        public Visitor(FunctionAndTypeManager functionAndTypeManager, PlanVariableAllocator variableAllocator)
        {
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        }

        @Override
        public List<ProjectionContext> visitCall(CallExpression call, Void context)
        {
            FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle());
            boolean local = !functionMetadata.getImplementationType().isExternal();

            // Break function arguments into local and remote projections first
            ImmutableList.Builder<RowExpression> newArgumentsBuilder = ImmutableList.builder();
            List<ProjectionContext> processedArguments = processArguments(call.getArguments(), newArgumentsBuilder, local);
            List<RowExpression> newArguments = newArgumentsBuilder.build();
            CallExpression newCall = new CallExpression(
                    call.getDisplayName(),
                    call.getFunctionHandle(),
                    call.getType(),
                    newArguments);

            if (local) {
                if (processedArguments.size() == 0 || (processedArguments.size() == 1 && !processedArguments.get(0).isRemote())) {
                    // This call and all its arguments are local
                    return ImmutableList.of();
                }
                else if (!processedArguments.get(processedArguments.size() - 1).isRemote()) {
                    // This call and its arguments has local projections, merge the call into the last local projection
                    ImmutableList.Builder<ProjectionContext> projectionContextBuilder = ImmutableList.builder();
                    projectionContextBuilder.addAll(processedArguments.subList(0, processedArguments.size() - 1));
                    ProjectionContext last = processedArguments.get(processedArguments.size() - 1);
                    projectionContextBuilder.add(new ProjectionContext(
                            ImmutableMap.of(
                                    variableAllocator.newVariable(call),
                                    new CallExpression(
                                            call.getDisplayName(),
                                            call.getFunctionHandle(),
                                            call.getType(),
                                            newArguments.stream()
                                                    .map(argument -> argument instanceof VariableReferenceExpression ? last.getProjections().get(argument) : argument)
                                                    .collect(toImmutableList()))),
                            false));
                    return projectionContextBuilder.build();
                }
                else {
                    // This call is local but last projection is remote, add another level of projection
                    ImmutableList.Builder<ProjectionContext> projectionContextBuilder = ImmutableList.builder();
                    projectionContextBuilder.addAll(processedArguments);
                    projectionContextBuilder.add(new ProjectionContext(ImmutableMap.of(variableAllocator.newVariable(newCall), newCall), false));
                    return projectionContextBuilder.build();
                }
            }
            else {
                // this call is remote, add another level of projection
                // TODO if all arguments are input reference or constant (maybe variable reference?) we could skip a projection
                ImmutableList.Builder<ProjectionContext> projectionContextBuilder = ImmutableList.builder();
                projectionContextBuilder.addAll(processedArguments);
                projectionContextBuilder.add(new ProjectionContext(ImmutableMap.of(variableAllocator.newVariable(newCall), newCall), true));
                return projectionContextBuilder.build();
            }
        }

        @Override
        public List<ProjectionContext> visitInputReference(InputReferenceExpression reference, Void context)
        {
            throw new IllegalStateException("Optimizers should not see InputReferenceExpression");
        }

        @Override
        public List<ProjectionContext> visitConstant(ConstantExpression literal, Void context)
        {
            return ImmutableList.of();
        }

        @Override
        public List<ProjectionContext> visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return ImmutableList.of();
        }

        @Override
        public List<ProjectionContext> visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return ImmutableList.of();
        }

        @Override
        public List<ProjectionContext> visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            ImmutableList.Builder<RowExpression> newArgumentsBuilder = ImmutableList.builder();
            List<ProjectionContext> processedArguments = processArguments(specialForm.getArguments(), newArgumentsBuilder, true);
            List<RowExpression> newArguments = newArgumentsBuilder.build();
            if (processedArguments.size() == 0 || (processedArguments.size() == 1 && !processedArguments.get(0).isRemote())) {
                // Arguments do not contain remote projection
                return ImmutableList.of();
            }
            else if (!processedArguments.get(processedArguments.size() - 1).isRemote()) {
                // There are remote projections, but the previous stage is local, so merge them
                ImmutableList.Builder<ProjectionContext> projectionContextBuilder = ImmutableList.builder();
                projectionContextBuilder.addAll(processedArguments.subList(0, processedArguments.size() - 1));
                ProjectionContext last = processedArguments.get(processedArguments.size() - 1);
                projectionContextBuilder.add(new ProjectionContext(
                        ImmutableMap.of(
                                variableAllocator.newVariable(specialForm),
                                new SpecialFormExpression(
                                        specialForm.getForm(),
                                        specialForm.getType(),
                                        newArguments.stream()
                                                .map(argument -> argument instanceof VariableReferenceExpression ? last.getProjections().get(argument) : argument)
                                                .collect(toImmutableList()))),
                        false));
                return projectionContextBuilder.build();
            }
            else {
                // Last projection is remote, add another level of projection
                ImmutableList.Builder<ProjectionContext> projectionContextBuilder = ImmutableList.builder();
                projectionContextBuilder.addAll(processedArguments);
                projectionContextBuilder.add(new ProjectionContext(
                        ImmutableMap.of(
                                variableAllocator.newVariable(specialForm),
                                new SpecialFormExpression(
                                        specialForm.getForm(),
                                        specialForm.getType(),
                                        newArguments)),
                        false));
                return projectionContextBuilder.build();
            }
        }

        private List<ProjectionContext> processArguments(List<RowExpression> arguments, ImmutableList.Builder<RowExpression> newArguments, boolean local)
        {
            // Break function arguments into local and remote projections first
            ImmutableList.Builder<List<ProjectionContext>> argumentProjections = ImmutableList.builder();

            for (RowExpression argument : arguments) {
                if (local && argument instanceof ConstantExpression) {
                    newArguments.add(argument);
                }
                else {
                    List<ProjectionContext> argumentProjection = argument.accept(this, null);
                    if (argumentProjection.isEmpty()) {
                        VariableReferenceExpression variable = variableAllocator.newVariable(argument);
                        argumentProjection = ImmutableList.of(new ProjectionContext(ImmutableMap.of(variable, argument), false));
                    }
                    argumentProjections.add(argumentProjection);
                    newArguments.add(getAssignedArgument(argumentProjection));
                }
            }
            return mergeProjectionContexts(argumentProjections.build());
        }
    }

    public static class ProjectionContext
    {
        private final Map<VariableReferenceExpression, RowExpression> projections;
        private final boolean remote;

        ProjectionContext(Map<VariableReferenceExpression, RowExpression> projections, boolean remote)
        {
            this.projections = requireNonNull(projections, "projections is null");
            this.remote = remote;
        }

        public Map<VariableReferenceExpression, RowExpression> getProjections()
        {
            return projections;
        }

        public boolean isRemote()
        {
            return remote;
        }
    }
}
