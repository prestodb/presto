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

import com.facebook.presto.common.type.FunctionType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.VariableAllocator;
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
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.ExternalCallExpressionChecker;
import com.facebook.presto.sql.planner.optimizations.SymbolMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isRemoteFunctionsEnabled;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.REMOTE;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.UNKNOWN;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.type.JsonPathType.JSON_PATH;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class PlanRemoteProjections
        implements Rule<ProjectNode>
{
    private static final Pattern<ProjectNode> PATTERN = project();

    private final FunctionAndTypeManager functionAndTypeManager;

    public PlanRemoteProjections(FunctionAndTypeManager functionAndTypeManager)
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
            return Result.ofPlanNode(new ProjectNode(node.getSourceLocation(), node.getId(), node.getSource(), node.getAssignments(), LOCAL));
        }
        if (!isRemoteFunctionsEnabled(context.getSession())) {
            throw new PrestoException(GENERIC_USER_ERROR, "Remote functions are not enabled");
        }
        List<ProjectionContext> projectionContexts = planRemoteAssignments(node.getAssignments(), context.getVariableAllocator());
        checkState(!projectionContexts.isEmpty(), "Expect non-empty projectionContexts");
        PlanNode rewritten = node.getSource();
        for (ProjectionContext projectionContext : projectionContexts) {
            rewritten = new ProjectNode(node.getSourceLocation(), context.getIdAllocator().getNextId(), rewritten, Assignments.builder().putAll(projectionContext.getProjections()).build(), projectionContext.remote ? REMOTE : LOCAL);
        }
        return Result.ofPlanNode(rewritten);
    }

    @VisibleForTesting
    public List<ProjectionContext> planRemoteAssignments(Assignments assignments, VariableAllocator variableAllocator)
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
        ImmutableList.Builder<ProjectionContext> dedupedProjectionContexts = ImmutableList.builder();
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
                dedupedProjectionContexts.add(new ProjectionContext(projections, projectionContexts.get(i).isRemote()));
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
                dedupedProjectionContexts.add(new ProjectionContext(dedupedProjectionsBuilder.build(), projectionContexts.get(i).isRemote()));
                mapper = mapperBuilder.build();
            }
        }
        return dedupedProjectionContexts.build();
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
        private final VariableAllocator variableAllocator;

        public Visitor(FunctionAndTypeManager functionAndTypeManager, VariableAllocator variableAllocator)
        {
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        }

        @Override
        public List<ProjectionContext> visitCall(CallExpression call, Void context)
        {
            FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle());
            boolean local = !functionMetadata.getImplementationType().isExternalExecution();

            // Special handling for $internal$try: extract remote functions from the lambda body.
            // The argument is either a LambdaDefinitionExpression directly or a BIND(captured_vars..., lambda)
            // after lambda capture desugaring.
            if (local && functionMetadata.getName().getObjectName().equals("$internal$try")
                    && call.getArguments().size() == 1
                    && isLambdaOrBindWithLambda(call.getArguments().get(0))) {
                return processInternalTry(call);
            }

            // Break function arguments into local and remote projections first
            ImmutableList.Builder<RowExpression> newArgumentsBuilder = ImmutableList.builder();
            List<ProjectionContext> processedArguments = processArguments(call.getArguments(), newArgumentsBuilder, local);
            List<RowExpression> newArguments = newArgumentsBuilder.build();
            CallExpression newCall = new CallExpression(
                    call.getSourceLocation(),
                    call.getDisplayName(),
                    call.getFunctionHandle(),
                    call.getType(),
                    newArguments);

            if (local) {
                if (processedArguments.isEmpty() || (processedArguments.size() == 1 && !processedArguments.get(0).isRemote())) {
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
                                            call.getSourceLocation(),
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
            if (processedArguments.isEmpty() || (processedArguments.size() == 1 && !processedArguments.get(0).isRemote())) {
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

        private static boolean isLambdaOrBindWithLambda(RowExpression expression)
        {
            if (expression instanceof LambdaDefinitionExpression) {
                return true;
            }
            if (expression instanceof SpecialFormExpression
                    && ((SpecialFormExpression) expression).getForm() == SpecialFormExpression.Form.BIND) {
                List<RowExpression> bindArgs = ((SpecialFormExpression) expression).getArguments();
                return bindArgs.get(bindArgs.size() - 1) instanceof LambdaDefinitionExpression;
            }
            return false;
        }

        private List<ProjectionContext> processInternalTry(CallExpression call)
        {
            RowExpression tryArgument = call.getArguments().get(0);
            LambdaDefinitionExpression lambda;
            List<RowExpression> bindVariables;

            if (tryArgument instanceof LambdaDefinitionExpression) {
                lambda = (LambdaDefinitionExpression) tryArgument;
                bindVariables = ImmutableList.of();
            }
            else {
                // BIND(captured_var1, ..., captured_varN, (param1, ..., paramN) -> body)
                SpecialFormExpression bind = (SpecialFormExpression) tryArgument;
                List<RowExpression> bindArgs = bind.getArguments();
                lambda = (LambdaDefinitionExpression) bindArgs.get(bindArgs.size() - 1);
                bindVariables = bindArgs.subList(0, bindArgs.size() - 1);
            }

            // Substitute lambda parameters with captured variables in the body so that
            // the visitor sees plan-level variables instead of lambda-scoped parameters
            RowExpression body = lambda.getBody();
            if (!bindVariables.isEmpty()) {
                List<String> lambdaParams = lambda.getArguments();
                checkState(bindVariables.size() == lambdaParams.size(),
                        "BIND variable count doesn't match lambda parameter count");
                ImmutableMap.Builder<String, RowExpression> substitutions = ImmutableMap.builder();
                for (int i = 0; i < lambdaParams.size(); i++) {
                    substitutions.put(lambdaParams.get(i), bindVariables.get(i));
                }
                body = VariableSubstitutor.substitute(body, substitutions.build());
            }

            // Visit the substituted body to extract remote function projections
            List<ProjectionContext> bodyProjections = body.accept(this, null);

            if (bodyProjections.isEmpty()) {
                // No remote functions in the lambda body
                return ImmutableList.of();
            }

            ProjectionContext lastBodyProjection = bodyProjections.get(bodyProjections.size() - 1);
            checkState(lastBodyProjection.getProjections().size() == 1,
                    "Expected single projection in last body projection");

            // Get the result expression for the new lambda body
            RowExpression newBody;
            List<ProjectionContext> priorProjections;
            if (!lastBodyProjection.isRemote()) {
                // Fold the local expression into the lambda body to avoid consecutive local projections
                newBody = getOnlyElement(lastBodyProjection.getProjections().values());
                priorProjections = bodyProjections.subList(0, bodyProjections.size() - 1);
            }
            else {
                // Use the result variable as the lambda body
                newBody = getOnlyElement(lastBodyProjection.getProjections().keySet());
                priorProjections = bodyProjections;
            }

            // Build the new $internal$try argument: wrap the result in a lambda with BIND
            // to capture any referenced variables from prior projections
            RowExpression newTryArgument = buildLambdaWithBind(lambda.getSourceLocation(), newBody);

            CallExpression newCall = new CallExpression(
                    call.getSourceLocation(),
                    call.getDisplayName(),
                    call.getFunctionHandle(),
                    call.getType(),
                    ImmutableList.of(newTryArgument));

            ImmutableList.Builder<ProjectionContext> projectionContextBuilder = ImmutableList.builder();
            projectionContextBuilder.addAll(priorProjections);
            projectionContextBuilder.add(new ProjectionContext(
                    ImmutableMap.of(variableAllocator.newVariable(newCall), newCall), false));
            return projectionContextBuilder.build();
        }

        private RowExpression buildLambdaWithBind(Optional<SourceLocation> sourceLocation, RowExpression body)
        {
            List<VariableReferenceExpression> capturedVariables = VariablesExtractor.extractAll(body).stream()
                    .distinct()
                    .collect(toImmutableList());

            if (capturedVariables.isEmpty()) {
                // No captured variables, return a simple lambda
                return new LambdaDefinitionExpression(
                        sourceLocation,
                        ImmutableList.of(),
                        ImmutableList.of(),
                        body);
            }

            // Create lambda parameters for each captured variable and substitute in the body
            ImmutableList.Builder<Type> paramTypes = ImmutableList.builder();
            ImmutableList.Builder<String> paramNames = ImmutableList.builder();
            ImmutableMap.Builder<String, RowExpression> reverseSubstitutions = ImmutableMap.builder();
            ImmutableList.Builder<RowExpression> bindArgs = ImmutableList.builder();

            for (VariableReferenceExpression captured : capturedVariables) {
                VariableReferenceExpression param = variableAllocator.newVariable(captured);
                paramTypes.add(captured.getType());
                paramNames.add(param.getName());
                reverseSubstitutions.put(captured.getName(), param);
                bindArgs.add(captured);
            }

            RowExpression lambdaBody = VariableSubstitutor.substitute(body, reverseSubstitutions.build());
            LambdaDefinitionExpression newLambda = new LambdaDefinitionExpression(
                    sourceLocation,
                    paramTypes.build(),
                    paramNames.build(),
                    lambdaBody);

            bindArgs.add(newLambda);
            // BIND's return type is the bound function type: () -> returnType
            Type bindReturnType = new FunctionType(ImmutableList.of(), body.getType());
            return new SpecialFormExpression(
                    sourceLocation,
                    SpecialFormExpression.Form.BIND,
                    bindReturnType,
                    bindArgs.build());
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
                        if (local && argument.getType().equals(JSON_PATH)) {
                            // JsonPath type is not serializable and cannot be an output of a ProjectNode.
                            // Keep it inline when the parent function is local.
                            newArguments.add(argument);
                            continue;
                        }
                        // WHEN clauses are structural parts of SWITCH expressions and cannot be
                        // standalone variables. Process their sub-arguments to ensure referenced
                        // variables are tracked through projection layers, then reconstruct the WHEN.
                        if (local && argument instanceof SpecialFormExpression
                                && ((SpecialFormExpression) argument).getForm() == SpecialFormExpression.Form.WHEN) {
                            SpecialFormExpression when = (SpecialFormExpression) argument;
                            ImmutableList.Builder<RowExpression> whenSubArgs = ImmutableList.builder();
                            List<ProjectionContext> whenSubProjections = processArguments(when.getArguments(), whenSubArgs, true);
                            newArguments.add(new SpecialFormExpression(
                                    when.getForm(), when.getType(), whenSubArgs.build()));
                            if (!whenSubProjections.isEmpty()) {
                                argumentProjections.add(whenSubProjections);
                            }
                            continue;
                        }
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

    /**
     * Substitutes {@link VariableReferenceExpression} instances in a {@link RowExpression} tree
     * based on a name-to-expression mapping.
     */
    private static class VariableSubstitutor
            implements RowExpressionVisitor<RowExpression, Void>
    {
        private final Map<String, RowExpression> substitutions;

        VariableSubstitutor(Map<String, RowExpression> substitutions)
        {
            this.substitutions = substitutions;
        }

        static RowExpression substitute(RowExpression expression, Map<String, RowExpression> substitutions)
        {
            return expression.accept(new VariableSubstitutor(substitutions), null);
        }

        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            List<RowExpression> newArguments = call.getArguments().stream()
                    .map(arg -> arg.accept(this, null))
                    .collect(toImmutableList());
            if (newArguments.equals(call.getArguments())) {
                return call;
            }
            return new CallExpression(
                    call.getSourceLocation(),
                    call.getDisplayName(),
                    call.getFunctionHandle(),
                    call.getType(),
                    newArguments);
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Void context)
        {
            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            RowExpression newBody = lambda.getBody().accept(this, null);
            if (newBody.equals(lambda.getBody())) {
                return lambda;
            }
            return new LambdaDefinitionExpression(
                    lambda.getSourceLocation(),
                    lambda.getArgumentTypes(),
                    lambda.getArguments(),
                    newBody);
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            RowExpression replacement = substitutions.get(reference.getName());
            return replacement != null ? replacement : reference;
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            List<RowExpression> newArguments = specialForm.getArguments().stream()
                    .map(arg -> arg.accept(this, null))
                    .collect(toImmutableList());
            if (newArguments.equals(specialForm.getArguments())) {
                return specialForm;
            }
            return new SpecialFormExpression(
                    specialForm.getSourceLocation(),
                    specialForm.getForm(),
                    specialForm.getType(),
                    newArguments);
        }
    }
}
