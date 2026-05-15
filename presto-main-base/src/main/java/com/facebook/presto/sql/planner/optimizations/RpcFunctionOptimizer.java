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
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.RPCNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.getRpcDispatchBatchSize;
import static com.facebook.presto.SystemSessionProperties.getRpcStreamingMode;
import static com.facebook.presto.SystemSessionProperties.isRpcFunctionOptimizerEnabled;
import static java.util.Objects.requireNonNull;

public class RpcFunctionOptimizer
        implements PlanOptimizer
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final Supplier<Set<String>> rpcFunctionNamesSupplier;

    public RpcFunctionOptimizer()
    {
        this(ImmutableSet::of);
    }

    public RpcFunctionOptimizer(Supplier<Set<String>> rpcFunctionNamesSupplier)
    {
        this.rpcFunctionNamesSupplier = requireNonNull(rpcFunctionNamesSupplier, "rpcFunctionNamesSupplier is null");
    }

    public boolean isRpcFunction(CallExpression call)
    {
        return rpcFunctionNamesSupplier.get().contains(
                call.getDisplayName().toLowerCase(Locale.ENGLISH));
    }

    @Override
    public PlanOptimizerResult optimize(
            PlanNode plan,
            Session session,
            TypeProvider types,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        if (!isRpcFunctionOptimizerEnabled(session)) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }

        Rewriter rewriter = new Rewriter(session, idAllocator, variableAllocator, rpcFunctionNamesSupplier.get());
        PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, plan, null);
        return PlanOptimizerResult.optimizerResult(rewrittenPlan, rewriter.isPlanChanged());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Session session;
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;
        private final Set<String> rpcFunctionNames;
        private boolean planChanged;

        private Rewriter(Session session, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator, Set<String> rpcFunctionNames)
        {
            this.session = requireNonNull(session, "session is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
            this.rpcFunctionNames = requireNonNull(rpcFunctionNames, "rpcFunctionNames is null");
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource());

            // Rewrite all assignment expressions, extracting RPC calls
            RpcExtractionContext extractionContext = new RpcExtractionContext();
            RowExpressionRewriter<RpcExtractionContext> rpcRewriter = createRpcRewriter();

            Assignments.Builder newAssignments = Assignments.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry
                    : node.getAssignments().getMap().entrySet()) {
                RowExpression rewritten = RowExpressionTreeRewriter.rewriteWith(
                        rpcRewriter, entry.getValue(), extractionContext);
                newAssignments.put(entry.getKey(), rewritten);
            }

            List<ExtractedRpcCall> extractedCalls = extractionContext.getExtractedCalls();
            if (extractedCalls.isEmpty()) {
                if (rewrittenSource != node.getSource()) {
                    return new ProjectNode(
                            node.getSourceLocation(),
                            node.getId(),
                            rewrittenSource,
                            node.getAssignments(),
                            node.getLocality());
                }
                return node;
            }

            planChanged = true;

            // Build RPCNode chain (calls are in bottom-up order from the rewriter).
            // For each extracted RPC call, we:
            // 1. Project non-variable argument expressions into named columns
            //    (a pre-RPCNode ProjectNode).
            // 2. Create an RPCNode that references those columns by name
            //    (argumentColumns), keeping the original argument expressions
            //    (arguments) for the C++ plan converter to extract types and
            //    constants.
            PlanNode currentSource = rewrittenSource;
            for (ExtractedRpcCall extracted : extractedCalls) {
                RPCNode.StreamingMode streamingMode = parseStreamingMode(extracted.originalCall);
                int dispatchBatchSize = parseDispatchBatchSize(extracted.originalCall);

                List<RowExpression> substitutedArgs = extracted.substitutedCall.getArguments();
                ImmutableList.Builder<String> argumentColumns = ImmutableList.builder();
                Assignments.Builder projections = Assignments.builder();
                boolean needsProjection = false;

                // Pass through all source columns in the projection.
                for (VariableReferenceExpression sourceVar : currentSource.getOutputVariables()) {
                    projections.put(sourceVar, sourceVar);
                }

                for (int i = 0; i < substitutedArgs.size(); i++) {
                    RowExpression arg = substitutedArgs.get(i);
                    if (arg instanceof VariableReferenceExpression) {
                        // Argument is already a column reference — use directly.
                        argumentColumns.add(((VariableReferenceExpression) arg).getName());
                    }
                    else {
                        // Argument is an expression or constant — project it
                        // into a new column before the RPCNode.
                        VariableReferenceExpression argVar = variableAllocator.newVariable(
                                "__rpc_arg_",
                                arg.getType());
                        projections.put(argVar, arg);
                        argumentColumns.add(argVar.getName());
                        needsProjection = true;
                    }
                }

                if (needsProjection) {
                    currentSource = new ProjectNode(
                            node.getSourceLocation(),
                            idAllocator.getNextId(),
                            currentSource,
                            projections.build(),
                            ProjectNode.Locality.LOCAL);
                }

                RPCNode rpcNode = new RPCNode(
                        node.getSourceLocation(),
                        idAllocator.getNextId(),
                        currentSource,
                        extracted.originalCall.getDisplayName(),
                        extracted.originalCall.getArguments(),
                        argumentColumns.build(),
                        extracted.resultVar,
                        streamingMode,
                        dispatchBatchSize);

                currentSource = rpcNode;
            }

            return new ProjectNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    currentSource,
                    newAssignments.build(),
                    node.getLocality());
        }

        private RowExpressionRewriter<RpcExtractionContext> createRpcRewriter()
        {
            return new RowExpressionRewriter<RpcExtractionContext>()
            {
                @Override
                public RowExpression rewriteCall(
                        CallExpression node,
                        RpcExtractionContext context,
                        RowExpressionTreeRewriter<RpcExtractionContext> treeRewriter)
                {
                    if (node.getDisplayName().equals("$internal$try")
                            && node.getArguments().size() == 1
                            && isLambdaOrBindWithLambda(node.getArguments().get(0))) {
                        RowExpression result = rewriteTryWithRpcFunction(node, context);
                        if (result != null) {
                            return result;
                        }
                    }

                    if (!rpcFunctionNames.contains(
                            node.getDisplayName().toLowerCase(Locale.ENGLISH))) {
                        return null;
                    }

                    // Rewrite children first (inner RPC calls become result variables).
                    // Record both: originalCall for option parsing (arg[3] constant),
                    // substitutedCall for RPCNode arguments (inner RPCs replaced).
                    CallExpression rewritten = treeRewriter.defaultRewrite(node, context);

                    VariableReferenceExpression resultVar = variableAllocator.newVariable(
                            "__rpc_result_",
                            node.getType());

                    context.addCall(node, rewritten, resultVar);

                    return resultVar;
                }

                @Override
                public RowExpression rewriteLambda(
                        LambdaDefinitionExpression node,
                        RpcExtractionContext context,
                        RowExpressionTreeRewriter<RpcExtractionContext> treeRewriter)
                {
                    // Do not traverse into lambda bodies — RPC calls inside lambdas
                    // have per-element semantics incompatible with RPCNode batching.
                    // TRY lambdas are handled specially in rewriteCall above.
                    return node;
                }
            };
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

        private RowExpression rewriteTryWithRpcFunction(
                CallExpression tryCall,
                RpcExtractionContext context)
        {
            RowExpression tryArgument = tryCall.getArguments().get(0);
            LambdaDefinitionExpression lambda;
            List<RowExpression> bindVariables;

            if (tryArgument instanceof LambdaDefinitionExpression) {
                lambda = (LambdaDefinitionExpression) tryArgument;
                bindVariables = ImmutableList.of();
            }
            else {
                SpecialFormExpression bind = (SpecialFormExpression) tryArgument;
                List<RowExpression> bindArgs = bind.getArguments();
                lambda = (LambdaDefinitionExpression) bindArgs.get(bindArgs.size() - 1);
                bindVariables = bindArgs.subList(0, bindArgs.size() - 1);
            }

            RowExpression body = lambda.getBody();
            if (!bindVariables.isEmpty()) {
                List<String> lambdaParams = lambda.getArguments();
                ImmutableMap.Builder<String, RowExpression> substitutions = ImmutableMap.builder();
                for (int i = 0; i < lambdaParams.size(); i++) {
                    substitutions.put(lambdaParams.get(i), bindVariables.get(i));
                }
                body = VariableSubstitutor.substitute(body, substitutions.build());
            }

            if (!containsRpcFunction(body)) {
                return null;
            }

            RowExpression rewrittenBody = RowExpressionTreeRewriter.rewriteWith(
                    createRpcRewriter(), body, context);

            LambdaDefinitionExpression newLambda = new LambdaDefinitionExpression(
                    lambda.getSourceLocation(),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    rewrittenBody);

            return new CallExpression(
                    tryCall.getSourceLocation(),
                    tryCall.getDisplayName(),
                    tryCall.getFunctionHandle(),
                    tryCall.getType(),
                    ImmutableList.of(newLambda));
        }

        private boolean containsRpcFunction(RowExpression expression)
        {
            if (expression instanceof CallExpression) {
                CallExpression call = (CallExpression) expression;
                if (rpcFunctionNames.contains(call.getDisplayName().toLowerCase(Locale.ENGLISH))) {
                    return true;
                }
                return call.getArguments().stream().anyMatch(this::containsRpcFunction);
            }
            if (expression instanceof SpecialFormExpression) {
                return ((SpecialFormExpression) expression).getArguments().stream()
                        .anyMatch(this::containsRpcFunction);
            }
            return false;
        }

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
                ImmutableList.Builder<RowExpression> newArgs = ImmutableList.builder();
                boolean changed = false;
                for (RowExpression arg : call.getArguments()) {
                    RowExpression newArg = arg.accept(this, null);
                    newArgs.add(newArg);
                    changed |= newArg != arg;
                }
                return changed
                        ? new CallExpression(call.getSourceLocation(), call.getDisplayName(), call.getFunctionHandle(), call.getType(), newArgs.build())
                        : call;
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
                // Filter out substitutions that are shadowed by lambda parameters
                Map<String, RowExpression> filtered = substitutions.entrySet().stream()
                        .filter(e -> !lambda.getArguments().contains(e.getKey()))
                        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
                if (filtered.isEmpty()) {
                    return lambda;
                }
                RowExpression newBody = lambda.getBody().accept(new VariableSubstitutor(filtered), null);
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
                ImmutableList.Builder<RowExpression> newArgs = ImmutableList.builder();
                boolean changed = false;
                for (RowExpression arg : specialForm.getArguments()) {
                    RowExpression newArg = arg.accept(this, null);
                    newArgs.add(newArg);
                    changed |= newArg != arg;
                }
                return changed
                        ? new SpecialFormExpression(specialForm.getSourceLocation(), specialForm.getForm(), specialForm.getType(), newArgs.build())
                        : specialForm;
            }
        }

        private static class RpcExtractionContext
        {
            private final List<ExtractedRpcCall> extractedCalls = new ArrayList<>();

            void addCall(CallExpression originalCall,
                    CallExpression substitutedCall,
                    VariableReferenceExpression resultVar)
            {
                extractedCalls.add(new ExtractedRpcCall(originalCall, substitutedCall, resultVar));
            }

            List<ExtractedRpcCall> getExtractedCalls()
            {
                return extractedCalls;
            }
        }

        private static class ExtractedRpcCall
        {
            final CallExpression originalCall;
            final CallExpression substitutedCall;
            final VariableReferenceExpression resultVar;

            ExtractedRpcCall(CallExpression originalCall,
                    CallExpression substitutedCall,
                    VariableReferenceExpression resultVar)
            {
                this.originalCall = originalCall;
                this.substitutedCall = substitutedCall;
                this.resultVar = resultVar;
            }
        }

        private ConstantExpression extractConstant(RowExpression expression)
        {
            if (expression instanceof ConstantExpression) {
                return (ConstantExpression) expression;
            }
            if (expression instanceof CallExpression) {
                CallExpression call = (CallExpression) expression;
                if (call.getDisplayName().equalsIgnoreCase("CAST") && !call.getArguments().isEmpty()) {
                    return extractConstant(call.getArguments().get(0));
                }
            }
            return null;
        }

        private Optional<JsonNode> parseOptionsJson(CallExpression rpcCall)
        {
            List<RowExpression> args = rpcCall.getArguments();
            if (args.size() < 3) {
                return Optional.empty();
            }
            ConstantExpression optionsArg = extractConstant(args.get(args.size() - 1));
            if (optionsArg == null || optionsArg.getValue() == null) {
                return Optional.empty();
            }
            try {
                String optionsStr;
                if (optionsArg.getValue() instanceof Slice) {
                    optionsStr = ((Slice) optionsArg.getValue()).toStringUtf8();
                }
                else {
                    optionsStr = optionsArg.getValue().toString();
                }
                return Optional.of(OBJECT_MAPPER.readTree(optionsStr));
            }
            catch (IOException e) {
                return Optional.empty();
            }
        }

        private RPCNode.StreamingMode parseStreamingMode(CallExpression rpcCall)
        {
            // Per-function override from constant options JSON takes precedence.
            Optional<RPCNode.StreamingMode> fromJson = parseOptionsJson(rpcCall)
                    .map(json -> json.path("streaming_mode").asText(""))
                    .filter(mode -> mode.equalsIgnoreCase("batch"))
                    .map(mode -> RPCNode.StreamingMode.BATCH);
            if (fromJson.isPresent()) {
                return fromJson.get();
            }
            // Fall back to session property.
            return getRpcStreamingMode(session);
        }

        private int parseDispatchBatchSize(CallExpression rpcCall)
        {
            // Per-function override from constant options JSON takes precedence.
            Optional<Integer> fromJson = parseOptionsJson(rpcCall)
                    .map(json -> json.path("dispatch_batch_size"))
                    .filter(JsonNode::isNumber)
                    .map(JsonNode::asInt);
            if (fromJson.isPresent()) {
                return fromJson.get();
            }
            // Fall back to session property.
            return getRpcDispatchBatchSize(session);
        }
    }
}
