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
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.isRpcFunctionOptimizerEnabled;
import static java.util.Objects.requireNonNull;

/**
 * Plan optimizer that rewrites RPC function calls to use the single-operator
 * RPCNode for async execution.
 *
 * This optimizer detects ProjectNodes containing RPC function calls — including
 * calls nested inside wrapper expressions like {@code UPPER(rpc(...))},
 * {@code COALESCE(rpc(...), 'default')}, or {@code rpc1(rpc2(x))} — and
 * rewrites them by extracting each RPC call into its own RPCNode.
 *
 * <p>Uses {@link RowExpressionTreeRewriter} to traverse expression trees
 * bottom-up, following the same pattern as {@code FbAIFunctionRewriter}.
 *
 * Before:
 *   ProjectNode(output = UPPER(rpc_function(arg1, arg2)))
 *
 * After:
 *   Source -> RPCNode -> ProjectNode(output = UPPER(__rpc_result_0))
 *
 * RPC functions are detected by function name: any function whose name is in
 * the known set of RPC function names (e.g., fb_llm_inference) is treated as
 * an RPC function. Functions are registered in C++ via the
 * AsyncRPCFunctionRegistry and discovered by the sidecar at startup.
 *
 * To add a new RPC function:
 * 1. Register the AsyncRPCFunction with signatures in the C++ worker
 * 2. The function name will be automatically discovered via the sidecar
 */
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

    /**
     * Check if a CallExpression is an RPC function based on its name.
     */
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

        Rewriter rewriter = new Rewriter(idAllocator, variableAllocator, rpcFunctionNamesSupplier.get());
        PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, plan, null);
        return PlanOptimizerResult.optimizerResult(rewrittenPlan, rewriter.isPlanChanged());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;
        private final Set<String> rpcFunctionNames;
        private boolean planChanged;

        private Rewriter(PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator, Set<String> rpcFunctionNames)
        {
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

            ImmutableMap.Builder<VariableReferenceExpression, RowExpression> newAssignments =
                    ImmutableMap.builder();
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
                ImmutableMap.Builder<VariableReferenceExpression, RowExpression> projections =
                        ImmutableMap.builder();
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
                            new Assignments(projections.build()),
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
                    new Assignments(newAssignments.build()),
                    node.getLocality());
        }

        /**
         * Creates a {@link RowExpressionRewriter} that extracts RPC function calls.
         *
         * <p>When an RPC call is found, its children are rewritten first via
         * {@code defaultRewrite} (handling nested RPC calls bottom-up), then the
         * call is recorded and replaced with a result variable.
         *
         * <p>Non-RPC calls return null, triggering the default recursive rewrite.
         * Lambda expressions are returned unchanged (RPC calls inside lambdas
         * have per-element semantics incompatible with RPCNode batching).
         */
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
                    if (!rpcFunctionNames.contains(
                            node.getDisplayName().toLowerCase(Locale.ENGLISH))) {
                        return null;
                    }

                    // Rewrite children first (inner RPC calls become variables)
                    CallExpression rewritten = treeRewriter.defaultRewrite(node, context);

                    VariableReferenceExpression resultVar = variableAllocator.newVariable(
                            "__rpc_result_",
                            node.getType());

                    // Record: originalCall for option parsing, substitutedCall for RPCNode args
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
                    // have per-element semantics incompatible with RPCNode batching
                    return node;
                }
            };
        }

        /**
         * Collects RPC calls found during expression rewriting.
         */
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

        /**
         * Pairs an original RPC call (for option parsing from arg[3]) with its
         * substituted form (with inner RPC calls replaced by result variables,
         * providing the correct arguments for the RPCNode).
         */
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

        /**
         * Extracts a ConstantExpression from a RowExpression, handling Cast wrappers.
         */
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

        /**
         * Parses the options JSON from arg[3] into a JsonNode, or returns empty if unavailable.
         */
        private Optional<JsonNode> parseOptionsJson(CallExpression rpcCall)
        {
            List<RowExpression> args = rpcCall.getArguments();
            if (args.size() < 4) {
                return Optional.empty();
            }
            ConstantExpression optionsArg = extractConstant(args.get(3));
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
            catch (Exception e) {
                return Optional.empty();
            }
        }

        /**
         * Parses the streaming mode from the options JSON argument (arg[3]).
         */
        private RPCNode.StreamingMode parseStreamingMode(CallExpression rpcCall)
        {
            return parseOptionsJson(rpcCall)
                    .map(json -> json.path("streaming_mode").asText(""))
                    .filter(mode -> mode.equalsIgnoreCase("batch"))
                    .map(mode -> RPCNode.StreamingMode.BATCH)
                    .orElse(RPCNode.StreamingMode.PER_ROW);
        }

        /**
         * Parses the dispatch batch size from the options JSON argument (arg[3]).
         */
        private int parseDispatchBatchSize(CallExpression rpcCall)
        {
            return parseOptionsJson(rpcCall)
                    .map(json -> json.path("dispatch_batch_size"))
                    .filter(JsonNode::isNumber)
                    .map(JsonNode::asInt)
                    .orElse(0);
        }
    }
}
