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
package com.facebook.presto.plugin.clp.optimization;

import com.facebook.presto.plugin.clp.ClpColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Utility for rewriting CLP UDFs (e.g., <code>CLP_GET_*</code>) in {@link RowExpression} trees.
 * <p>
 * Traverses a query plan and rewrites calls to <code>CLP_GET_*</code> UDFs into
 * {@link VariableReferenceExpression}s with meaningful names derived from their arguments.
 * <p>
 * This enables querying fields that are not part of the original table schema but are available
 * in CLP.
 */
public final class ClpUdfRewriter
        implements ConnectorPlanOptimizer
{
    public static final String JSON_STRING_PLACEHOLDER = "__json_string";
    private final FunctionMetadataManager functionManager;

    public ClpUdfRewriter(FunctionMetadataManager functionManager)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator allocator, PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new Rewriter(idAllocator, allocator, collectExistingScanAssignments(maxSubplan)), maxSubplan);
    }

    /**
     * Collects all existing variable assignments from {@link TableScanNode} instances that map
     * {@link ColumnHandle}s to {@link VariableReferenceExpression}s.
     * <p>
     * This method traverses the given plan subtree, visiting the {@link TableScanNode} and
     * extracting its assignments. The resulting map allows tracking which scan-level variables
     * already exist for specific columns, so that subsequent optimizer passes (e.g., CLP UDF
     * rewriting) can reuse them instead of creating duplicates.
     * <p>
     * The key of the returned map is the {@link ColumnHandle}, and the value is the
     * {@link VariableReferenceExpression} assigned to it in the scan node.
     *
     * @param root the root {@link PlanNode} of the plan subtree
     * @return a map from column handle to its correh hsponding scan-level variable
     */
    private Map<ColumnHandle, VariableReferenceExpression> collectExistingScanAssignments(PlanNode root)
    {
        Map<ColumnHandle, VariableReferenceExpression> map = new HashMap<>();
        root.getSources().forEach(source -> map.putAll(collectExistingScanAssignments(source)));
        if (root instanceof TableScanNode) {
            TableScanNode scan = (TableScanNode) root;
            scan.getAssignments().forEach((var, handle) -> {
                map.putIfAbsent(handle, var);
            });
        }
        return map;
    }

    private class Rewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;
        private final Map<ColumnHandle, VariableReferenceExpression> globalColumnVarMap;

        public Rewriter(
                PlanNodeIdAllocator idAllocator,
                VariableAllocator variableAllocator,
                Map<ColumnHandle, VariableReferenceExpression> globalColumnVarMap)
        {
            this.idAllocator = idAllocator;
            this.variableAllocator = variableAllocator;
            this.globalColumnVarMap = globalColumnVarMap;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            Assignments.Builder newAssignments = Assignments.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().getMap().entrySet()) {
                newAssignments.put(
                        entry.getKey(),
                        rewriteClpUdfs(entry.getValue(), functionManager, variableAllocator, true));
            }

            PlanNode newSource = rewritePlanSubtree(node.getSource());
            return new ProjectNode(node.getSourceLocation(), idAllocator.getNextId(), newSource, newAssignments.build(), node.getLocality());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            return buildNewFilterNode(node);
        }

        /**
         * Rewrites <code>CLP_GET_*</code> UDFs in a {@link RowExpression}, collecting each
         * resulting variable into the given map along with its associated {@link ColumnHandle}.
         * <p>
         * Each <code>CLP_GET_*</code> UDF must take a single constant string argument, which is
         * used to construct the name of the variable reference (e.g.
         * <code>CLP_GET_STRING('foo')</code> becomes a variable name <code>foo</code>). Invalid
         * usages (e.g., non-constant arguments) will throw a {@link PrestoException}.
         *
         * @param expression the input expression to analyze and possibly rewrite
         * @param functionManager function manager used to resolve function metadata
         * @param variableAllocator variable allocator used to create new variable references
         * @param inProjectNode whether the CLP UDFs are in a {@link ProjectNode}
         * @return a possibly rewritten {@link RowExpression} with <code>CLP_GET_*</code> calls
         * replaced
         */
        private RowExpression rewriteClpUdfs(
                RowExpression expression,
                FunctionMetadataManager functionManager,
                VariableAllocator variableAllocator,
                boolean inProjectNode)
        {
            // Handle CLP_GET_* function calls
            if (expression instanceof CallExpression) {
                CallExpression call = (CallExpression) expression;
                String functionName = functionManager.getFunctionMetadata(call.getFunctionHandle()).getName().getObjectName().toUpperCase();

                if (inProjectNode && functionName.equals("CLP_GET_JSON_STRING")) {
                    VariableReferenceExpression newValue = variableAllocator.newVariable(
                            expression.getSourceLocation(),
                            JSON_STRING_PLACEHOLDER,
                            call.getType());
                    ClpColumnHandle targetHandle = new ClpColumnHandle(JSON_STRING_PLACEHOLDER, call.getType());

                    globalColumnVarMap.put(targetHandle, newValue);
                    return newValue;
                }
                else if (functionName.startsWith("CLP_GET_")) {
                    if (call.getArguments().size() != 1 || !(call.getArguments().get(0) instanceof ConstantExpression)) {
                        throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                                "CLP_GET_* UDF must have a single constant string argument");
                    }

                    ConstantExpression constant = (ConstantExpression) call.getArguments().get(0);
                    String jsonPath = ((Slice) constant.getValue()).toStringUtf8();
                    ClpColumnHandle targetHandle = new ClpColumnHandle(jsonPath, call.getType());

                    // Check if a variable with the same ClpColumnHandle already exists
                    VariableReferenceExpression existingVar = globalColumnVarMap.get(targetHandle);
                    if (existingVar != null) {
                        return existingVar;
                    }

                    VariableReferenceExpression newVar = variableAllocator.newVariable(
                            expression.getSourceLocation(),
                            encodeJsonPath(jsonPath),
                            call.getType());
                    globalColumnVarMap.put(targetHandle, newVar);
                    return newVar;
                }

                // Recurse into arguments
                List<RowExpression> rewrittenArgs = call.getArguments().stream()
                        .map(arg -> rewriteClpUdfs(arg, functionManager, variableAllocator, inProjectNode))
                        .collect(toImmutableList());

                return new CallExpression(call.getDisplayName(), call.getFunctionHandle(), call.getType(), rewrittenArgs);
            }

            // Handle special forms (e.g., AND, OR, etc.)
            if (expression instanceof SpecialFormExpression) {
                SpecialFormExpression special = (SpecialFormExpression) expression;

                List<RowExpression> rewrittenArgs = special.getArguments().stream()
                        .map(arg -> rewriteClpUdfs(arg, functionManager, variableAllocator, inProjectNode))
                        .collect(toImmutableList());

                return new SpecialFormExpression(special.getSourceLocation(), special.getForm(), special.getType(), rewrittenArgs);
            }

            return expression;
        }

        /**
         * Recursively rewrites the subtree of a plan node to include any new variables produced by
         * CLP UDF rewrites.
         *
         * @param node the plan node to rewrite
         * @return the rewritten plan node
         */
        private PlanNode rewritePlanSubtree(PlanNode node)
        {
            if (node instanceof TableScanNode) {
                return buildNewTableScanNode((TableScanNode) node);
            }
            else if (node instanceof FilterNode) {
                return buildNewFilterNode((FilterNode) node);
            }

            List<PlanNode> rewrittenChildren = node.getSources().stream()
                    .map(source -> rewritePlanSubtree(source))
                    .collect(toImmutableList());

            return node.replaceChildren(rewrittenChildren);
        }

        /**
         * Encodes a JSON path into a valid variable name by replacing uppercase letters with
         * "_ux<lowercase letter>", dots with "_dot_", and underscores with "_und_".
         * <p>
         * This is only used internally to ensure that the variable names generated from JSON paths
         * are valid and do not conflict with other variable names in the expression.
         *
         * @param jsonPath the JSON path to encode
         * @return the encoded variable name
         */
        private String encodeJsonPath(String jsonPath)
        {
            StringBuilder sb = new StringBuilder();
            for (char c : jsonPath.toCharArray()) {
                if (Character.isUpperCase(c)) {
                    sb.append("_ux").append(Character.toLowerCase(c));
                }
                else if (c == '.') {
                    sb.append("_dot_");
                }
                else if (c == '_') {
                    sb.append("_und_");
                }
                else {
                    sb.append(c);
                }
            }
            return sb.toString();
        }

        /**
         * Builds a new {@link TableScanNode} that includes additional
         * {@link VariableReferenceExpression}s and {@link ColumnHandle}s for rewritten CLP UDFs.
         *
         * @param node the original table scan node
         * @return the updated table scan node
         */
        private TableScanNode buildNewTableScanNode(TableScanNode node)
        {
            Set<VariableReferenceExpression> outputVars = new LinkedHashSet<>(node.getOutputVariables());
            Map<VariableReferenceExpression, ColumnHandle> newAssignments = new HashMap<>(node.getAssignments());

            // Add any missing variables for known handles
            globalColumnVarMap.forEach((handle, var) -> {
                outputVars.add(var);
                newAssignments.put(var, handle);
            });

            return new TableScanNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    node.getTable(),
                    new ArrayList<>(outputVars),
                    newAssignments,
                    node.getTableConstraints(),
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint(),
                    node.getCteMaterializationInfo());
        }

        /**
         * Builds a new {@link FilterNode} with its predicate rewritten to replace CLP UDF calls.
         *
         * @param node the original filter node
         * @return the updated filter node
         */
        private FilterNode buildNewFilterNode(FilterNode node)
        {
            RowExpression newPredicate = rewriteClpUdfs(node.getPredicate(), functionManager, variableAllocator, false);
            PlanNode newSource = rewritePlanSubtree(node.getSource());
            return new FilterNode(node.getSourceLocation(), idAllocator.getNextId(), newSource, newPredicate);
        }
    }
}
