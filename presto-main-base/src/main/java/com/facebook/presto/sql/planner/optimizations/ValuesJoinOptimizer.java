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
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isOptimizeValuesJoin;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.PlannerUtils.addProjections;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.callOperator;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/*
 * Optimizes joins with small-sized VALUES nodes for INNER and LEFT OUTER joins
 * (when only equi-conditions are present, no additional filters).
 *
 * For single row VALUES:
 *   INNER JOIN:
 *     - Inlines constants and filters rows where join condition matches
 *   LEFT JOIN:
 *     - Inlines constants for matching rows, NULLs for non-matching rows
 *     - Uses IF(condition, value, NULL) for each column
 *
 * For multiple row VALUES (without duplicate join keys):
 *   - Builds a MAP from join key -> ROW(all columns)
 *   - INNER JOIN: Filters with contains_key, then extracts via element_at
 *   - LEFT JOIN: Extracts via element_at (returns NULL for non-matching keys)
 *
 * Note: If the VALUES node contains duplicate join keys, the optimization is skipped
 * and the plan is returned unchanged to ensure correctness.
 */
public class ValuesJoinOptimizer
        implements PlanOptimizer
{
    private static final int MAX_VALUES_ROW_COUNT = 1000;

    private final Metadata metadata;

    public ValuesJoinOptimizer(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(warningCollector, "warningCollector is null");
        if (isOptimizeValuesJoin(session)) {
            Rewriter rewriter = new Rewriter(session, metadata, idAllocator, variableAllocator, metadata.getFunctionAndTypeManager());
            PlanNode rewritten = SimplePlanRewriter.rewriteWith(rewriter, plan, null);
            return PlanOptimizerResult.optimizerResult(rewritten, rewriter.isPlanChanged());
        }

        return PlanOptimizerResult.optimizerResult(plan, false);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Session session;
        private final Metadata metadata;
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;
        private final FunctionAndTypeManager functionAndTypeManager;
        private boolean planChanged;

        private Rewriter(Session session, Metadata metadata, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator, FunctionAndTypeManager functionAndTypeManager)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode left = node.getLeft();
            PlanNode right = node.getRight();

            PlanNode rewrittenLeft = rewriteWith(this, left);
            PlanNode rewrittenRight = rewriteWith(this, right);

            // Only optimize if right side is a simple ValuesNode (no projection/filter on top)
            if ((node.getType() == INNER || (node.getType() == LEFT && !node.getFilter().isPresent())) &&
                    rewrittenRight instanceof ValuesNode &&
                    !node.getCriteria().isEmpty()) {
                ValuesNode valuesNode = (ValuesNode) rewrittenRight;

                // Check if ValuesNode is within size limit
                if (valuesNode.getRows().size() > MAX_VALUES_ROW_COUNT) {
                    if (rewrittenLeft != node.getLeft() || rewrittenRight != node.getRight()) {
                        planChanged = true;
                        return replaceChildren(node, ImmutableList.of(rewrittenLeft, rewrittenRight));
                    }
                    return node;
                }

                PlanNode newProjection;

                if (valuesNode.getRows().size() == 1) {
                    // Single row: inline the constants with proper join semantics
                    newProjection = buildSingleRowJoin(node, rewrittenLeft, valuesNode);
                    if (newProjection == null) {
                        // Optimization not possible, return with children replaced if needed
                        if (rewrittenLeft != node.getLeft() || rewrittenRight != node.getRight()) {
                            planChanged = true;
                            return replaceChildren(node, ImmutableList.of(rewrittenLeft, rewrittenRight));
                        }
                        return node;
                    }
                }
                else {
                    // Skip optimization if there are duplicate keys
                    if (hasDuplicateKeys(valuesNode, node.getCriteria())) {
                        if (rewrittenLeft != node.getLeft() || rewrittenRight != node.getRight()) {
                            planChanged = true;
                            return replaceChildren(node, ImmutableList.of(rewrittenLeft, rewrittenRight));
                        }
                        return node;
                    }

                    // Multiple rows: create a map for efficient lookup
                    newProjection = buildMapBasedJoin(node, rewrittenLeft, valuesNode);
                    if (newProjection == null) {
                        // Optimization not possible, return with children replaced if needed
                        if (rewrittenLeft != node.getLeft() || rewrittenRight != node.getRight()) {
                            planChanged = true;
                            return replaceChildren(node, ImmutableList.of(rewrittenLeft, rewrittenRight));
                        }
                        return node;
                    }
                }

                planChanged = true;
                return newProjection;
            }

            if (rewrittenLeft != node.getLeft() || rewrittenRight != node.getRight()) {
                planChanged = true;
                return replaceChildren(node, ImmutableList.of(rewrittenLeft, rewrittenRight));
            }

            return node;
        }

        private PlanNode buildSingleRowJoin(JoinNode node, PlanNode leftSide, ValuesNode valuesNode)
        {
            List<RowExpression> singleRow = valuesNode.getRows().get(0);
            List<VariableReferenceExpression> valuesOutputVariables = valuesNode.getOutputVariables();

            // Build index mapping from variable name to its position in ValuesNode
            Map<String, Integer> nameToIndex = new java.util.HashMap<>();
            for (int i = 0; i < valuesOutputVariables.size(); i++) {
                nameToIndex.put(valuesOutputVariables.get(i).getName(), i);
            }

            // Get the constant value for a given variable name
            java.util.function.Function<String, RowExpression> getConstant = name -> {
                Integer index = nameToIndex.get(name);
                return index != null ? singleRow.get(index) : null;
            };

            if (node.getType() == INNER) {
                // INNER JOIN: filter on join condition using left side variables and constant values
                // We compare left variable = constant (not left variable = right variable)
                RowExpression filter = null;
                for (EquiJoinClause clause : node.getCriteria()) {
                    RowExpression rightConstant = getConstant.apply(clause.getRight().getName());
                    if (rightConstant == null) {
                        // Couldn't find constant, skip optimization
                        return null;
                    }
                    RowExpression equality = callOperator(
                            functionAndTypeManager.getFunctionAndTypeResolver(),
                            OperatorType.EQUAL,
                            BOOLEAN,
                            clause.getLeft(),
                            rightConstant);
                    if (filter == null) {
                        filter = equality;
                    }
                    else {
                        filter = specialForm(AND, BOOLEAN, filter, equality);
                    }
                }

                // First filter based on join condition, then project the VALUES columns
                PlanNode filterNode = new FilterNode(
                        leftSide.getSourceLocation(),
                        idAllocator.getNextId(),
                        leftSide,
                        filter);

                // Now add the VALUES columns as constant projections using NEW variables
                // (not the ValuesNode's output variables, to avoid identity issues)
                ImmutableList.Builder<VariableReferenceExpression> newVariables = ImmutableList.builder();
                for (VariableReferenceExpression var : valuesOutputVariables) {
                    newVariables.add(variableAllocator.newVariable(var.getName(), var.getType()));
                }

                return addProjections(
                        filterNode,
                        idAllocator,
                        variableAllocator,
                        singleRow,
                        newVariables.build());
            }
            else {
                // LEFT JOIN: use IF(condition, value, NULL) for each column
                // Build join condition using left variables and constants
                RowExpression joinCondition = null;
                for (EquiJoinClause clause : node.getCriteria()) {
                    RowExpression rightConstant = getConstant.apply(clause.getRight().getName());
                    if (rightConstant == null) {
                        // Couldn't find constant, skip optimization
                        return null;
                    }
                    RowExpression equality = callOperator(
                            functionAndTypeManager.getFunctionAndTypeResolver(),
                            OperatorType.EQUAL,
                            BOOLEAN,
                            clause.getLeft(),
                            rightConstant);
                    if (joinCondition == null) {
                        joinCondition = equality;
                    }
                    else {
                        joinCondition = specialForm(AND, BOOLEAN, joinCondition, equality);
                    }
                }

                // Build conditional assignments: IF(condition, constant, NULL)
                List<RowExpression> conditionalValues = new java.util.ArrayList<>();
                for (int i = 0; i < valuesOutputVariables.size(); i++) {
                    RowExpression value = singleRow.get(i);
                    conditionalValues.add(specialForm(IF, value.getType(), joinCondition, value, constantNull(value.getType())));
                }

                // Use NEW variables (not the ValuesNode's output variables)
                ImmutableList.Builder<VariableReferenceExpression> newVariables = ImmutableList.builder();
                for (VariableReferenceExpression var : valuesOutputVariables) {
                    newVariables.add(variableAllocator.newVariable(var.getName(), var.getType()));
                }

                return addProjections(
                        leftSide,
                        idAllocator,
                        variableAllocator,
                        conditionalValues,
                        newVariables.build());
            }
        }

        private RowExpression buildEquiJoinFilterWithConstants(
                List<EquiJoinClause> criteria,
                List<VariableReferenceExpression> rightVariables,
                List<RowExpression> rightValues)
        {
            RowExpression filter = null;
            for (EquiJoinClause clause : criteria) {
                // Find the index of the right variable by name
                int rightIndex = -1;
                String targetName = clause.getRight().getName();
                for (int i = 0; i < rightVariables.size(); i++) {
                    if (rightVariables.get(i).getName().equals(targetName)) {
                        rightIndex = i;
                        break;
                    }
                }

                // Use the constant value if found, otherwise use the original variable
                RowExpression rightExpr = (rightIndex >= 0) ? rightValues.get(rightIndex) : clause.getRight();

                RowExpression equality = callOperator(
                        functionAndTypeManager.getFunctionAndTypeResolver(),
                        OperatorType.EQUAL,
                        BOOLEAN,
                        clause.getLeft(),
                        rightExpr);
                if (filter == null) {
                    filter = equality;
                }
                else {
                    filter = specialForm(AND, BOOLEAN, filter, equality);
                }
            }
            return filter;
        }

        private PlanNode buildMapBasedJoin(JoinNode node, PlanNode leftSide, ValuesNode valuesNode)
        {
            FunctionResolution functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());

            List<VariableReferenceExpression> outputVariables = valuesNode.getOutputVariables();
            List<EquiJoinClause> criteria = node.getCriteria();

            // Find key column indices for all join criteria
            List<Integer> keyColumnIndices = criteria.stream()
                    .map(clause -> findVariableIndex(outputVariables, clause.getRight()))
                    .collect(Collectors.toList());

            // Validate all indices were found
            if (keyColumnIndices.stream().anyMatch(idx -> idx < 0)) {
                return null; // Skip optimization if we can't find all key columns
            }

            // Build key type - use ROW for multiple keys, simple type for single key
            Type keyType;
            if (criteria.size() == 1) {
                keyType = outputVariables.get(keyColumnIndices.get(0)).getType();
            }
            else {
                List<RowType.Field> keyFields = criteria.stream()
                        .map(clause -> RowType.field(clause.getRight().getName(), clause.getRight().getType()))
                        .collect(toImmutableList());
                keyType = RowType.from(keyFields);
            }

            // Build the value RowType (all columns from ValuesNode)
            List<RowType.Field> valueFields = outputVariables.stream()
                    .map(var -> RowType.field(var.getName(), var.getType()))
                    .collect(toImmutableList());
            RowType valueRowType = RowType.from(valueFields);

            // Build keys and values lists, filtering out rows with any NULL key
            ImmutableList.Builder<RowExpression> keys = ImmutableList.builder();
            ImmutableList.Builder<RowExpression> values = ImmutableList.builder();

            for (List<RowExpression> row : valuesNode.getRows()) {
                // Check if any key column is NULL
                boolean hasNullKey = keyColumnIndices.stream()
                        .map(row::get)
                        .anyMatch(this::isNullConstant);

                // Skip rows with NULL keys - they can never match in a join
                if (hasNullKey) {
                    continue;
                }

                // Build the key expression
                RowExpression key;
                if (criteria.size() == 1) {
                    key = row.get(keyColumnIndices.get(0));
                }
                else {
                    // Build composite key as ROW
                    List<RowExpression> keyValues = keyColumnIndices.stream()
                            .map(row::get)
                            .collect(toImmutableList());
                    key = specialForm(ROW_CONSTRUCTOR, keyType, keyValues);
                }

                // Value: entire row as ROW
                RowExpression rowValue = specialForm(ROW_CONSTRUCTOR, valueRowType, row);

                keys.add(key);
                values.add(rowValue);
            }

            // Build key and value arrays
            List<RowExpression> keysList = keys.build();
            List<RowExpression> valuesList = values.build();

            // If all rows had NULL keys, the map is empty
            // For INNER JOIN: no matches possible, return empty result via false filter
            // For LEFT JOIN: all rows get NULLs, just project NULLs
            if (keysList.isEmpty()) {
                if (node.getType() == INNER) {
                    // Filter that's always false - no rows will match
                    RowExpression falseFilter = constant(false, BOOLEAN);
                    PlanNode filterNode = new FilterNode(
                            leftSide.getSourceLocation(),
                            idAllocator.getNextId(),
                            leftSide,
                            falseFilter);

                    // Still need to project NULL values for the VALUES columns
                    ImmutableList.Builder<RowExpression> nullValues = ImmutableList.builder();
                    for (VariableReferenceExpression var : outputVariables) {
                        nullValues.add(constantNull(var.getType()));
                    }

                    return addProjections(
                            filterNode,
                            idAllocator,
                            variableAllocator,
                            nullValues.build(),
                            outputVariables);
                }
                else {
                    // LEFT JOIN: all left rows get NULLs for VALUES columns
                    ImmutableList.Builder<RowExpression> nullValues = ImmutableList.builder();
                    for (VariableReferenceExpression var : outputVariables) {
                        nullValues.add(constantNull(var.getType()));
                    }

                    return addProjections(
                            leftSide,
                            idAllocator,
                            variableAllocator,
                            nullValues.build(),
                            outputVariables);
                }
            }

            RowExpression keyArray = call(
                    "ARRAY",
                    functionResolution.arrayConstructor(keysList.stream().map(RowExpression::getType).collect(toImmutableList())),
                    new ArrayType(keyType),
                    keysList);

            RowExpression valueArray = call(
                    "ARRAY",
                    functionResolution.arrayConstructor(valuesList.stream().map(RowExpression::getType).collect(toImmutableList())),
                    new ArrayType(valueRowType),
                    valuesList);

            // Build the map using MAP(keys, values) function
            MethodHandle keyEquals = functionAndTypeManager.getJavaScalarFunctionImplementation(
                    functionAndTypeManager.resolveOperator(OperatorType.EQUAL, fromTypes(keyType, keyType))).getMethodHandle();
            MethodHandle keyHashcode = functionAndTypeManager.getJavaScalarFunctionImplementation(
                    functionAndTypeManager.resolveOperator(OperatorType.HASH_CODE, fromTypes(keyType))).getMethodHandle();

            MapType mapType = new MapType(keyType, valueRowType, keyEquals, keyHashcode);
            RowExpression mapConstructor = call(functionAndTypeManager, "MAP", mapType, keyArray, valueArray);

            // Create a variable for the map and project it onto the left side
            VariableReferenceExpression mapVariable = variableAllocator.newVariable("valuesMap", mapType);
            PlanNode result = addProjections(
                    leftSide,
                    idAllocator,
                    variableAllocator,
                    ImmutableList.of(mapConstructor),
                    ImmutableList.of(mapVariable));

            // Build the lookup key from left side of join criteria
            RowExpression lookupKey;
            if (criteria.size() == 1) {
                lookupKey = criteria.get(0).getLeft();
            }
            else {
                // Build composite lookup key as ROW
                List<RowExpression> leftKeyExprs = criteria.stream()
                        .map(EquiJoinClause::getLeft)
                        .collect(toImmutableList());
                lookupKey = specialForm(ROW_CONSTRUCTOR, keyType, leftKeyExprs);
            }

            // For INNER JOIN: filter out rows where key doesn't exist in map
            if (node.getType() == INNER) {
                RowExpression containsKeyFilter = call(
                        functionAndTypeManager,
                        "contains_key",
                        BOOLEAN,
                        mapVariable,
                        lookupKey);

                result = new FilterNode(
                        result.getSourceLocation(),
                        idAllocator.getNextId(),
                        result,
                        containsKeyFilter);
            }

            // element_at(mapVariable, lookupKey) returns the matching row (or NULL for LEFT JOIN)
            RowExpression elementAt = call(
                    functionAndTypeManager,
                    "element_at",
                    valueRowType,
                    mapVariable,
                    lookupKey);

            // Extract each field from the row and assign to output variables
            ImmutableList.Builder<RowExpression> fieldExtractions = ImmutableList.builder();
            for (int i = 0; i < outputVariables.size(); i++) {
                RowExpression fieldAccess = specialForm(
                        DEREFERENCE,
                        outputVariables.get(i).getType(),
                        elementAt,
                        new ConstantExpression((long) i, INTEGER));
                fieldExtractions.add(fieldAccess);
            }

            return addProjections(
                    result,
                    idAllocator,
                    variableAllocator,
                    fieldExtractions.build(),
                    outputVariables);
        }

        private boolean isNullConstant(RowExpression expression)
        {
            return expression instanceof ConstantExpression && ((ConstantExpression) expression).isNull();
        }

        private RowExpression buildEquiJoinFilter(List<EquiJoinClause> criteria, Map<String, VariableReferenceExpression> nameToRightVar)
        {
            RowExpression filter = null;
            for (EquiJoinClause clause : criteria) {
                // Find the matching variable by name
                VariableReferenceExpression rightVar = nameToRightVar.getOrDefault(
                        clause.getRight().getName(),
                        clause.getRight());

                RowExpression equality = callOperator(
                        functionAndTypeManager.getFunctionAndTypeResolver(),
                        OperatorType.EQUAL,
                        BOOLEAN,
                        clause.getLeft(),
                        rightVar);
                if (filter == null) {
                    filter = equality;
                }
                else {
                    filter = specialForm(AND, BOOLEAN, filter, equality);
                }
            }
            return filter;
        }

        private int findVariableIndex(List<VariableReferenceExpression> variables, VariableReferenceExpression target)
        {
            for (int i = 0; i < variables.size(); i++) {
                if (variables.get(i).getName().equals(target.getName())) {
                    return i;
                }
            }
            return -1;
        }

        private boolean hasDuplicateKeys(ValuesNode valuesNode, List<EquiJoinClause> criteria)
        {
            List<VariableReferenceExpression> outputVariables = valuesNode.getOutputVariables();
            List<Integer> keyColumnIndices = criteria.stream()
                    .map(clause -> findVariableIndex(outputVariables, clause.getRight()))
                    .collect(Collectors.toList());

            // If any index is -1, we couldn't find the key column - treat as duplicate to skip optimization
            if (keyColumnIndices.stream().anyMatch(idx -> idx < 0)) {
                return true;
            }

            List<List<Object>> allKeys = valuesNode.getRows().stream()
                    .map(row -> keyColumnIndices.stream()
                            .map(row::get)
                            .map(expr -> expr instanceof ConstantExpression ? ((ConstantExpression) expr).getValue() : expr)
                            .collect(Collectors.toList()))
                    .collect(Collectors.toList());

            return allKeys.size() != allKeys.stream().distinct().count();
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }
    }
}
