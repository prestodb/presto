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

import com.facebook.presto.Session;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FunctionAndTypeResolver;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isSimplifyAggregationsOverConstant;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static java.util.Objects.requireNonNull;

/**
 * Folds aggregation functions to constants when the aggregation argument is
 * a constant. This optimization is valid regardless of source cardinality
 * for functions whose result is independent of row count:
 * <ul>
 *   <li>{@code MIN(constant)} &rarr; constant (NULL if constant is NULL)</li>
 *   <li>{@code MAX(constant)} &rarr; constant (NULL if constant is NULL)</li>
 *   <li>{@code ARBITRARY(constant)} &rarr; constant (NULL if constant is NULL)</li>
 *   <li>{@code APPROX_DISTINCT(non-null-constant)} &rarr; 1</li>
 *   <li>{@code APPROX_DISTINCT(NULL)} &rarr; 0</li>
 * </ul>
 *
 * <p>Functions like SUM and COUNT are NOT folded because their results
 * depend on the number of rows (e.g., SUM(5) over N rows = 5*N).
 *
 * <p>Works with any grouping (global or GROUP BY): foldable aggregations are
 * removed from the aggregation node and replaced with constant assignments in
 * a ProjectNode on top. If all aggregations are folded and there are no
 * grouping keys, the entire node is replaced with a ValuesNode.
 *
 * <p>The rule bails out if any aggregation has a filter, mask, distinct, or
 * ordering clause, since those could affect the result.
 */
public class SimplifyAggregationsOverConstant
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation();
    private final StandardFunctionResolution functionResolution;
    private final FunctionAndTypeResolver functionAndTypeResolver;
    private final RowExpressionOptimizer rowExpressionOptimizer;

    public SimplifyAggregationsOverConstant(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        this.functionAndTypeResolver = functionAndTypeManager.getFunctionAndTypeResolver();
        this.rowExpressionOptimizer = new RowExpressionOptimizer(functionAndTypeManager);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isSimplifyAggregationsOverConstant(session);
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        if (node.getStep() != AggregationNode.Step.SINGLE) {
            return Result.empty();
        }

        Map<VariableReferenceExpression, Aggregation> aggregations = node.getAggregations();
        if (aggregations.isEmpty()) {
            return Result.empty();
        }

        // Resolve source to find constant values from ProjectNode assignments or ValuesNode rows
        PlanNode resolvedSource = context.getLookup().resolve(node.getSource());
        ConnectorSession connectorSession = context.getSession().toConnectorSession();
        ConstantResolver constantResolver = buildConstantResolver(resolvedSource, connectorSession);

        // Try to fold each aggregation to a constant
        Map<VariableReferenceExpression, RowExpression> foldedConstants = new LinkedHashMap<>();
        Map<VariableReferenceExpression, Aggregation> remainingAggregations = new LinkedHashMap<>();

        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : aggregations.entrySet()) {
            Optional<RowExpression> folded = tryFold(entry.getValue(), constantResolver);
            if (folded.isPresent()) {
                foldedConstants.put(entry.getKey(), folded.get());
            }
            else {
                remainingAggregations.put(entry.getKey(), entry.getValue());
            }
        }

        if (foldedConstants.isEmpty()) {
            return Result.empty();
        }

        // If all aggregations are folded and there are no grouping keys, replace with ValuesNode
        if (remainingAggregations.isEmpty() && node.getGroupingKeys().isEmpty()) {
            List<RowExpression> row = new ArrayList<>();
            for (VariableReferenceExpression outputVar : node.getOutputVariables()) {
                row.add(foldedConstants.get(outputVar));
            }
            return Result.ofPlanNode(new ValuesNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getOutputVariables(),
                    ImmutableList.of(row),
                    Optional.empty()));
        }

        // Otherwise, remove folded aggregations and project their constants on top
        AggregationNode newAggregation = new AggregationNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                node.getSource(),
                remainingAggregations,
                node.getGroupingSets(),
                node.getPreGroupedVariables(),
                node.getStep(),
                node.getHashVariable(),
                node.getGroupIdVariable(),
                node.getAggregationId());

        Assignments.Builder assignments = Assignments.builder();
        // Pass through grouping keys and remaining aggregations from the new aggregation node
        for (VariableReferenceExpression var : newAggregation.getOutputVariables()) {
            assignments.put(var, var);
        }
        // Add folded aggregations as constant projections
        for (Map.Entry<VariableReferenceExpression, RowExpression> entry : foldedConstants.entrySet()) {
            assignments.put(entry.getKey(), entry.getValue());
        }

        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                newAggregation,
                assignments.build()));
    }

    private Optional<RowExpression> tryFold(Aggregation aggregation, ConstantResolver constantResolver)
    {
        // Bail out if aggregation has filter, mask, distinct, or ordering
        if (aggregation.getFilter().isPresent()
                || aggregation.getMask().isPresent()
                || aggregation.isDistinct()
                || aggregation.getOrderBy().isPresent()) {
            return Optional.empty();
        }

        FunctionHandle functionHandle = aggregation.getFunctionHandle();
        List<RowExpression> arguments = aggregation.getArguments();

        // For functions with arguments, resolve the argument to a constant
        if (arguments.size() != 1) {
            return Optional.empty();
        }

        RowExpression argument = arguments.get(0);
        Optional<ConstantExpression> resolvedConstant = constantResolver.resolve(argument);
        if (!resolvedConstant.isPresent()) {
            return Optional.empty();
        }

        ConstantExpression constantArg = resolvedConstant.get();
        boolean isNull = constantArg.isNull();

        // MIN(constant) -> constant (NULL if null)
        if (functionResolution.isMinFunction(functionHandle)) {
            if (isNull) {
                return Optional.of(constantNull(aggregation.getCall().getType()));
            }
            return Optional.of(constantArg);
        }

        // MAX(constant) -> constant (NULL if null)
        if (functionResolution.isMaxFunction(functionHandle)) {
            if (isNull) {
                return Optional.of(constantNull(aggregation.getCall().getType()));
            }
            return Optional.of(constantArg);
        }

        // ARBITRARY(constant) -> constant (NULL if null)
        if (isArbitraryFunction(functionHandle)) {
            if (isNull) {
                return Optional.of(constantNull(aggregation.getCall().getType()));
            }
            return Optional.of(constantArg);
        }

        // APPROX_DISTINCT(constant) -> 1 if non-null, 0 if null
        if (functionResolution.isApproximateCountDistinctFunction(functionHandle)) {
            return Optional.of(constant(isNull ? 0L : 1L, BIGINT));
        }

        return Optional.empty();
    }

    private ConstantResolver buildConstantResolver(PlanNode resolvedSource, ConnectorSession connectorSession)
    {
        if (resolvedSource instanceof ProjectNode) {
            Assignments assignments = ((ProjectNode) resolvedSource).getAssignments();
            return expression -> {
                if (expression instanceof ConstantExpression) {
                    return Optional.of((ConstantExpression) expression);
                }
                if (expression instanceof VariableReferenceExpression) {
                    RowExpression assigned = assignments.get((VariableReferenceExpression) expression);
                    if (assigned instanceof ConstantExpression) {
                        return Optional.of((ConstantExpression) assigned);
                    }
                    // Try to evaluate the expression to a constant (e.g., CAST(1 AS BIGINT))
                    if (assigned != null) {
                        RowExpression optimized = rowExpressionOptimizer.optimize(assigned, ExpressionOptimizer.Level.OPTIMIZED, connectorSession);
                        if (optimized instanceof ConstantExpression) {
                            return Optional.of((ConstantExpression) optimized);
                        }
                    }
                }
                return Optional.empty();
            };
        }

        if (resolvedSource instanceof ValuesNode) {
            ValuesNode valuesNode = (ValuesNode) resolvedSource;
            List<List<RowExpression>> rows = valuesNode.getRows();
            if (rows.size() == 1) {
                List<VariableReferenceExpression> outputVars = valuesNode.getOutputVariables();
                List<RowExpression> row = rows.get(0);
                return expression -> {
                    if (expression instanceof ConstantExpression) {
                        return Optional.of((ConstantExpression) expression);
                    }
                    if (expression instanceof VariableReferenceExpression) {
                        int index = outputVars.indexOf(expression);
                        if (index >= 0 && index < row.size()) {
                            RowExpression value = row.get(index);
                            if (value instanceof ConstantExpression) {
                                return Optional.of((ConstantExpression) value);
                            }
                            // Try to evaluate the expression to a constant
                            RowExpression optimized = rowExpressionOptimizer.optimize(value, ExpressionOptimizer.Level.OPTIMIZED, connectorSession);
                            if (optimized instanceof ConstantExpression) {
                                return Optional.of((ConstantExpression) optimized);
                            }
                        }
                    }
                    return Optional.empty();
                };
            }
        }

        // For other node types, we can only resolve literal constants
        return expression -> {
            if (expression instanceof ConstantExpression) {
                return Optional.of((ConstantExpression) expression);
            }
            return Optional.empty();
        };
    }

    private boolean isArbitraryFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName()
                .equals(functionAndTypeResolver.qualifyObjectName(QualifiedName.of("arbitrary")));
    }

    @FunctionalInterface
    private interface ConstantResolver
    {
        Optional<ConstantExpression> resolve(RowExpression expression);
    }
}
