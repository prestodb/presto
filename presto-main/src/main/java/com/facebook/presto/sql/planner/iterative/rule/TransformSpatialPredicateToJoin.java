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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.planner.ExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.sql.planner.SymbolsExtractor.extractUnique;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.util.SpatialJoinUtils.extractSupportedSpatialFunctions;
import static com.google.common.base.Verify.verify;

/**
 * Applies to broadcast spatial joins using ST_Contains and ST_Intersects functions.
 * <p>
 * Applies only if all of the following conditions are met:
 * - each argument of the spatial function is an expression using at least one of the symbols
 *      from the join relations
 * - each argument's expression uses symbols from only one side of the join
 * - different arguments use symbols from different sides of the join
 * <p>
 * Replaces cross join node with a filter on top with a single spatial join node.
 * <p>
 * Pushes non-trivial expressions of the spatial function arguments into projections on top of
 * join child nodes.
 * <p>
 * For example, rewrites ST_Contains(ST_GeometryFromText(a.wkt), ST_Point(b.longitude, b.latitude)) join
 * as ST_Contains(st_geometryfromtext, st_point) with st_geometryfromtext -> 'ST_GeometryFromText(a.wkt)' and
 * st_point -> 'ST_Point(b.longitude, b.latitude)' projections on top of child nodes.
 */
public class TransformSpatialPredicateToJoin
        implements Rule<FilterNode>
{
    private static final Capture<JoinNode> JOIN = newCapture();
    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(join().capturedAs(JOIN).matching(node -> node.isCrossJoin())));
    private static final TypeSignature GEOMETRY_TYPE_SIGNATURE = parseTypeSignature("Geometry");

    private final Metadata metadata;

    public TransformSpatialPredicateToJoin(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return SystemSessionProperties.isSpatialJoinEanbled(session);
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(JOIN);

        Expression filter = node.getPredicate();
        List<FunctionCall> spatialFunctions = extractSupportedSpatialFunctions(filter);
        if (spatialFunctions.isEmpty()) {
            return Result.empty();
        }

        for (FunctionCall spatialFunction : spatialFunctions) {
            List<Expression> arguments = spatialFunction.getArguments();
            verify(arguments.size() == 2);

            Expression firstArgument = arguments.get(0);
            Expression secondArgument = arguments.get(1);

            Set<Symbol> firstSymbols = extractUnique(firstArgument);
            Set<Symbol> secondSymbols = extractUnique(secondArgument);

            if (firstSymbols.isEmpty() || secondSymbols.isEmpty()) {
                return Result.empty();
            }

            Optional<Symbol> newFirstSymbol = newSymbol(context, firstArgument);
            Optional<Symbol> newSecondSymbol = newSymbol(context, secondArgument);

            Expression newFirstArgument = toExpression(newFirstSymbol, firstArgument);
            Expression newSecondArgument = toExpression(newSecondSymbol, secondArgument);

            Expression newSpatialFunction = new FunctionCall(spatialFunction.getName(), ImmutableList.of(newFirstArgument, newSecondArgument));
            Expression newFilter = replaceExpression(filter, ImmutableMap.of(spatialFunction, newSpatialFunction));

            PlanNode leftNode = joinNode.getLeft();
            PlanNode rightNode = joinNode.getRight();

            List<Symbol> leftSymbols = leftNode.getOutputSymbols();
            List<Symbol> rightSymbols = rightNode.getOutputSymbols();

            PlanNode newLeftNode;
            PlanNode newRightNode;

            if (leftSymbols.containsAll(firstSymbols)
                    && containsNone(leftSymbols, secondSymbols)
                    && rightSymbols.containsAll(secondSymbols)
                    && containsNone(rightSymbols, firstSymbols)) {
                newLeftNode = newFirstSymbol.map(symbol -> addProjection(context, leftNode, symbol, firstArgument)).orElse(leftNode);
                newRightNode = newSecondSymbol.map(symbol -> addProjection(context, rightNode, symbol, secondArgument)).orElse(rightNode);
            }
            else if (leftSymbols.containsAll(secondSymbols)
                    && containsNone(leftSymbols, firstSymbols)
                    && rightSymbols.containsAll(firstSymbols)
                    && containsNone(rightSymbols, secondSymbols)) {
                newLeftNode = newSecondSymbol.map(symbol -> addProjection(context, leftNode, symbol, secondArgument)).orElse(leftNode);
                newRightNode = newFirstSymbol.map(symbol -> addProjection(context, rightNode, symbol, firstArgument)).orElse(rightNode);
            }
            else {
                continue;
            }

            return Result.ofPlanNode(new JoinNode(
                    node.getId(),
                    joinNode.getType(),
                    newLeftNode,
                    newRightNode,
                    joinNode.getCriteria(),
                    node.getOutputSymbols(),
                    Optional.of(newFilter),
                    joinNode.getLeftHashSymbol(),
                    joinNode.getRightHashSymbol(),
                    joinNode.getDistributionType()));
        }

        return Result.empty();
    }

    private Expression toExpression(Optional<Symbol> optionalSymbol, Expression defaultExpression)
    {
        return optionalSymbol.map(symbol -> (Expression) symbol.toSymbolReference()).orElse(defaultExpression);
    }

    private Optional<Symbol> newSymbol(Context context, Expression expression)
    {
        if (expression instanceof SymbolReference) {
            return Optional.empty();
        }

        return Optional.of(context.getSymbolAllocator().newSymbol(expression, metadata.getType(GEOMETRY_TYPE_SIGNATURE)));
    }

    private PlanNode addProjection(Context context, PlanNode node, Symbol symbol, Expression expression)
    {
        Assignments.Builder projections = Assignments.builder();
        for (Symbol outputSymbol : node.getOutputSymbols()) {
            projections.putIdentity(outputSymbol);
        }

        projections.put(symbol, expression);
        return new ProjectNode(context.getIdAllocator().getNextId(), node, projections.build());
    }

    private boolean containsNone(Collection<Symbol> values, Collection<Symbol> testValues)
    {
        return values.stream().noneMatch(ImmutableSet.copyOf(testValues)::contains);
    }
}
