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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.tree.StackableAstVisitor.StackableAstVisitorContext;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;
import static java.util.Collections.reverse;

public class FunctionArgumentCheckerForAccessControlUtils
{
    private static final QualifiedName TRANSFORM = QualifiedName.of("transform");
    private static final QualifiedName CARDINALITY = QualifiedName.of("cardinality");

    private FunctionArgumentCheckerForAccessControlUtils() {}

    // Returns whether function argument at `argumentIndex` for function `node` needs to be checked
    // for column level access control.
    // For e.g., consider SQL `transform(arr, col -> col.x)`
    // Here, we only need to check for access of subfield `x` in column `arr` which is of type `Array<struct>`.
    // So we can just parse lambda and ignore the first argument for access checks.
    public static boolean isUnusedArgumentForAccessControl(FunctionCall node, int argumentIndex, ExpressionAnalyzer.Context context)
    {
        if (node.getName().equals(TRANSFORM)) {
            checkState(node.getArguments().size() == 2);
            return argumentIndex == 0;
        }
        if (node.getName().equals(CARDINALITY)) {
            checkState(node.getArguments().size() == 1);
            return argumentIndex == 0;
        }
        return false;
    }

    // Parses arguments of function `node` which are a lambda expression, and returns a map
    // of their lambda arguments to resolved subfield.
    // For e.g., consider SQL `SELECT transform(arr, col -> col.x) FROM table`
    // Return value = Map('col' -> ResolvedSubfield(table.arr))
    public static Map<Identifier, ResolvedSubfield> getResolvedLambdaArguments(
            FunctionCall node,
            StackableAstVisitorContext<ExpressionAnalyzer.Context> context,
            Map<NodeRef<Expression>, Type> expressionTypes)
    {
        ImmutableMap.Builder<Identifier, ResolvedSubfield> resolvedLambdaArguments = ImmutableMap.builder();
        if (node.getName().equals(TRANSFORM)) {
            checkState(node.getArguments().size() == 2);
            if (!(node.getArguments().get(1) instanceof LambdaExpression)) {
                return ImmutableMap.of();
            }
            Expression arrayExpression = node.getArguments().get(0);
            LambdaExpression lambdaExpression = ((LambdaExpression) node.getArguments().get(1));
            Optional<ResolvedSubfield> resolvedSubfield = resolveSubfield(arrayExpression, context, expressionTypes);
            if (resolvedSubfield.isPresent()) {
                resolvedLambdaArguments.put(
                        lambdaExpression.getArguments().get(0).getName(),
                        resolvedSubfield.get());
            }
        }
        return resolvedLambdaArguments.build();
    }

    public static Optional<ResolvedSubfield> resolveSubfield(
            Expression node,
            StackableAstVisitorContext<ExpressionAnalyzer.Context> context,
            Map<NodeRef<Expression>, Type> expressionTypes)
    {
        // If expression is nested with multiple dereferences and subscripts, we only look at the topmost one.
        if (!isTopMostReference(node, context)) {
            return Optional.empty();
        }

        Scope scope = context.getContext().getScope();
        Expression childNode = node;
        List<Subfield.PathElement> columnDereferences = new ArrayList<>();
        while (true) {
            // Dereference row/array/map expressions
            if (childNode instanceof SubscriptExpression) {
                SubscriptExpression subscriptExpression = (SubscriptExpression) childNode;
                childNode = subscriptExpression.getBase();
                Type baseType = expressionTypes.get(NodeRef.of(childNode));
                if (baseType == null || !(baseType instanceof RowType)) {
                    continue;
                }
                int index = toIntExact(((LongLiteral) subscriptExpression.getIndex()).getValue());
                RowType baseRowType = (RowType) baseType;
                Optional<String> dereference = baseRowType.getFields().get(index - 1).getName();
                if (!dereference.isPresent()) {
                    break;
                }
                columnDereferences.add(new Subfield.NestedField(dereference.get()));
                continue;
            }

            QualifiedName childQualifiedName;
            // Dereference subfield expressions
            if (childNode instanceof DereferenceExpression) {
                childQualifiedName = DereferenceExpression.getQualifiedName((DereferenceExpression) childNode);
            }
            // Base case
            else if (childNode instanceof Identifier) {
                childQualifiedName = QualifiedName.of(((Identifier) childNode).getValue());
            }
            else {
                break;
            }
            // If we found the full de-referenced expression, return it as a ResolvedSubfield
            if (childQualifiedName != null) {
                Optional<ResolvedField> resolvedField = scope.tryResolveField(childNode, childQualifiedName);
                if (resolvedField.isPresent() && !resolvedField.get().getField().getOriginTable().isPresent()) {
                    // Try to resolve using lambda expressions
                    Optional<ResolvedSubfield> resolvedSubField = Optional.ofNullable(context.getContext().getResolvedLambdaArguments().get(childNode));
                    if (resolvedSubField.isPresent()) {
                        resolvedField = Optional.of(resolvedSubField.get().getResolvedField());
                        columnDereferences.addAll(Lists.reverse(resolvedSubField.get().getSubfield().getPath()));
                    }
                }
                if (resolvedField.isPresent() &&
                        resolvedField.get().getField().getOriginColumnName().isPresent() &&
                        resolvedField.get().getField().getOriginTable().isPresent()) {
                    reverse(columnDereferences);
                    return Optional.of(new ResolvedSubfield(
                            resolvedField.get(),
                            new Subfield(resolvedField.get().getField().getOriginColumnName().get(), columnDereferences)));
                }
            }
            // If we cannot resolve full de-referenced name, that means that there are
            // more dereferences to be resolved, so we continue the while loop with new childNode.
            if (childNode instanceof DereferenceExpression) {
                columnDereferences.add(new Subfield.NestedField(((DereferenceExpression) childNode).getField().getValue()));
                childNode = ((DereferenceExpression) childNode).getBase();
                continue;
            }
            break;
        }
        return Optional.empty();
    }

    public static boolean isDereferenceOrSubscript(Expression node)
    {
        return node instanceof DereferenceExpression || node instanceof SubscriptExpression;
    }

    public static boolean isTopMostReference(Expression node, StackableAstVisitorContext<ExpressionAnalyzer.Context> context)
    {
        if (!context.getPreviousNode().isPresent()) {
            return true;
        }
        return !isDereferenceOrSubscript((Expression) context.getPreviousNode().get());
    }
}
