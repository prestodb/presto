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

import com.facebook.presto.common.type.EnumType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeWithName;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.UnknownTypeException;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class ExpressionTreeUtils
{
    private ExpressionTreeUtils() {}

    static List<FunctionCall> extractAggregateFunctions(
            Map<NodeRef<FunctionCall>, FunctionHandle> functionHandles,
            Iterable<? extends Node> nodes,
            FunctionAndTypeResolver functionAndTypeResolver)
    {
        return extractExpressions(nodes, FunctionCall.class, isAggregationPredicate(functionHandles, functionAndTypeResolver));
    }

    static List<FunctionCall> extractWindowFunctions(Iterable<? extends Node> nodes)
    {
        return extractExpressions(nodes, FunctionCall.class, ExpressionTreeUtils::isWindowFunction);
    }

    static List<FunctionCall> extractExternalFunctions(Map<NodeRef<FunctionCall>, FunctionHandle> functionHandles, Iterable<? extends Node> nodes, FunctionAndTypeResolver functionAndTypeResolver)
    {
        return extractExpressions(nodes, FunctionCall.class, isExternalFunctionPredicate(functionHandles, functionAndTypeResolver));
    }

    public static <T extends Expression> List<T> extractExpressions(
            Iterable<? extends Node> nodes,
            Class<T> clazz)
    {
        return extractExpressions(nodes, clazz, alwaysTrue());
    }

    private static Predicate<FunctionCall> isAggregationPredicate(Map<NodeRef<FunctionCall>, FunctionHandle> functionHandles, FunctionAndTypeResolver functionAndTypeResolver)
    {
        return functionCall -> (functionAndTypeResolver.getFunctionMetadata(functionHandles.get(NodeRef.of(functionCall))).getFunctionKind() == AGGREGATE || functionCall.getFilter().isPresent())
                && !functionCall.getWindow().isPresent()
                || functionCall.getOrderBy().isPresent();
    }

    private static boolean isWindowFunction(FunctionCall functionCall)
    {
        return functionCall.getWindow().isPresent();
    }

    private static Predicate<FunctionCall> isExternalFunctionPredicate(Map<NodeRef<FunctionCall>, FunctionHandle> functionHandles, FunctionAndTypeResolver functionAndTypeResolver)
    {
        return functionCall -> functionAndTypeResolver.getFunctionMetadata(functionHandles.get(NodeRef.of(functionCall))).getImplementationType().isExternalExecution();
    }

    private static <T extends Expression> List<T> extractExpressions(
            Iterable<? extends Node> nodes,
            Class<T> clazz,
            Predicate<T> predicate)
    {
        requireNonNull(nodes, "nodes is null");
        requireNonNull(clazz, "clazz is null");
        requireNonNull(predicate, "predicate is null");

        return ImmutableList.copyOf(nodes).stream()
                .flatMap(node -> linearizeNodes(node).stream())
                .filter(clazz::isInstance)
                .map(clazz::cast)
                .filter(predicate)
                .collect(toImmutableList());
    }

    private static List<Node> linearizeNodes(Node node)
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        new DefaultExpressionTraversalVisitor<Node, Void>()
        {
            @Override
            public Node process(Node node, Void context)
            {
                Node result = super.process(node, context);
                nodes.add(node);
                return result;
            }
        }.process(node, null);
        return nodes.build();
    }

    public static boolean isEqualComparisonExpression(Expression expression)
    {
        return expression instanceof ComparisonExpression && ((ComparisonExpression) expression).getOperator() == ComparisonExpression.Operator.EQUAL;
    }

    public static Optional<TypeWithName> tryResolveEnumLiteralType(QualifiedName qualifiedName, FunctionAndTypeResolver functionAndTypeResolver)
    {
        Optional<QualifiedName> prefix = qualifiedName.getPrefix();
        if (!prefix.isPresent()) {
            // an enum literal should be of the form `MyEnum.my_key`
            return Optional.empty();
        }
        try {
            Type baseType = functionAndTypeResolver.getType(parseTypeSignature(prefix.get().toString()));
            if (baseType instanceof TypeWithName
                    && ((TypeWithName) baseType).getType() instanceof EnumType
                    && ((EnumType<?>) ((TypeWithName) baseType).getType()).getEnumMap().containsKey(qualifiedName.getSuffix().toUpperCase(ENGLISH))) {
                return Optional.of((TypeWithName) baseType);
            }
        }
        catch (IllegalArgumentException | UnknownTypeException e) {
            return Optional.empty();
        }
        return Optional.empty();
    }

    public static Object resolveEnumLiteral(DereferenceExpression node, Type nodeType)
    {
        QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(node);

        EnumType enumType = (EnumType) ((TypeWithName) nodeType).getType();
        String enumKey = qualifiedName.getSuffix().toUpperCase(ENGLISH);
        checkArgument(enumType.getEnumMap().containsKey(enumKey), format("No key '%s' in enum '%s'", enumKey, nodeType.getDisplayName()));
        Object enumValue = enumType.getEnumMap().get(enumKey);
        return enumValue instanceof String ? utf8Slice((String) enumValue) : enumValue;
    }

    public static FieldId checkAndGetColumnReferenceField(Expression expression, Multimap<NodeRef<Expression>, FieldId> columnReferences)
    {
        checkState(columnReferences.containsKey(NodeRef.of(expression)), "Missing field reference for expression");
        checkState(columnReferences.get(NodeRef.of(expression)).size() == 1, "Multiple field references for expression");

        return columnReferences.get(NodeRef.of(expression)).iterator().next();
    }

    public static boolean isNonNullConstant(Expression expression)
    {
        Expression tempExpression = expression;
        while (tempExpression instanceof Cast) {
            tempExpression = ((Cast) tempExpression).getExpression();
        }

        if (tempExpression instanceof NullLiteral) {
            return false;
        }

        // now allow for things like ARRAY, ROW(...) where null is OK
        return isConstant(tempExpression);
    }

    public static boolean isConstant(Expression expression)
    {
        Expression tempExpression = expression;
        while (tempExpression instanceof Cast) {
            tempExpression = ((Cast) tempExpression).getExpression();
        }

        if (tempExpression instanceof Literal) {
            return true;
        }

        if (tempExpression instanceof ArrayConstructor) {
            return ((ArrayConstructor) tempExpression).getValues().stream().allMatch(ExpressionTreeUtils::isConstant);
        }

        // ROW an MAP are special so we explicitly do that here.
        if (tempExpression instanceof Row) {
            return (((Row) tempExpression).getItems().stream().allMatch(ExpressionTreeUtils::isConstant));
        }

        if (tempExpression instanceof FunctionCall) {
            // Hack to just allow map constructor
            if (((FunctionCall) tempExpression).getName().getSuffix().equalsIgnoreCase("map")) {
                return ((FunctionCall) tempExpression).getArguments().stream().allMatch(ExpressionTreeUtils::isConstant);
            }
        }

        // Everything else is considered non-const
        return false;
    }

    public static Optional<SourceLocation> getSourceLocation(Optional<NodeLocation> nodeLocation)
    {
        return nodeLocation.isPresent()
                ? Optional.of(new SourceLocation(nodeLocation.get().getLineNumber(), nodeLocation.get().getColumnNumber()))
                : Optional.empty();
    }

    public static Optional<SourceLocation> getSourceLocation(Node node)
    {
        Optional<NodeLocation> nodeLocation = node.getLocation();
        if (!node.getLocation().isPresent()) {
            // See if any child has a location
            nodeLocation = node.getChildren().stream()
                    .map(x -> x.getLocation())
                    .filter(Optional::isPresent)
                    .findFirst()
                    .map(x -> x.get());
        }

        return getSourceLocation(nodeLocation);
    }

    public static Optional<NodeLocation> getNodeLocation(Optional<SourceLocation> sourceLocation)
    {
        if (sourceLocation.isPresent()) {
            return Optional.of(new NodeLocation(sourceLocation.get().getLine(), sourceLocation.get().getColumn()));
        }

        return Optional.empty();
    }

    public static SymbolReference createSymbolReference(VariableReferenceExpression variableReferenceExpression)
    {
        return new SymbolReference(getNodeLocation(variableReferenceExpression.getSourceLocation()), variableReferenceExpression.getName());
    }
}
