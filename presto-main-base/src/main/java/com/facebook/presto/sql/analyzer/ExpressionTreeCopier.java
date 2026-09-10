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

import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.CharLiteral;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.CurrentUser;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.EnumLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;

/**
 * Creates a deep copy of an expression tree.
 * <p>
 * The analyzer stores per-node state (types, coercions, column references, ...) keyed by
 * node identity. When the same expression needs to be analyzed in two different contexts
 * (for example a SELECT item and a HAVING predicate that references it by alias), each
 * context must get its own copy of the tree, otherwise the state recorded for one context
 * silently overwrites the state recorded for the other.
 * <p>
 * The copy is structurally equal to the original ({@code copy.equals(original)}) but does
 * not share any node with it, except for the {@link com.facebook.presto.sql.tree.Query}
 * nested inside a {@link SubqueryExpression}, which is not part of the expression tree.
 */
final class ExpressionTreeCopier
        extends ExpressionRewriter<Void>
{
    private ExpressionTreeCopier() {}

    /**
     * @throws UnsupportedOperationException if the expression contains a node that cannot be copied
     */
    public static <T extends Expression> T copy(T expression)
    {
        T copy = ExpressionTreeRewriter.rewriteWith(new ExpressionTreeCopier(), expression);
        verifyNoSharedNodes(expression, copy);
        return copy;
    }

    private static void verifyNoSharedNodes(Expression original, Expression copy)
    {
        Set<Node> originalNodes = Collections.newSetFromMap(new IdentityHashMap<>());
        collectNodes(original, originalNodes);
        Set<Node> copiedNodes = Collections.newSetFromMap(new IdentityHashMap<>());
        collectNodes(copy, copiedNodes);
        for (Node node : copiedNodes) {
            if (originalNodes.contains(node)) {
                throw new UnsupportedOperationException(format("Cannot copy expression: %s (node of type %s was not copied)", original, node.getClass().getSimpleName()));
            }
        }
    }

    private static void collectNodes(Node node, Set<Node> nodes)
    {
        nodes.add(node);
        if (node instanceof SubqueryExpression) {
            // the nested query is intentionally shared between the original and the copy
            return;
        }
        for (Node child : node.getChildren()) {
            collectNodes(child, nodes);
        }
    }

    @Override
    public Expression rewriteLiteral(Literal node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        Optional<NodeLocation> location = node.getLocation();
        if (node instanceof LongLiteral) {
            String value = Long.toString(((LongLiteral) node).getValue());
            return location.isPresent() ? new LongLiteral(location.get(), value) : new LongLiteral(value);
        }
        if (node instanceof DoubleLiteral) {
            String value = Double.toString(((DoubleLiteral) node).getValue());
            return location.isPresent() ? new DoubleLiteral(location.get(), value) : new DoubleLiteral(value);
        }
        if (node instanceof DecimalLiteral) {
            return new DecimalLiteral(location, ((DecimalLiteral) node).getValue());
        }
        if (node instanceof StringLiteral) {
            String value = ((StringLiteral) node).getValue();
            return location.isPresent() ? new StringLiteral(location.get(), value) : new StringLiteral(value);
        }
        if (node instanceof CharLiteral) {
            return new CharLiteral(location, ((CharLiteral) node).getValue());
        }
        if (node instanceof BooleanLiteral) {
            String value = Boolean.toString(((BooleanLiteral) node).getValue());
            return location.isPresent() ? new BooleanLiteral(location.get(), value) : new BooleanLiteral(value);
        }
        if (node instanceof NullLiteral) {
            return location.isPresent() ? new NullLiteral(location.get()) : new NullLiteral();
        }
        if (node instanceof BinaryLiteral) {
            return new BinaryLiteral(location, ((BinaryLiteral) node).toHexString());
        }
        if (node instanceof GenericLiteral) {
            GenericLiteral literal = (GenericLiteral) node;
            return location.isPresent()
                    ? new GenericLiteral(location.get(), literal.getType(), literal.getValue())
                    : new GenericLiteral(literal.getType(), literal.getValue());
        }
        if (node instanceof TimeLiteral) {
            String value = ((TimeLiteral) node).getValue();
            return location.isPresent() ? new TimeLiteral(location.get(), value) : new TimeLiteral(value);
        }
        if (node instanceof TimestampLiteral) {
            String value = ((TimestampLiteral) node).getValue();
            return location.isPresent() ? new TimestampLiteral(location.get(), value) : new TimestampLiteral(value);
        }
        if (node instanceof IntervalLiteral) {
            IntervalLiteral literal = (IntervalLiteral) node;
            return location.isPresent()
                    ? new IntervalLiteral(location.get(), literal.getValue(), literal.getSign(), literal.getStartField(), literal.getEndField())
                    : new IntervalLiteral(literal.getValue(), literal.getSign(), literal.getStartField(), literal.getEndField());
        }
        if (node instanceof EnumLiteral) {
            EnumLiteral literal = (EnumLiteral) node;
            return new EnumLiteral(location, literal.getType(), literal.getValue());
        }
        throw new UnsupportedOperationException("Cannot copy literal of type " + node.getClass().getName());
    }

    @Override
    public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        return copyIdentifier(node);
    }

    @Override
    public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        return new SymbolReference(node.getLocation(), node.getName());
    }

    @Override
    public Expression rewriteParameter(Parameter node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        return node.getLocation().isPresent()
                ? new Parameter(node.getLocation().get(), node.getPosition())
                : new Parameter(node.getPosition());
    }

    @Override
    public Expression rewriteFieldReference(FieldReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        return new FieldReference(node.getLocation(), node.getFieldIndex());
    }

    @Override
    public Expression rewriteCurrentTime(CurrentTime node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        Optional<NodeLocation> location = node.getLocation();
        if (node.getPrecision() == null) {
            return location.isPresent() ? new CurrentTime(location.get(), node.getFunction()) : new CurrentTime(node.getFunction());
        }
        return location.isPresent()
                ? new CurrentTime(location.get(), node.getFunction(), node.getPrecision())
                : new CurrentTime(node.getFunction(), node.getPrecision());
    }

    @Override
    public Expression rewriteCurrentUser(CurrentUser node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        return node.getLocation().isPresent() ? new CurrentUser(node.getLocation().get()) : new CurrentUser();
    }

    @Override
    public Expression rewriteGroupingOperation(GroupingOperation node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        // grouping columns are not children of the node and are not visited by the tree rewriter
        List<QualifiedName> groupingColumns = node.getGroupingColumns().stream()
                .map(column -> column instanceof Identifier
                        ? QualifiedName.of(ImmutableList.of(copyIdentifier((Identifier) column)))
                        : DereferenceExpression.getQualifiedName((DereferenceExpression) column))
                .collect(ImmutableList.toImmutableList());
        return new GroupingOperation(node.getLocation(), groupingColumns);
    }

    @Override
    public Expression rewriteSubqueryExpression(SubqueryExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        // the nested Query is a statement, not an expression, and is shared with the original
        return node.getLocation().isPresent()
                ? new SubqueryExpression(node.getLocation().get(), node.getQuery())
                : new SubqueryExpression(node.getQuery());
    }

    @Override
    public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        // The default rewrite only creates new nodes when a child changed, which is not the case for
        // calls without arguments (e.g. count(*)) or for window components without expressions
        // (e.g. OVER (), UNBOUNDED PRECEDING), so all parts are rebuilt explicitly.
        List<Expression> arguments = node.getArguments().stream()
                .map(argument -> treeRewriter.rewrite(argument, context))
                .collect(ImmutableList.toImmutableList());
        Optional<Expression> filter = node.getFilter().map(expression -> treeRewriter.rewrite(expression, context));
        Optional<OrderBy> orderBy = node.getOrderBy().map(value -> copyOrderBy(value, treeRewriter, context));
        Optional<Window> window = node.getWindow().map(value -> copyWindow(value, treeRewriter, context));
        return node.getLocation().isPresent()
                ? new FunctionCall(node.getLocation().get(), node.getName(), window, filter, orderBy, node.isDistinct(), node.isIgnoreNulls(), arguments)
                : new FunctionCall(node.getName(), window, filter, orderBy, node.isDistinct(), node.isIgnoreNulls(), arguments);
    }

    private static Window copyWindow(Window window, ExpressionTreeRewriter<Void> treeRewriter, Void context)
    {
        List<Expression> partitionBy = window.getPartitionBy().stream()
                .map(expression -> treeRewriter.rewrite(expression, context))
                .collect(ImmutableList.toImmutableList());
        Optional<OrderBy> orderBy = window.getOrderBy().map(value -> copyOrderBy(value, treeRewriter, context));
        Optional<WindowFrame> frame = window.getFrame().map(value -> copyWindowFrame(value, treeRewriter, context));
        return window.getLocation().isPresent()
                ? new Window(window.getLocation().get(), partitionBy, orderBy, frame)
                : new Window(partitionBy, orderBy, frame);
    }

    private static WindowFrame copyWindowFrame(WindowFrame frame, ExpressionTreeRewriter<Void> treeRewriter, Void context)
    {
        FrameBound start = copyFrameBound(frame.getStart(), treeRewriter, context);
        Optional<FrameBound> end = frame.getEnd().map(bound -> copyFrameBound(bound, treeRewriter, context));
        return frame.getLocation().isPresent()
                ? new WindowFrame(frame.getLocation().get(), frame.getType(), start, end)
                : new WindowFrame(frame.getType(), start, end);
    }

    private static FrameBound copyFrameBound(FrameBound bound, ExpressionTreeRewriter<Void> treeRewriter, Void context)
    {
        Optional<Expression> value = bound.getValue().map(expression -> treeRewriter.rewrite(expression, context));
        if (bound.getLocation().isPresent()) {
            return value.isPresent()
                    ? new FrameBound(bound.getLocation().get(), bound.getType(), value.get())
                    : new FrameBound(bound.getLocation().get(), bound.getType());
        }
        return value.isPresent() ? new FrameBound(bound.getType(), value.get()) : new FrameBound(bound.getType());
    }

    private static OrderBy copyOrderBy(OrderBy orderBy, ExpressionTreeRewriter<Void> treeRewriter, Void context)
    {
        List<SortItem> sortItems = orderBy.getSortItems().stream()
                .map(item -> {
                    Expression sortKey = treeRewriter.rewrite(item.getSortKey(), context);
                    return item.getLocation().isPresent()
                            ? new SortItem(item.getLocation().get(), sortKey, item.getOrdering(), item.getNullOrdering())
                            : new SortItem(sortKey, item.getOrdering(), item.getNullOrdering());
                })
                .collect(ImmutableList.toImmutableList());
        return orderBy.getLocation().isPresent() ? new OrderBy(orderBy.getLocation().get(), sortItems) : new OrderBy(sortItems);
    }

    @Override
    public Expression rewriteArrayConstructor(ArrayConstructor node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        if (!node.getValues().isEmpty()) {
            return null;
        }
        return node.getLocation().isPresent() ? new ArrayConstructor(node.getLocation().get(), ImmutableList.of()) : new ArrayConstructor(ImmutableList.of());
    }

    @Override
    public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        Expression base = treeRewriter.rewrite(node.getBase(), context);
        Identifier field = copyIdentifier(node.getField());
        return node.getLocation().isPresent()
                ? new DereferenceExpression(node.getLocation().get(), base, field)
                : new DereferenceExpression(base, field);
    }

    @Override
    public Expression rewriteRow(Row node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        ImmutableList.Builder<Row.Field> fields = ImmutableList.builder();
        for (Row.Field field : node.getFields()) {
            fields.add(new Row.Field(
                    field.getLocation(),
                    field.getName().map(ExpressionTreeCopier::copyIdentifier),
                    treeRewriter.rewrite(field.getExpression(), context)));
        }
        return node.getLocation().isPresent() ? new Row(node.getLocation().get(), fields.build()) : new Row(fields.build());
    }

    @Override
    public Expression rewriteLambdaExpression(LambdaExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        List<LambdaArgumentDeclaration> arguments = node.getArguments().stream()
                .map(argument -> new LambdaArgumentDeclaration(copyIdentifier(argument.getName())))
                .collect(ImmutableList.toImmutableList());
        Expression body = treeRewriter.rewrite(node.getBody(), context);
        return new LambdaExpression(node.getLocation(), arguments, body);
    }

    private static Identifier copyIdentifier(Identifier node)
    {
        return node.getLocation().isPresent()
                ? new Identifier(node.getLocation().get(), node.getValue(), node.isDelimited())
                : new Identifier(node.getValue(), node.isDelimited());
    }
}
