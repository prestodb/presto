package com.facebook.presto.sql.planner;

import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.base.Charsets;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;

public class ExpressionInterpreter
        extends AstVisitor<Object, Void>
{
    private final SymbolResolver resolver;

    public ExpressionInterpreter(SymbolResolver resolver)
    {
        checkNotNull(resolver, "resolver is null");
        this.resolver = resolver;
    }

    @Override
    protected Object visitQualifiedNameReference(QualifiedNameReference node, Void context)
    {
        Symbol symbol = Symbol.fromQualifiedName(node.getName());
        return resolver.getValue(symbol);
    }

    @Override
    protected Long visitLongLiteral(LongLiteral node, Void context)
    {
        return Long.valueOf(node.getValue());
    }

    @Override
    protected Double visitDoubleLiteral(DoubleLiteral node, Void context)
    {
        return Double.valueOf(node.getValue());
    }

    @Override
    protected Slice visitStringLiteral(StringLiteral node, Void context)
    {
        return Slices.wrappedBuffer(node.getValue().getBytes(UTF_8));
    }

    @Override
    protected Object visitNullLiteral(NullLiteral node, Void context)
    {
        return null;
    }

    @Override
    protected Object visitIsNullPredicate(IsNullPredicate node, Void context)
    {
        Object value = process(node.getValue(), context);

        if (value instanceof Expression) {
            return node;
        }

        return value == null;
    }

    @Override
    protected Object visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
    {
        Object value = process(node.getValue(), context);

        if (value instanceof Expression) {
            return node;
        }

        return value != null;
    }

    @Override
    protected Object visitCoalesceExpression(CoalesceExpression node, Void context)
    {
        for (Expression expression : node.getOperands()) {
            Object value = process(expression, context);

            if (value instanceof Expression) {
                // TODO: optimize this case
                return node;
            }

            if (value != null) {
                return value;
            }
        }
        return null;
    }

    @Override
    protected Object visitArithmeticExpression(ArithmeticExpression node, Void context)
    {
        Object left = process(node.getLeft(), context);
        if (left == null) {
            return null;
        }
        Object right = process(node.getRight(), context);
        if (right == null) {
            return null;
        }

        if (left instanceof Expression || right instanceof Expression) {
            return node;
        }

        Number leftNumber = (Number) left;
        Number rightNumber = (Number) right;
        switch (node.getType()) {
            case ADD:
                if (leftNumber instanceof Long && rightNumber instanceof Long) {
                    return leftNumber.longValue() + rightNumber.longValue();
                }
                else {
                    return leftNumber.doubleValue() + rightNumber.doubleValue();
                }
            case SUBTRACT:
                if (leftNumber instanceof Long && rightNumber instanceof Long) {
                    return leftNumber.longValue() - rightNumber.longValue();
                }
                else {
                    return leftNumber.doubleValue() - rightNumber.doubleValue();
                }
            case DIVIDE:
                if (leftNumber instanceof Long && rightNumber instanceof Long) {
                    return leftNumber.longValue() / rightNumber.longValue();
                }
                else {
                    return leftNumber.doubleValue() / rightNumber.doubleValue();
                }
            case MULTIPLY:
                if (leftNumber instanceof Long && rightNumber instanceof Long) {
                    return leftNumber.longValue() * rightNumber.longValue();
                }
                else {
                    return leftNumber.doubleValue() * rightNumber.doubleValue();
                }
            case MODULUS:
                if (leftNumber instanceof Long && rightNumber instanceof Long) {
                    return leftNumber.longValue() % rightNumber.longValue();
                }
                else {
                    return leftNumber.doubleValue() % rightNumber.doubleValue();
                }
            default:
                throw new UnsupportedOperationException("not yet implemented: " + node.getType());
        }
    }

    @Override
    protected Object visitComparisonExpression(ComparisonExpression node, Void context)
    {
        Object left = process(node.getLeft(), context);
        if (left == null) {
            return null;
        }
        Object right = process(node.getRight(), context);
        if (right == null) {
            return null;
        }

        if (left instanceof Number && right instanceof Number) {
            switch (node.getType()) {
                case EQUAL:
                    return ((Number) left).doubleValue() == ((Number) right).doubleValue();
                case NOT_EQUAL:
                    return ((Number) left).doubleValue() != ((Number) right).doubleValue();
                case LESS_THAN:
                    return ((Number) left).doubleValue() < ((Number) right).doubleValue();
                case LESS_THAN_OR_EQUAL:
                    return ((Number) left).doubleValue() <= ((Number) right).doubleValue();
                case GREATER_THAN:
                    return ((Number) left).doubleValue() > ((Number) right).doubleValue();
                case GREATER_THAN_OR_EQUAL:
                    return ((Number) left).doubleValue() >= ((Number) right).doubleValue();
            }
        }
        else if (left instanceof Slice && right instanceof Slice) {
            switch (node.getType()) {
                case EQUAL:
                    return left.equals(right);
                case NOT_EQUAL:
                    return !left.equals(right);
                case LESS_THAN:
                    return ((Slice) left).compareTo((Slice) right) < 0;
                case LESS_THAN_OR_EQUAL:
                    return ((Slice) left).compareTo((Slice) right) <= 0;
                case GREATER_THAN:
                    return ((Slice) left).compareTo((Slice) right) > 0;
                case GREATER_THAN_OR_EQUAL:
                    return ((Slice) left).compareTo((Slice) right) >= 0;
            }
        }

        return new ComparisonExpression(node.getType(), toExpression(left), toExpression(right));
    }

    @Override
    protected Object visitNullIfExpression(NullIfExpression node, Void context)
    {
        Object first = process(node.getFirst(), context);
        if (first == null) {
            return null;
        }
        Object second = process(node.getSecond(), context);
        if (second == null) {
            return null;
        }

        if (first instanceof Number && second instanceof Number) {
            return ((Number) first).doubleValue() == ((Number) second).doubleValue() ? null : first;
        }
        else if (first instanceof Slice && second instanceof Slice) {
            return first.equals(second) ? null : first;
        }

        return node;
    }

    @Override
    protected Object visitNotExpression(NotExpression node, Void context)
    {
        Object value = process(node.getValue(), context);
        if (value == null) {
            return null;
        }

        if (value instanceof Expression) {
            return node;
        }

        return !(Boolean) value;
    }

    @Override
    protected Object visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
    {
        Object left = process(node.getLeft(), context);
        Object right = process(node.getRight(), context);

        switch (node.getType()) {
            case AND: {
                // if either left or right is false, result is always false regardless of nulls
                if (Boolean.FALSE.equals(left) || Boolean.TRUE.equals(right)) {
                    return left;
                }

                if (Boolean.FALSE.equals(right) || Boolean.TRUE.equals(left)) {
                    return right;
                }
            }
            case OR: {
                // if either left or right is true, result is always true regardless of nulls
                if (Boolean.TRUE.equals(left) || Boolean.FALSE.equals(right)) {
                    return left;
                }

                if (Boolean.TRUE.equals(right) || Boolean.FALSE.equals(left)) {
                    return right;
                }
            }
        }

        return node;
    }

    @Override
    protected Object visitBooleanLiteral(BooleanLiteral node, Void context)
    {
        return node.equals(BooleanLiteral.TRUE_LITERAL);
    }

    @Override
    protected Object visitExpression(Expression node, Void context)
    {
        throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
    }

    @Override
    protected Object visitNode(Node node, Void context)
    {
        throw new UnsupportedOperationException("Evaluator visitor can only handle Expression nodes");
    }

    private static Expression toExpression(Object object)
    {
        if (object instanceof Expression) {
            return (Expression) object;
        }

        if (object instanceof Long) {
            return new LongLiteral(object.toString());
        }

        if (object instanceof Double) {
            return new DoubleLiteral(object.toString());
        }

        if (object instanceof Slice) {
            return new StringLiteral(((Slice) object).toString(Charsets.UTF_8));
        }

        throw new UnsupportedOperationException("not yet implemented");
    }
}
