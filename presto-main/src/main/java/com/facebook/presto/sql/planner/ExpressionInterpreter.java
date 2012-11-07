package com.facebook.presto.sql.planner;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.sql.compiler.Slot;
import com.facebook.presto.sql.compiler.SlotReference;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;

import java.util.Map;

import static com.google.common.base.Charsets.UTF_8;
import static java.lang.String.format;

class ExpressionInterpreter
        extends AstVisitor<Object, BlockCursor[]>
{
    private final Map<Slot, Integer> slotToChannelMapping;

    public ExpressionInterpreter(Map<Slot, Integer> slotToChannelMapping)
    {
        this.slotToChannelMapping = slotToChannelMapping;
    }

    @Override
    public Object visitSlotReference(SlotReference node, BlockCursor[] inputs)
    {
        Slot slot = node.getSlot();
        Integer channel = slotToChannelMapping.get(slot);
        BlockCursor input = inputs[channel];

        // TODO: support channels with composite tuples
        switch (slot.getType()) {
            case LONG:
                return input.getLong(0);
            case DOUBLE:
                return input.getDouble(0);
            case STRING:
                return input.getSlice(0);
            default:
                throw new UnsupportedOperationException("not yet implemented: " + slot.getType());
        }
    }

    @Override
    protected Object visitQualifiedNameReference(QualifiedNameReference node, BlockCursor[] context)
    {
        throw new UnsupportedOperationException("QualifiedNames should be rewritten to slot references");
    }

    @Override
    protected Object visitLongLiteral(LongLiteral node, BlockCursor[] context)
    {
        return Long.valueOf(node.getValue());
    }

    @Override
    protected Object visitDoubleLiteral(DoubleLiteral node, BlockCursor[] context)
    {
        return Double.valueOf(node.getValue());
    }

    @Override
    protected Object visitStringLiteral(StringLiteral node, BlockCursor[] context)
    {
        return Slices.wrappedBuffer(node.getValue().getBytes(UTF_8));
    }

    @Override
    protected Object visitArithmeticExpression(ArithmeticExpression node, BlockCursor[] context)
    {
        Number left = (Number) process(node.getLeft(), context);
        Number right = (Number) process(node.getRight(), context);

        switch (node.getType()) {
            case ADD:
                if (left instanceof Long && right instanceof Long) {
                    return left.longValue() + right.longValue();
                }
                else {
                    return left.doubleValue() + right.doubleValue();
                }
            case SUBTRACT:
                if (left instanceof Long && right instanceof Long) {
                    return left.longValue() - right.longValue();
                }
                else {
                    return left.doubleValue() - right.doubleValue();
                }
            case DIVIDE:
                if (left instanceof Long && right instanceof Long) {
                    return left.longValue() / right.longValue();
                }
                else {
                    return left.doubleValue() / right.doubleValue();
                }
            case MULTIPLY:
                if (left instanceof Long && right instanceof Long) {
                    return left.longValue() * right.longValue();
                }
                else {
                    return left.doubleValue() * right.doubleValue();
                }
            default:
                throw new UnsupportedOperationException("not yet implemented: " + node.getType());
        }
    }

    @Override
    protected Object visitComparisonExpression(ComparisonExpression node, BlockCursor[] context)
    {
        Object left = process(node.getLeft(), context);
        Object right = process(node.getRight(), context);

        if (left instanceof Number && right instanceof Number) {
            switch (node.getType()) {
                case EQUAL:
                    return left.equals(right);
                case NOT_EQUAL:
                    return !left.equals(right);
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

        throw new UnsupportedOperationException(format("not yet implemented: %s(%s, %s)", node.getType(), left.getClass().getName(), right.getClass().getName()));
    }

    @Override
    protected Object visitLogicalBinaryExpression(LogicalBinaryExpression node, BlockCursor[] context)
    {
        Boolean left = (Boolean) process(node.getLeft(), context);
        Boolean right = (Boolean) process(node.getRight(), context);

        switch (node.getType()) {
            case AND:
                return left && right;
            case OR:
                return left || right;
        }

        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    protected Object visitExpression(Expression node, BlockCursor[] context)
    {
        throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
    }

    @Override
    protected Object visitNode(Node node, BlockCursor[] context)
    {
        throw new UnsupportedOperationException("Evaluator visitor can only handle Expression nodes");
    }
}
