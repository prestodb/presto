package com.facebook.presto.sql.planner;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.sql.compiler.Slot;
import com.facebook.presto.sql.compiler.Type;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;

import java.util.Map;

public class InterpretedProjectionFunction
        implements ProjectionFunction
{
    private final Type type;
    private final Expression expression;
    private final Map<Slot, Integer> slotToChannelMappings;

    public InterpretedProjectionFunction(Type type, Expression expression, Map<Slot, Integer> slotToChannelMappings)
    {
        this.type = type;
        this.expression = expression;
        this.slotToChannelMappings = slotToChannelMappings;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return new TupleInfo(type.getRawType());
    }

    @Override
    public void project(TupleReadable[] cursors, BlockBuilder output)
    {
        ExpressionInterpreter evaluator = new ExpressionInterpreter(slotToChannelMappings);
        Object value = evaluator.process(expression, cursors);

        if (value == null) {
            output.appendNull();
        }
        else {
            switch (type) {
                case LONG:
                    output.append((Long) value);
                    break;
                case DOUBLE:
                    output.append((Double) value);
                    break;
                case STRING:
                    output.append((Slice) value);
                    break;
                default:
                    throw new UnsupportedOperationException("not yet implemented: " + type);
            }
        }
    }
}
