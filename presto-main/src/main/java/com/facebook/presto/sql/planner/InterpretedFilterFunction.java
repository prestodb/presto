package com.facebook.presto.sql.planner;

import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.sql.compiler.Slot;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.tuple.TupleReadable;

import java.util.Map;

import static java.lang.Boolean.TRUE;

public class InterpretedFilterFunction
        implements FilterFunction
{
    private final Expression predicate;
    private final Map<Slot, Integer> slotToChannelMapping;

    public InterpretedFilterFunction(Expression predicate, Map<Slot, Integer> slotToChannelMapping)
    {
        this.predicate = predicate;
        this.slotToChannelMapping = slotToChannelMapping;
    }

    @Override
    public boolean filter(TupleReadable... cursors)
    {
        ExpressionInterpreter evaluator = new ExpressionInterpreter(slotToChannelMapping);
        Boolean result = (Boolean) evaluator.process(predicate, cursors);
        return result == TRUE;
    }
}
