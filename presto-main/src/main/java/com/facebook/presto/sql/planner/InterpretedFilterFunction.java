package com.facebook.presto.sql.planner;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.sql.compiler.Slot;
import com.facebook.presto.sql.tree.Expression;

import java.util.Map;

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
    public boolean filter(BlockCursor[] cursors)
    {
        ExpressionInterpreter evaluator = new ExpressionInterpreter(slotToChannelMapping);
        return (Boolean) evaluator.process(predicate, cursors);
    }

}
