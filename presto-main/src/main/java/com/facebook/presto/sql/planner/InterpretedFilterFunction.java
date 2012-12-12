package com.facebook.presto.sql.planner;

import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.tuple.TupleReadable;

import java.util.Map;

import static java.lang.Boolean.TRUE;

public class InterpretedFilterFunction
        implements FilterFunction
{
    private final Expression predicate;
    private final ChannelSymbolResolver resolver;
    private final ExpressionInterpreter evaluator;

    public InterpretedFilterFunction(Expression predicate, Map<Symbol, Integer> symbolToChannelMapping, Map<Symbol, Type> symbols)
    {
        this.predicate = predicate;
        resolver = new ChannelSymbolResolver(symbols, symbolToChannelMapping);
        evaluator = new ExpressionInterpreter(resolver);
    }

    @Override
    public boolean filter(TupleReadable... cursors)
    {
        resolver.setInputs(cursors);
        Object result = evaluator.process(predicate, null);
        return result == TRUE;
    }
}
