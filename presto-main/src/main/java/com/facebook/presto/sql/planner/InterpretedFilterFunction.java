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
    private final Map<Symbol, Integer> symbolToChannelMapping;
    private final Map<Symbol, Type> symbols;

    public InterpretedFilterFunction(Expression predicate, Map<Symbol, Integer> symbolToChannelMapping, Map<Symbol, Type> symbols)
    {
        this.predicate = predicate;
        this.symbolToChannelMapping = symbolToChannelMapping;
        this.symbols = symbols;
    }

    @Override
    public boolean filter(TupleReadable... cursors)
    {
        ChannelSymbolResolver resolver = new ChannelSymbolResolver(symbols, symbolToChannelMapping, cursors);
        ExpressionInterpreter evaluator = new ExpressionInterpreter(resolver);
        Object result = evaluator.process(predicate, null);
        return result == TRUE;
    }
}
