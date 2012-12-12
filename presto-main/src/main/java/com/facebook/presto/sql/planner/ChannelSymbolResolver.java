package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ChannelSymbolResolver
        implements SymbolResolver
{
    private final Map<Symbol, Type> symbols;
    private final Map<Symbol, Integer> symbolToChannelMapping;
    private TupleReadable[] inputs;

    public ChannelSymbolResolver(Map<Symbol, Type> symbols, Map<Symbol, Integer> symbolToChannelMapping)
    {
        checkNotNull(symbols, "symbols is null");
        Preconditions.checkNotNull(symbolToChannelMapping, "symbolToChannelMapping is null");

        this.symbols = ImmutableMap.copyOf(symbols);
        this.symbolToChannelMapping = ImmutableMap.copyOf(symbolToChannelMapping);
    }

    public void setInputs(TupleReadable[] inputs)
    {
        this.inputs = inputs;
    }

    public Object getValue(Symbol symbol)
    {
        checkState(symbols.containsKey(symbol), "Unknown symbol: %s", symbol);

        Integer channel = symbolToChannelMapping.get(symbol);
        checkState(channel != null, "Unknown channel for symbol: %s", symbol);

        TupleReadable input = inputs[channel];

        // TODO: support channels with composite tuples
        if (input.isNull(0)) {
            return null;
        }

        switch (symbols.get(symbol)) {
            case LONG:
                return input.getLong(0);
            case DOUBLE:
                return input.getDouble(0);
            case STRING:
                return input.getSlice(0);
            default:
                throw new UnsupportedOperationException("not yet implemented: " + symbols.get(symbol));
        }
    }
}
