package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ChannelSymbolResolver
        implements SymbolResolver
{
    private final Map<Symbol, Integer> symbolToChannelMapping;
    private TupleReadable[] inputs;

    public ChannelSymbolResolver(Map<Symbol, Integer> symbolToChannelMapping)
    {
        Preconditions.checkNotNull(symbolToChannelMapping, "symbolToChannelMapping is null");

        this.symbolToChannelMapping = ImmutableMap.copyOf(symbolToChannelMapping);
    }

    public void setInputs(TupleReadable[] inputs)
    {
        this.inputs = inputs;
    }

    public Object getValue(Symbol symbol)
    {
        Integer channel = symbolToChannelMapping.get(symbol);
        TupleReadable input = inputs[channel];

        // TODO: support channels with composite tuples
        if (input.isNull(0)) {
            return null;
        }

        switch (input.getTupleInfo().getTypes().get(0)) {
            case FIXED_INT_64:
                return input.getLong(0);
            case DOUBLE:
                return input.getDouble(0);
            case VARIABLE_BINARY:
                return input.getSlice(0);
            default:
                throw new UnsupportedOperationException("not yet implemented");
        }
    }
}
