package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.analyzer.Symbol;

public interface SymbolResolver
{
    Object getValue(Symbol symbol);
}
