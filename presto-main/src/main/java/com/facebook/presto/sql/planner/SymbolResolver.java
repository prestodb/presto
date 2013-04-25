package com.facebook.presto.sql.planner;

public interface SymbolResolver
{
    Object getValue(Symbol symbol);
}
