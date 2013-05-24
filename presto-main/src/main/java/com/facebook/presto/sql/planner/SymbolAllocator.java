package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

public class SymbolAllocator
{
    private final Map<Symbol, Type> symbols = new HashMap<>();

    public Symbol newSymbol(String nameHint, Type type)
    {
        Preconditions.checkNotNull(nameHint, "name is null");

        // TODO: workaround for the fact that QualifiedName lowercases parts
        nameHint = nameHint.toLowerCase();

        if (nameHint.contains("_")) {
            nameHint = nameHint.substring(0, nameHint.indexOf("_"));
        }

        String unique = nameHint;

        int id = 1;
        while (symbols.containsKey(new Symbol(unique))) {
            unique = nameHint + "_"+ id;
            id++;
        }

        Symbol symbol = new Symbol(unique);
        symbols.put(symbol, type);
        return symbol;
    }

    public Symbol newSymbol(Expression expression, Type type)
    {
        String nameHint = "expr";
        if (expression instanceof QualifiedNameReference) {
            nameHint = ((QualifiedNameReference) expression).getName().getSuffix();
        }
        else if (expression instanceof FunctionCall) {
            nameHint = ((FunctionCall) expression).getName().getSuffix();
        }

        return newSymbol(nameHint, type);
    }

    public Symbol newSymbol(Field field)
    {
        String nameHint = field.getName().or("field");
        return newSymbol(nameHint, field.getType());
    }

    public Map<Symbol, Type> getTypes()
    {
        return symbols;
    }
}
