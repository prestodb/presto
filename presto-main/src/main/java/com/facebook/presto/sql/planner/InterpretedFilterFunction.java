package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.facebook.presto.tuple.TupleReadable;

import java.util.Map;

import static java.lang.Boolean.TRUE;

public class InterpretedFilterFunction
        implements FilterFunction
{
    private final Expression predicate;
    private final TupleInputResolver resolver = new TupleInputResolver();
    private final ExpressionInterpreter evaluator;

    public InterpretedFilterFunction(Expression predicate, Map<Symbol, Input> symbolToInputMappings, Metadata metadata, Session session)
    {
        // pre-compute symbol -> input mappings and replace the corresponding nodes in the tree
        this.predicate = TreeRewriter.rewriteWith(new SymbolToInputRewriter(symbolToInputMappings), predicate);
        evaluator = ExpressionInterpreter.expressionInterpreter(resolver, metadata, session);
    }

    @Override
    public boolean filter(TupleReadable... cursors)
    {
        resolver.setInputs(cursors);
        Object result = evaluator.process(predicate, null);
        return result == TRUE;
    }
}
