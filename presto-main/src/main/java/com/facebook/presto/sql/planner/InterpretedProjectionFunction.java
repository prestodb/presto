package com.facebook.presto.sql.planner;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.Input;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;

import java.util.Map;

public class InterpretedProjectionFunction
        implements ProjectionFunction
{
    private final Type type;
    private final Expression expression;
    private final TupleInputResolver resolver = new TupleInputResolver();
    private final ExpressionInterpreter evaluator;

    public InterpretedProjectionFunction(Type type, Expression expression, Map<Symbol, Input> symbolToInputMapping, Metadata metadata, Session session)
    {
        this.type = type;

        // pre-compute symbol -> input mappings and replace the corresponding nodes in the tree
        this.expression = TreeRewriter.rewriteWith(new SymbolToInputRewriter(symbolToInputMapping), expression);

        evaluator = ExpressionInterpreter.expressionInterpreter(resolver, metadata, session);
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return new TupleInfo(type.getRawType());
    }

    @Override
    public void project(TupleReadable[] cursors, BlockBuilder output)
    {
        resolver.setInputs(cursors);
        Object value = evaluator.process(expression, null);

        if (value == null) {
            output.appendNull();
        }
        else {
            switch (type) {
                case LONG:
                    output.append((Long) value);
                    break;
                case DOUBLE:
                    output.append((Double) value);
                    break;
                case STRING:
                    output.append((Slice) value);
                    break;
                default:
                    throw new UnsupportedOperationException("not yet implemented: " + type);
            }
        }
    }
}
