package com.facebook.presto.sql.planner;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import io.airlift.slice.Slice;

import java.util.Map;

public class InterpretedProjectionFunction
        implements ProjectionFunction
{
    private final Type type;
    private final Expression expression;
    private final TupleInputResolver resolver = new TupleInputResolver();
    private final ExpressionInterpreter evaluator;

    private final Map<Input, Type> inputTypes;

    public InterpretedProjectionFunction(Type type, Expression expression, Map<Symbol, Input> symbolToInputMapping, Metadata metadata, Session session, Map<Input, Type> inputTypes)
    {
        this.type = type;

        // pre-compute symbol -> input mappings and replace the corresponding nodes in the tree
        this.expression = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(symbolToInputMapping), expression);

        evaluator = ExpressionInterpreter.expressionInterpreter(resolver, metadata, session);

        this.inputTypes = inputTypes;
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
        project(output);
    }

    @Override
    public void project(RecordCursor cursor, BlockBuilder output)
    {
        resolver.setCursor(cursor, inputTypes);
        project(output);
    }

    private void project(BlockBuilder output)
    {
        Object value = evaluator.process(expression, null);

        if (value == null) {
            output.appendNull();
        }
        else {
            switch (type) {
                case BOOLEAN:
                    output.append((Boolean) value);
                    break;
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
