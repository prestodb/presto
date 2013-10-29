/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    private final ExpressionInterpreter evaluator;

    public InterpretedProjectionFunction(Type type, Expression expression, Map<Symbol, Input> symbolToInputMapping, Metadata metadata, Session session)
    {
        this.type = type;

        // pre-compute symbol -> input mappings and replace the corresponding nodes in the tree
        Expression rewritten = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(symbolToInputMapping), expression);

        evaluator = ExpressionInterpreter.expressionInterpreter(rewritten, metadata, session);
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return new TupleInfo(type.getRawType());
    }

    @Override
    public void project(TupleReadable[] cursors, BlockBuilder output)
    {
        Object value = evaluator.evaluate(cursors);
        append(output, value);
    }

    @Override
    public void project(RecordCursor cursor, BlockBuilder output)
    {
        Object value = evaluator.evaluate(cursor);
        append(output, value);
    }

    private void append(BlockBuilder output, Object value)
    {
        if (value == null) {
            output.appendNull();
        }
        else {
            switch (type) {
                case BOOLEAN:
                    output.append((Boolean) value);
                    break;
                case BIGINT:
                    output.append((Long) value);
                    break;
                case DOUBLE:
                    output.append((Double) value);
                    break;
                case VARCHAR:
                    output.append((Slice) value);
                    break;
                default:
                    throw new UnsupportedOperationException("not yet implemented: " + type);
            }
        }
    }
}
