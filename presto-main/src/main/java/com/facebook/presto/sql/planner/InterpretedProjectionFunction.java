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
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.type.Type;
import io.airlift.slice.Slice;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class InterpretedProjectionFunction
        implements ProjectionFunction
{
    private final Type type;
    private final ExpressionInterpreter evaluator;

    public InterpretedProjectionFunction(Type type, Expression expression, Map<Symbol, Input> symbolToInputMapping, Metadata metadata, Session session)
    {
        this.type = checkNotNull(type, "type is null");

        // pre-compute symbol -> input mappings and replace the corresponding nodes in the tree
        Expression rewritten = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(symbolToInputMapping), expression);

        evaluator = ExpressionInterpreter.expressionInterpreter(rewritten, metadata, session);
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public void project(BlockCursor[] cursors, BlockBuilder output)
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
            return;
        }

        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            output.append((Boolean) value);
        }
        else if (javaType == long.class) {
            output.append((Long) value);
        }
        else if (javaType == double.class) {
            output.append((Double) value);
        }
        else if (javaType == Slice.class) {
            output.append((Slice) value);
        }
        else {
            throw new UnsupportedOperationException("not yet implemented: " + type);
        }
    }
}
