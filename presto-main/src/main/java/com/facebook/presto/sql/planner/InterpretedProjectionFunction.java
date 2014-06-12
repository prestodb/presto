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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Input;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.util.IdentityHashMap;
import java.util.Map;

import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static com.google.common.base.Preconditions.checkNotNull;

public class InterpretedProjectionFunction
        implements ProjectionFunction
{
    private final Type type;
    private final ExpressionInterpreter evaluator;

    public InterpretedProjectionFunction(
            Expression expression,
            Map<Symbol, Type> symbolTypes,
            Map<Symbol, Input> symbolToInputMappings,
            Metadata metadata,
            SqlParser sqlParser,
            ConnectorSession session)
    {
        // pre-compute symbol -> input mappings and replace the corresponding nodes in the tree
        Expression rewritten = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(symbolToInputMappings), expression);

        // analyze expression so we can know the type of every expression in the tree
        ImmutableMap.Builder<Input, Type> inputTypes = ImmutableMap.builder();
        for (Map.Entry<Symbol, Input> entry : symbolToInputMappings.entrySet()) {
            inputTypes.put(entry.getValue(), symbolTypes.get(entry.getKey()));
        }
        IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypesFromInput(session, metadata, sqlParser, inputTypes.build(), rewritten);
        this.type = checkNotNull(expressionTypes.get(rewritten), "type is null");

        evaluator = ExpressionInterpreter.expressionInterpreter(rewritten, metadata, session, expressionTypes);
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
            output.appendBoolean((Boolean) value);
        }
        else if (javaType == long.class) {
            output.appendLong((Long) value);
        }
        else if (javaType == double.class) {
            output.appendDouble((Double) value);
        }
        else if (javaType == Slice.class) {
            output.appendSlice((Slice) value);
        }
        else {
            throw new UnsupportedOperationException("not yet implemented: " + type);
        }
    }
}
