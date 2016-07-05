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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class InterpretedProjectionFunction
        implements ProjectionFunction
{
    private final Type type;
    private final ExpressionInterpreter evaluator;
    private final Set<Integer> inputChannels;
    private final boolean deterministic;

    public InterpretedProjectionFunction(
            Expression expression,
            Map<Symbol, Type> symbolTypes,
            Map<Symbol, Integer> symbolToInputMappings,
            Metadata metadata,
            SqlParser sqlParser,
            Session session)
    {
        // pre-compute symbol -> input mappings and replace the corresponding nodes in the tree
        Expression rewritten = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(symbolToInputMappings), expression);

        // analyze expression so we can know the type of every expression in the tree
        ImmutableMap.Builder<Integer, Type> inputTypes = ImmutableMap.builder();
        for (Map.Entry<Symbol, Integer> entry : symbolToInputMappings.entrySet()) {
            inputTypes.put(entry.getValue(), symbolTypes.get(entry.getKey()));
        }
        IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypesFromInput(session, metadata, sqlParser, inputTypes.build(), rewritten, emptyList() /* parameters already replaced */);
        this.type = requireNonNull(expressionTypes.get(rewritten), "type is null");

        evaluator = ExpressionInterpreter.expressionInterpreter(rewritten, metadata, session, expressionTypes);
        InputReferenceExtractor inputReferenceExtractor = new InputReferenceExtractor();
        inputReferenceExtractor.process(rewritten, null);
        this.inputChannels = inputReferenceExtractor.getInputChannels();
        this.deterministic = DeterminismEvaluator.isDeterministic(expression);
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public void project(int position, Block[] blocks, BlockBuilder output)
    {
        Object value = evaluator.evaluate(position, blocks);
        append(output, value);
    }

    @Override
    public void project(RecordCursor cursor, BlockBuilder output)
    {
        Object value = evaluator.evaluate(cursor);
        append(output, value);
    }

    @Override
    public Set<Integer> getInputChannels()
    {
        return inputChannels;
    }

    @Override
    public boolean isDeterministic()
    {
        return deterministic;
    }

    private void append(BlockBuilder output, Object value)
    {
        if (value == null) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            type.writeBoolean(output, (Boolean) value);
        }
        else if (javaType == long.class) {
            type.writeLong(output, (Long) value);
        }
        else if (javaType == double.class) {
            type.writeDouble(output, (Double) value);
        }
        else if (javaType == Slice.class) {
            Slice slice = (Slice) value;
            type.writeSlice(output, slice, 0, slice.length());
        }
        else {
            type.writeObject(output, value);
        }
    }
}
