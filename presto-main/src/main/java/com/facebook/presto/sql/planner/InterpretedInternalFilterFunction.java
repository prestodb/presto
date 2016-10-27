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
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.InternalJoinFilterFunction;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;

public class InterpretedInternalFilterFunction
        implements FilterFunction, InternalJoinFilterFunction
{
    private final ExpressionInterpreter evaluator;
    private final Set<Integer> inputChannels;

    public InterpretedInternalFilterFunction(
            Expression predicate,
            Map<Symbol, Type> symbolTypes,
            Map<Symbol, Integer> symbolToInputMappings,
            Metadata metadata,
            SqlParser sqlParser,
            Session session)
    {
        // pre-compute symbol -> input mappings and replace the corresponding nodes in the tree
        Expression rewritten = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(symbolToInputMappings), predicate);

        // analyze expression so we can know the type of every expression in the tree
        ImmutableMap.Builder<Integer, Type> inputTypes = ImmutableMap.builder();
        for (Map.Entry<Symbol, Integer> entry : symbolToInputMappings.entrySet()) {
            inputTypes.put(entry.getValue(), symbolTypes.get(entry.getKey()));
        }
        IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypesFromInput(session, metadata, sqlParser, inputTypes.build(), rewritten, emptyList() /* parameters already rewritten */);

        evaluator = ExpressionInterpreter.expressionInterpreter(rewritten, metadata, session, expressionTypes);
        InputReferenceExtractor inputReferenceExtractor = new InputReferenceExtractor();
        inputReferenceExtractor.process(rewritten, null);
        this.inputChannels = ImmutableSet.copyOf(inputReferenceExtractor.getInputChannels());
    }

    @Override
    public boolean filter(int leftPosition, Block[] leftBlocks, int rightPosition, Block[] rightBlocks)
    {
        return evaluator.evaluate(leftPosition, leftBlocks, rightPosition, rightBlocks) == TRUE;
    }

    @Override
    public boolean filter(int position, Block... blocks)
    {
        return evaluator.evaluate(position, blocks) == TRUE;
    }

    @Override
    public boolean filter(RecordCursor cursor)
    {
        return evaluator.evaluate(cursor) == TRUE;
    }

    @Override
    public Set<Integer> getInputChannels()
    {
        return inputChannels;
    }
}
