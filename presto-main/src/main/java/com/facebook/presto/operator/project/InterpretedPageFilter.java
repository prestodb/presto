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
package com.facebook.presto.operator.project;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DeterminismEvaluator;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolToInputParameterRewriter;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;

@NotThreadSafe
public class InterpretedPageFilter
        implements PageFilter
{
    private final ExpressionInterpreter evaluator;
    private final InputChannels inputChannels;
    private final boolean deterministic;
    private boolean[] selectedPositions = new boolean[0];

    public InterpretedPageFilter(
            Expression expression,
            TypeProvider symbolTypes,
            Map<Symbol, Integer> symbolToInputMappings,
            Metadata metadata,
            SqlParser sqlParser,
            Session session)
    {
        SymbolToInputParameterRewriter rewriter = new SymbolToInputParameterRewriter(symbolTypes, symbolToInputMappings);
        Expression rewritten = rewriter.rewrite(expression);
        this.inputChannels = new InputChannels(rewriter.getInputChannels());
        this.deterministic = DeterminismEvaluator.isDeterministic(expression);

        // analyze rewritten expression so we can know the type of every expression in the tree
        List<Type> inputTypes = rewriter.getInputTypes();
        ImmutableMap.Builder<Integer, Type> parameterTypes = ImmutableMap.builder();
        for (int parameter = 0; parameter < inputTypes.size(); parameter++) {
            Type type = inputTypes.get(parameter);
            parameterTypes.put(parameter, type);
        }
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypesFromInput(session, metadata, sqlParser, parameterTypes.build(), rewritten, emptyList());
        this.evaluator = ExpressionInterpreter.expressionInterpreter(rewritten, metadata, session, expressionTypes);
    }

    @Override
    public boolean isDeterministic()
    {
        return deterministic;
    }

    @Override
    public InputChannels getInputChannels()
    {
        return inputChannels;
    }

    @Override
    public SelectedPositions filter(ConnectorSession session, Page page)
    {
        if (selectedPositions.length < page.getPositionCount()) {
            selectedPositions = new boolean[page.getPositionCount()];
        }

        for (int position = 0; position < page.getPositionCount(); position++) {
            selectedPositions[position] = filter(page, position);
        }

        return PageFilter.positionsArrayToSelectedPositions(selectedPositions, page.getPositionCount());
    }

    private boolean filter(Page page, int position)
    {
        return TRUE.equals(evaluator.evaluate(position, page));
    }
}
