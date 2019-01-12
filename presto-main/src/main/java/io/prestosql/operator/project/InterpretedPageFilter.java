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
package io.prestosql.operator.project;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.DeterminismEvaluator;
import io.prestosql.sql.planner.ExpressionInterpreter;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolToInputParameterRewriter;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.NodeRef;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.List;
import java.util.Map;

import static io.prestosql.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
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
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypesFromInput(session, metadata, sqlParser, parameterTypes.build(), rewritten, emptyList(), WarningCollector.NOOP);
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
