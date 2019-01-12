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
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.Work;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
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

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.type.TypeUtils.writeNativeValue;
import static io.prestosql.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class InterpretedPageProjection
        implements PageProjection
{
    private final ExpressionInterpreter evaluator;
    private final InputChannels inputChannels;
    private final boolean deterministic;
    private BlockBuilder blockBuilder;

    public InterpretedPageProjection(
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

        blockBuilder = evaluator.getType().createBlockBuilder(null, 1);
    }

    @Override
    public Type getType()
    {
        return evaluator.getType();
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
    public Work<Block> project(ConnectorSession session, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
    {
        return new InterpretedPageProjectionWork(yieldSignal, page, selectedPositions);
    }

    private class InterpretedPageProjectionWork
            implements Work<Block>
    {
        private final DriverYieldSignal yieldSignal;
        private final Page page;
        private final SelectedPositions selectedPositions;

        private int nextIndexOrPosition;
        private Block result;

        public InterpretedPageProjectionWork(DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
        {
            this.yieldSignal = requireNonNull(yieldSignal, "yieldSignal is null");
            this.page = requireNonNull(page, "page is null");
            this.selectedPositions = requireNonNull(selectedPositions, "selectedPositions is null");
            this.nextIndexOrPosition = selectedPositions.getOffset();
        }

        @Override
        public boolean process()
        {
            checkState(result == null, "result has been generated");
            int length = selectedPositions.getOffset() + selectedPositions.size();
            if (selectedPositions.isList()) {
                int[] positions = selectedPositions.getPositions();
                while (nextIndexOrPosition < length) {
                    writeNativeValue(evaluator.getType(), blockBuilder, evaluator.evaluate(positions[nextIndexOrPosition], page));
                    nextIndexOrPosition++;
                    if (yieldSignal.isSet()) {
                        return false;
                    }
                }
            }
            else {
                while (nextIndexOrPosition < length) {
                    writeNativeValue(evaluator.getType(), blockBuilder, evaluator.evaluate(nextIndexOrPosition, page));
                    nextIndexOrPosition++;
                    if (yieldSignal.isSet()) {
                        return false;
                    }
                }
            }

            result = blockBuilder.build();
            blockBuilder = blockBuilder.newBlockBuilderLike(null);
            return true;
        }

        @Override
        public Block getResult()
        {
            checkState(result != null, "result has not been generated");
            return result;
        }
    }
}
