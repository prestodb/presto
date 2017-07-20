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
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolToInputParameterRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.expressionInterpreter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class InterpretedCursorProcessor
        implements CursorProcessor
{
    @Nullable
    private final ExpressionInterpreter filter;
    private final List<ExpressionInterpreter> projections;
    private final List<Type> types;

    public InterpretedCursorProcessor(
            Optional<Expression> filter,
            List<Expression> projections,
            Map<Symbol, Type> symbolTypes,
            Map<Symbol, Integer> symbolToInputMappings,
            Metadata metadata,
            SqlParser sqlParser,
            Session session)
    {
        this.filter = filter.map(expression -> getExpressionInterpreter(expression, symbolTypes, symbolToInputMappings, metadata, sqlParser, session)).orElse(null);
        this.projections = projections.stream()
                .map(expression -> getExpressionInterpreter(expression, symbolTypes, symbolToInputMappings, metadata, sqlParser, session))
                .collect(toImmutableList());
        this.types = this.projections.stream()
                .map(ExpressionInterpreter::getType)
                .collect(toImmutableList());
    }

    private static ExpressionInterpreter getExpressionInterpreter(
            Expression expression,
            Map<Symbol, Type> symbolTypes,
            Map<Symbol, Integer> symbolToInputMappings,
            Metadata metadata,
            SqlParser sqlParser,
            Session session)
    {
        SymbolToInputParameterRewriter rewriter = new SymbolToInputParameterRewriter(symbolTypes, symbolToInputMappings);
        Expression rewritten = rewriter.rewrite(expression);

        // analyze rewritten expression so we can know the type of every expression in the tree
        List<Type> inputTypes = rewriter.getInputTypes();
        ImmutableMap.Builder<Integer, Type> parameterTypes = ImmutableMap.builder();
        for (int parameter = 0; parameter < inputTypes.size(); parameter++) {
            Type type = inputTypes.get(parameter);
            parameterTypes.put(parameter, type);
        }

        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypesFromInput(session, metadata, sqlParser, parameterTypes.build(), rewritten, emptyList());
        return expressionInterpreter(rewritten, metadata, session, expressionTypes);
    }

    @Override
    public CursorProcessorOutput process(ConnectorSession session, DriverYieldSignal yieldSignal, RecordCursor cursor, PageBuilder pageBuilder)
    {
        checkArgument(!pageBuilder.isFull(), "page builder can't be full");
        requireNonNull(yieldSignal, "yieldSignal is null");

        int position = 0;
        while (true) {
            if (pageBuilder.isFull() || yieldSignal.isSet()) {
                return new CursorProcessorOutput(position, false);
            }

            if (!cursor.advanceNextPosition()) {
                return new CursorProcessorOutput(position, true);
            }

            if (filter(cursor)) {
                pageBuilder.declarePosition();
                for (int channel = 0; channel < projections.size(); channel++) {
                    project(cursor, channel, pageBuilder);
                }
            }
            position++;
        }
    }

    private boolean filter(RecordCursor cursor)
    {
        return filter == null || TRUE.equals(filter.evaluate(cursor));
    }

    private void project(RecordCursor cursor, int channel, PageBuilder pageBuilder)
    {
        ExpressionInterpreter projection = projections.get(channel);
        Object value = projection.evaluate(cursor);
        writeNativeValue(types.get(channel), pageBuilder.getBlockBuilder(channel), value);
    }
}
