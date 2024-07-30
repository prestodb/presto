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
package com.facebook.presto.sql.relational;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.expressions.ExpressionOptimizerProvider;

import java.util.function.Function;

import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.DO_NOT_EVALUATE;
import static com.facebook.presto.sql.planner.LiteralEncoder.toRowExpression;
import static java.util.Objects.requireNonNull;

public final class DelegatingRowExpressionOptimizer
        implements ExpressionOptimizer
{
    private static final int MAX_OPTIMIZATION_ATTEMPTS = 10;
    private final ExpressionOptimizerProvider expressionOptimizerManager;
    private final ExpressionOptimizer inMemoryOptimizer;

    public DelegatingRowExpressionOptimizer(Metadata metadata, ExpressionOptimizerProvider expressionOptimizerManager)
    {
        requireNonNull(metadata, "metadata is null");
        this.expressionOptimizerManager = requireNonNull(expressionOptimizerManager, "expressionOptimizerManager is null");
        this.inMemoryOptimizer = new RowExpressionOptimizer(metadata);
    }

    @Override
    public RowExpression optimize(RowExpression rowExpression, Level level, ConnectorSession session)
    {
        ExpressionOptimizer delegate = expressionOptimizerManager.getExpressionOptimizer();
        RowExpression originalExpression;
        for (int i = 0; i < MAX_OPTIMIZATION_ATTEMPTS; i++) {
            // Do not optimize VariableReferenceExpression, ConstantExpression, and InputReferenceExpression because they cannot be optimized further
            if (rowExpression instanceof VariableReferenceExpression || rowExpression instanceof ConstantExpression || rowExpression instanceof InputReferenceExpression) {
                return rowExpression;
            }
            originalExpression = rowExpression;
            rowExpression = delegate.optimize(rowExpression, level, session);
            rowExpression = inMemoryOptimizer.optimize(rowExpression, DO_NOT_EVALUATE, session);
            if (originalExpression.equals(rowExpression)) {
                break;
            }
        }
        return rowExpression;
    }

    @Override
    public Object optimize(RowExpression rowExpression, Level level, ConnectorSession session, Function<VariableReferenceExpression, Object> variableResolver)
    {
        ExpressionOptimizer delegate = expressionOptimizerManager.getExpressionOptimizer();
        Object currentExpression = rowExpression;
        Object originalExpression;
        for (int i = 0; i < MAX_OPTIMIZATION_ATTEMPTS; i++) {
            // Do not optimize VariableReferenceExpression, ConstantExpression, and InputReferenceExpression because they cannot be optimized further
            if (currentExpression instanceof VariableReferenceExpression || currentExpression instanceof ConstantExpression || currentExpression instanceof InputReferenceExpression) {
                return currentExpression;
            }
            originalExpression = currentExpression;
            currentExpression = delegate.optimize(toRowExpression(currentExpression, rowExpression.getType()), level, session, variableResolver);
            currentExpression = inMemoryOptimizer.optimize(toRowExpression(currentExpression, rowExpression.getType()), DO_NOT_EVALUATE, session);
            if (originalExpression.equals(currentExpression)) {
                break;
            }
        }
        return currentExpression;
    }
}
