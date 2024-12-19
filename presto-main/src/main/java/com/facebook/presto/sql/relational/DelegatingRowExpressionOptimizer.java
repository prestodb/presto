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
import com.facebook.presto.spi.relation.ExpressionOptimizerProvider;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.function.Function;

import static com.facebook.presto.sql.planner.LiteralEncoder.toRowExpression;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class DelegatingRowExpressionOptimizer
        implements ExpressionOptimizer
{
    private static final int DEFAULT_MAX_OPTIMIZATION_ATTEMPTS = 10;
    private final ExpressionOptimizerProvider expressionOptimizerManager;
    private final int maxOptimizationAttempts;

    public DelegatingRowExpressionOptimizer(Metadata metadata, ExpressionOptimizerProvider expressionOptimizerManager)
    {
        this(metadata, expressionOptimizerManager, DEFAULT_MAX_OPTIMIZATION_ATTEMPTS);
    }

    public DelegatingRowExpressionOptimizer(Metadata metadata, ExpressionOptimizerProvider expressionOptimizerManager, int maxOptimizationAttempts)
    {
        requireNonNull(metadata, "metadata is null");
        this.expressionOptimizerManager = requireNonNull(expressionOptimizerManager, "expressionOptimizerManager is null");
        checkArgument(maxOptimizationAttempts > 0, "maxOptimizationAttempts must be greater than 0");
        this.maxOptimizationAttempts = maxOptimizationAttempts;
    }

    @Override
    public RowExpression optimize(RowExpression rowExpression, Level level, ConnectorSession session)
    {
        ExpressionOptimizer delegate = expressionOptimizerManager.getExpressionOptimizer(session);
        RowExpression originalExpression;
        for (int i = 0; i < maxOptimizationAttempts; i++) {
            // Do not optimize ConstantExpression, and InputReferenceExpression because they cannot be optimized further
            if (rowExpression instanceof ConstantExpression || rowExpression instanceof InputReferenceExpression) {
                return rowExpression;
            }
            originalExpression = rowExpression;
            rowExpression = delegate.optimize(rowExpression, level, session);
            requireNonNull(rowExpression, "optimized expression is null");
            if (originalExpression.equals(rowExpression)) {
                break;
            }
        }
        return rowExpression;
    }

    @Override
    public Object optimize(RowExpression rowExpression, Level level, ConnectorSession session, Function<VariableReferenceExpression, Object> variableResolver)
    {
        ExpressionOptimizer delegate = expressionOptimizerManager.getExpressionOptimizer(session);
        Object currentExpression = rowExpression;
        Object originalExpression;
        for (int i = 0; i < maxOptimizationAttempts; i++) {
            // Do not optimize ConstantExpression, and InputReferenceExpression because they cannot be optimized further
            if (currentExpression instanceof ConstantExpression || currentExpression instanceof InputReferenceExpression) {
                return currentExpression;
            }
            originalExpression = currentExpression;
            currentExpression = delegate.optimize(toRowExpression(currentExpression, rowExpression.getType()), level, session, variableResolver);
            if (currentExpression == null || currentExpression.equals(originalExpression)) {
                break;
            }
        }
        return currentExpression;
    }
}
