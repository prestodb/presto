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
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.expressions.ExpressionManager;

import java.util.function.Function;

import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.DO_NOT_EVALUATE;
import static com.facebook.presto.sql.planner.LiteralEncoder.toRowExpression;
import static java.util.Objects.requireNonNull;

public final class DelegatingRowExpressionOptimizer
        implements ExpressionOptimizer
{
    private final ExpressionOptimizer inMemoryOptimizer;
    private final ExpressionOptimizer delegate;

    public DelegatingRowExpressionOptimizer(Metadata metadata, ExpressionManager expressionManager)
    {
        this(
                requireNonNull(metadata, "metadata is null"),
                requireNonNull(expressionManager, "expressionManager is null").getExpressionOptimizer());
    }

    public DelegatingRowExpressionOptimizer(Metadata metadata, ExpressionOptimizer delegate)
    {
        requireNonNull(metadata, "metadata is null");
        this.inMemoryOptimizer = new RowExpressionOptimizer(metadata);
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public RowExpression optimize(RowExpression rowExpression, Level level, ConnectorSession session)
    {
        RowExpression originalExpression;
        do {
            originalExpression = rowExpression;
            rowExpression = delegate.optimize(rowExpression, level, session);
            rowExpression = inMemoryOptimizer.optimize(rowExpression, DO_NOT_EVALUATE, session);
        } while (!originalExpression.equals(rowExpression));
        return rowExpression;
    }

    @Override
    public Object optimize(RowExpression rowExpression, Level level, ConnectorSession session, Function<VariableReferenceExpression, Object> variableResolver)
    {
        Object currentExpression = rowExpression;
        Object originalExpression;
        do {
            originalExpression = currentExpression;
            currentExpression = delegate.optimize(toRowExpression(currentExpression, rowExpression.getType()), level, session, variableResolver);
            currentExpression = inMemoryOptimizer.optimize(toRowExpression(currentExpression, rowExpression.getType()), DO_NOT_EVALUATE, session);
        } while (!originalExpression.equals(currentExpression));
        return currentExpression;
    }
}
