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

import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;

import java.util.function.Function;

import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.sql.planner.LiteralEncoder.toRowExpression;
import static java.util.Objects.requireNonNull;

public final class RowExpressionOptimizer
        implements ExpressionOptimizer
{
    private final FunctionAndTypeManager functionAndTypeManager;

    public RowExpressionOptimizer(Metadata metadata)
    {
        this(requireNonNull(metadata, "metadata is null").getFunctionAndTypeManager());
    }

    public RowExpressionOptimizer(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    @Override
    public RowExpression optimize(RowExpression rowExpression, Level level, ConnectorSession session)
    {
        if (level.ordinal() <= OPTIMIZED.ordinal()) {
            return toRowExpression(rowExpression.getSourceLocation(), new RowExpressionInterpreter(rowExpression, functionAndTypeManager, session, level).optimize(), rowExpression.getType());
        }
        throw new IllegalArgumentException("Not supported optimization level: " + level);
    }

    @Override
    public RowExpression optimize(RowExpression expression, Level level, ConnectorSession session, Function<VariableReferenceExpression, Object> variableResolver)
    {
        RowExpressionInterpreter interpreter = new RowExpressionInterpreter(expression, functionAndTypeManager, session, level);
        return toRowExpression(expression.getSourceLocation(), interpreter.optimize(variableResolver::apply), expression.getType());
    }
}
