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
package com.facebook.presto.plugin.jdbc.optimization;

import com.facebook.presto.expressions.translator.FunctionTranslator;
import com.facebook.presto.expressions.translator.RowExpressionTreeTranslator;
import com.facebook.presto.expressions.translator.TranslatedExpression;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JdbcJoinPredicateToSqlTranslator
        extends JdbcFilterToSqlTranslator
{
    private final FunctionMetadataManager functionMetadataManager;
    private final FunctionTranslator<JdbcExpression> functionTranslator;
    private final String quote;

    public JdbcJoinPredicateToSqlTranslator(FunctionMetadataManager functionMetadataManager, FunctionTranslator<JdbcExpression> functionTranslator, String quote)
    {
        super(functionMetadataManager, functionTranslator, quote);
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.functionTranslator = requireNonNull(functionTranslator, "functionTranslator is null");
        this.quote = requireNonNull(quote, "quote is null");
    }
    @Override
    public TranslatedExpression<JdbcExpression> translateVariable(VariableReferenceExpression variable, Map<VariableReferenceExpression, ColumnHandle> context, RowExpressionTreeTranslator<JdbcExpression, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
    {
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.get(variable);
        requireNonNull(columnHandle, format("Unrecognized variable %s", variable));
        StringBuilder expression = new StringBuilder();
        if (columnHandle.getTableAlias().isPresent()) {
            expression.append(quote).append(columnHandle.getTableAlias().get()).append(quote).append(".");
        }
        expression.append(quote).append(columnHandle.getColumnName()).append(quote);
        return new TranslatedExpression<>(
                Optional.of(new JdbcExpression(expression.toString())),
                variable,
                ImmutableList.of());
    }
}
