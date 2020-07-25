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

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimeWithTimeZoneType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.expressions.translator.FunctionTranslator;
import com.facebook.presto.expressions.translator.RowExpressionTranslator;
import com.facebook.presto.expressions.translator.RowExpressionTreeTranslator;
import com.facebook.presto.expressions.translator.TranslatedExpression;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.expressions.translator.TranslatedExpression.untranslated;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JdbcFilterToSqlTranslator
        extends RowExpressionTranslator<JdbcExpression, Map<VariableReferenceExpression, ColumnHandle>>
{
    private final FunctionMetadataManager functionMetadataManager;
    private final FunctionTranslator<JdbcExpression> functionTranslator;
    private final String quote;

    public JdbcFilterToSqlTranslator(FunctionMetadataManager functionMetadataManager, FunctionTranslator<JdbcExpression> functionTranslator, String quote)
    {
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.functionTranslator = requireNonNull(functionTranslator, "functionTranslator is null");
        this.quote = requireNonNull(quote, "quote is null");
    }

    @Override
    public TranslatedExpression<JdbcExpression> translateConstant(ConstantExpression literal, Map<VariableReferenceExpression, ColumnHandle> context, RowExpressionTreeTranslator<JdbcExpression, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
    {
        if (isSupportedType(literal.getType())) {
            return new TranslatedExpression<>(
                    Optional.of(new JdbcExpression("?", ImmutableList.of(literal))),
                    literal,
                    ImmutableList.of());
        }
        return untranslated(literal);
    }

    @Override
    public TranslatedExpression<JdbcExpression> translateVariable(VariableReferenceExpression variable, Map<VariableReferenceExpression, ColumnHandle> context, RowExpressionTreeTranslator<JdbcExpression, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
    {
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.get(variable);
        requireNonNull(columnHandle, format("Unrecognized variable %s", variable));
        return new TranslatedExpression<>(
                Optional.of(new JdbcExpression(quote + columnHandle.getColumnName().replace(quote, quote + quote) + quote)),
                variable,
                ImmutableList.of());
    }

    @Override
    public TranslatedExpression<JdbcExpression> translateLambda(LambdaDefinitionExpression lambda, Map<VariableReferenceExpression, ColumnHandle> context, RowExpressionTreeTranslator<JdbcExpression, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
    {
        return untranslated(lambda);
    }

    @Override
    public TranslatedExpression<JdbcExpression> translateCall(CallExpression call, Map<VariableReferenceExpression, ColumnHandle> context, RowExpressionTreeTranslator<JdbcExpression, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
    {
        List<TranslatedExpression<JdbcExpression>> translatedExpressions = call.getArguments().stream()
                .map(expression -> rowExpressionTreeTranslator.rewrite(expression, context))
                .collect(toImmutableList());

        FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(call.getFunctionHandle());

        try {
            return functionTranslator.translate(functionMetadata, call, translatedExpressions);
        }
        catch (Throwable t) {
            // no-op
        }
        return untranslated(call, translatedExpressions);
    }

    @Override
    public TranslatedExpression<JdbcExpression> translateSpecialForm(SpecialFormExpression specialForm, Map<VariableReferenceExpression, ColumnHandle> context, RowExpressionTreeTranslator<JdbcExpression, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
    {
        List<TranslatedExpression<JdbcExpression>> translatedExpressions = specialForm.getArguments().stream()
                .map(expression -> rowExpressionTreeTranslator.rewrite(expression, context))
                .collect(toImmutableList());

        List<JdbcExpression> jdbcExpressions = translatedExpressions.stream()
                .map(TranslatedExpression::getTranslated)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());

        if (jdbcExpressions.size() < translatedExpressions.size()) {
            return untranslated(specialForm, translatedExpressions);
        }

        List<String> sqlBodies = jdbcExpressions.stream()
                .map(JdbcExpression::getExpression)
                .map(sql -> '(' + sql + ')')
                .collect(toImmutableList());
        List<ConstantExpression> variableBindings = jdbcExpressions.stream()
                .map(JdbcExpression::getBoundConstantValues)
                .flatMap(List::stream)
                .collect(toImmutableList());

        switch (specialForm.getForm()) {
            case AND:
                return new TranslatedExpression<>(
                        Optional.of(new JdbcExpression(format("(%s)", Joiner.on(" AND ").join(sqlBodies)), variableBindings)),
                        specialForm,
                        translatedExpressions);
            case OR:
                return new TranslatedExpression<>(
                        Optional.of(new JdbcExpression(format("(%s)", Joiner.on(" OR ").join(sqlBodies)), variableBindings)),
                        specialForm,
                        translatedExpressions);
            case IN:
                return new TranslatedExpression<>(
                        Optional.of(new JdbcExpression(format("(%s IN (%s))", sqlBodies.get(0), Joiner.on(" , ").join(sqlBodies.subList(1, sqlBodies.size()))), variableBindings)),
                        specialForm,
                        translatedExpressions);
        }
        return untranslated(specialForm, translatedExpressions);
    }

    private static boolean isSupportedType(Type type)
    {
        Type validType = requireNonNull(type, "type is null");
        return validType.equals(BigintType.BIGINT) ||
                validType.equals(TinyintType.TINYINT) ||
                validType.equals(SmallintType.SMALLINT) ||
                validType.equals(IntegerType.INTEGER) ||
                validType.equals(DoubleType.DOUBLE) ||
                validType.equals(RealType.REAL) ||
                validType.equals(BooleanType.BOOLEAN) ||
                validType.equals(DateType.DATE) ||
                validType.equals(TimeType.TIME) ||
                validType.equals(TimeWithTimeZoneType.TIME_WITH_TIME_ZONE) ||
                validType.equals(TimestampType.TIMESTAMP) ||
                validType.equals(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE) ||
                validType instanceof VarcharType ||
                validType instanceof CharType;
    }
}
