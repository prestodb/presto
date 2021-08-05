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

import com.facebook.presto.common.function.OperatorType;
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
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.expressions.translator.FunctionTranslator;
import com.facebook.presto.expressions.translator.RowExpressionTranslator;
import com.facebook.presto.expressions.translator.RowExpressionTreeTranslator;
import com.facebook.presto.expressions.translator.TranslatedExpression;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.expressions.translator.TranslatedExpression.untranslated;
import static com.facebook.presto.plugin.jdbc.optimization.function.JdbcTranslationUtil.mergeSqlBodies;
import static com.facebook.presto.plugin.jdbc.optimization.function.JdbcTranslationUtil.mergeVariableBindings;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JdbcFilterToSqlTranslator
        extends RowExpressionTranslator<JdbcExpression, Map<VariableReferenceExpression, ColumnHandle>>
{
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final FunctionTranslator<JdbcExpression> functionTranslator;
    private final StandardFunctionResolution standardFunctionResolution;
    private final String quote;

    public JdbcFilterToSqlTranslator(
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            FunctionTranslator<JdbcExpression> functionTranslator,
            StandardFunctionResolution standardFunctionResolution,
            String quote)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.functionTranslator = requireNonNull(functionTranslator, "functionTranslator is null");
        this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standardFunctionResolution is null");
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
            TranslatedExpression<JdbcExpression> translate = functionTranslator.translate(functionMetadata, call, translatedExpressions);
            if (translate.getTranslated().isPresent()) {
                return translate;
            }

            FunctionHandle functionHandle = call.getFunctionHandle();

            if (standardFunctionResolution.isCastFunction(functionHandle)) {
                return handleCast(call, context, rowExpressionTreeTranslator);
            }

            if (standardFunctionResolution.isBetweenFunction(functionHandle)) {
                return handleBetween(call, context, rowExpressionTreeTranslator);
            }

            Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
            if (operatorTypeOptional.isPresent()) {
                OperatorType operatorType = operatorTypeOptional.get();
                if (operatorType.isArithmeticOperator() || operatorType.isComparisonOperator()) {
                    if (operatorType == IS_DISTINCT_FROM) {
                        return untranslated(call);
                    }

                    List<JdbcExpression> translatedArguments = translatedExpressions.stream()
                            .map(TranslatedExpression::getTranslated)
                            .map(Optional::get)
                            .collect(toImmutableList());

                    if (translatedArguments.size() != 2) {
                        return untranslated(call, translatedExpressions);
                    }

                    List<String> sqlBodies = mergeSqlBodies(translatedArguments);
                    List<ConstantExpression> variableBindings = mergeVariableBindings(translatedArguments);
                    String arithmeticOperator = String.format(" %s ", operatorType.getOperator());
                    return new TranslatedExpression<>(
                            Optional.of(new JdbcExpression(format("%s", Joiner.on(arithmeticOperator).join(sqlBodies)), variableBindings)),
                            call,
                            translatedExpressions);
                }
            }
        }
        catch (Throwable t) {
            // no-op
        }
        return untranslated(call, translatedExpressions);
    }

    private TranslatedExpression<JdbcExpression> handleCast(CallExpression cast,
            Map<VariableReferenceExpression, ColumnHandle> context,
            RowExpressionTreeTranslator<JdbcExpression, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
    {
        if (cast.getArguments().size() == 1) {
            RowExpression input = cast.getArguments().get(0);
            Type expectedType = cast.getType();
            if (typeManager.canCoerce(input.getType(), expectedType)) {
                return rowExpressionTreeTranslator.rewrite(input, context);
            }
        }

        return untranslated(cast);
    }

    private TranslatedExpression<JdbcExpression> handleBetween(CallExpression between,
            Map<VariableReferenceExpression, ColumnHandle> context,
            RowExpressionTreeTranslator<JdbcExpression, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
    {
        if (between.getArguments().size() == 3) {
            List<TranslatedExpression<JdbcExpression>> translatedExpressions = between.getArguments().stream()
                    .map(expression -> rowExpressionTreeTranslator.rewrite(expression, context))
                    .collect(toImmutableList());

            List<JdbcExpression> jdbcExpressions = translatedExpressions.stream()
                    .map(TranslatedExpression::getTranslated)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toImmutableList());

            if (jdbcExpressions.size() < translatedExpressions.size()) {
                return untranslated(between, translatedExpressions);
            }

            List<String> sqlBodies = mergeSqlBodies(jdbcExpressions);
            List<ConstantExpression> variableBindings = mergeVariableBindings(jdbcExpressions);

            return new TranslatedExpression<>(
                    Optional.of(new JdbcExpression(format("(%s BETWEEN %s)", sqlBodies.get(0), Joiner.on(" AND ").join(sqlBodies.subList(1, sqlBodies.size()))), variableBindings)),
                    between,
                    translatedExpressions);
        }

        return untranslated(between);
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

        List<String> sqlBodies = mergeSqlBodies(jdbcExpressions);
        List<ConstantExpression> variableBindings = mergeVariableBindings(jdbcExpressions);

        switch (specialForm.getForm()) {
            case IS_NULL:
                return new TranslatedExpression<>(
                        Optional.of(new JdbcExpression(format("(%s IS NULL)", sqlBodies.get(0)))),
                        specialForm,
                        translatedExpressions);
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
