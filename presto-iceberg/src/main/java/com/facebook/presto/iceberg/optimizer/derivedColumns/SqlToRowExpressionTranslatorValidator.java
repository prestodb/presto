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

package com.facebook.presto.iceberg.optimizer.derivedColumns;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalParseResult;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeUtils;
import com.facebook.presto.common.type.UnknownType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExistsExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression.Form;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.AtTimeZone;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CharLiteral;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.EnumLiteral;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.WhenClause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.common.function.OperatorType.BETWEEN;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.NEGATION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.common.type.Decimals.rescale;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.SWITCH;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.WHEN;
import static com.facebook.presto.util.DateTimeUtils.parseDate;
import static com.facebook.presto.util.DateTimeUtils.parseDayTimeInterval;
import static com.facebook.presto.util.DateTimeUtils.parseTimeWithTimeZone;
import static com.facebook.presto.util.DateTimeUtils.parseTimeWithoutTimeZone;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampLiteral;
import static com.facebook.presto.util.DateTimeUtils.parseYearMonthInterval;
import static com.facebook.presto.util.DateTimeUtils.timeHasTimeZone;
import static com.facebook.presto.util.DateTimeUtils.timestampHasTimeZone;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;

// The code is adapted from com.facebook.presto.sql.relational.SqlToRowExpressionTranslator
// In addition to sql to -> row expressions and Apply validation applicable to derived column expressions.
public class SqlToRowExpressionTranslatorValidator
        extends AstVisitor<RowExpression, Map<String, ColumnMetadata>>
{
    public static final Map<String, OperatorType> COMPARISON_OPERATORS =
            Stream.of(OperatorType.values()).filter(OperatorType::isComparisonOperator).collect(Collectors.toMap(OperatorType::getOperator, y -> y));
    public static final Map<String, OperatorType> ARITHMETIC_OPERATORS =
            Stream.of(OperatorType.values()).filter(OperatorType::isArithmeticOperator).collect(Collectors.toMap(OperatorType::getOperator, y -> y));
    public static final String CAST_NAME = "CAST";
    private static final String TRY_CAST_NAME = "TRY_CAST";
    private final StandardFunctionResolution functionResolution;
    private final FunctionMetadataManager functionMetadataManager;
    private final TypeManager typeManager;
    private final SqlFunctionProperties sqlFunctionProperties;

    public SqlToRowExpressionTranslatorValidator(StandardFunctionResolution functionResolution, FunctionMetadataManager functionMetadataManager, TypeManager typeManager, SqlFunctionProperties sqlFunctionProperties)
    {
        this.functionResolution = functionResolution;
        this.functionMetadataManager = functionMetadataManager;
        this.typeManager = typeManager;
        this.sqlFunctionProperties = sqlFunctionProperties;
    }

    @Override
    protected RowExpression visitIsNotNullPredicate(IsNotNullPredicate node, Map<String, ColumnMetadata> context)
    {
        return new CallExpression("not", functionResolution.notFunction(), BOOLEAN, ImmutableList.of(process(node.getValue(), context)));
    }

    @Override
    protected RowExpression visitIsNullPredicate(IsNullPredicate node, Map<String, ColumnMetadata> context)
    {
        RowExpression expression = process(node.getValue(), context);

        return new SpecialFormExpression(IS_NULL, BOOLEAN, expression);
    }

    @Override
    protected RowExpression visitArithmeticBinary(ArithmeticBinaryExpression node, Map<String, ColumnMetadata> context)
    {
        RowExpression leftRowExpression = process(node.getLeft(), context);
        RowExpression rightRowExpression = process(node.getRight(), context);
        RewrittenRowExpressions rewrittenRowExpressions = addRelevantCasts(leftRowExpression, rightRowExpression);
        checkState(rewrittenRowExpressions.leftRowExpression.getType().getTypeSignature().getBase()
                        .equals(rewrittenRowExpressions.rightRowExpression.getType().getTypeSignature().getBase()),
                format("Types on expression %s are not same %s != %s", node, leftRowExpression.getType(), rightRowExpression.getType()));
        if (ARITHMETIC_OPERATORS.containsKey(node.getOperator().getValue())) {
            OperatorType operatorType = ARITHMETIC_OPERATORS.get(node.getOperator().getValue());
            FunctionHandle functionHandle = functionResolution.arithmeticFunction(operatorType, rewrittenRowExpressions.leftRowExpression().getType(),
                    rewrittenRowExpressions.rightRowExpression().getType());
            if (functionHandle.getReturnType().isPresent()) {
                TypeSignature typeSignature = functionHandle.getReturnType().get();
                return new CallExpression(operatorType.name(),
                        functionHandle,
                        typeManager.getType(typeSignature),
                        List.of(rewrittenRowExpressions.leftRowExpression(), rewrittenRowExpressions.rightRowExpression()));
            }
        }
        throw new UnsupportedOperationException(format("Unsupported binary operator: %s", node.getOperator()));
    }

    @Override
    protected RowExpression visitCast(Cast node, Map<String, ColumnMetadata> context)
    {
        RowExpression value = process(node.getExpression(), context);
        if (value instanceof ConstantExpression && ((ConstantExpression) value).getValue() == null) {
            return nullConst(typeManager.getType(parseTypeSignature(node.getType())));
        }
        Type targetType = typeManager.getType(parseTypeSignature(node.getType()));
        if (node.isSafe()) {
            return new CallExpression(Optional.empty(), TRY_CAST_NAME,
                    functionResolution.lookupCast("TRY_CAST", value.getType(), targetType), targetType, List.of(value));
        }

        return new CallExpression(Optional.empty(), CAST_NAME,
                functionResolution.lookupCast(CAST_NAME, value.getType(), targetType), targetType, List.of(value));
    }

    @Override
    protected RowExpression visitLogicalBinaryExpression(LogicalBinaryExpression node, Map<String, ColumnMetadata> context)
    {
        RowExpression leftExpression = process(node.getLeft(), context);
        RowExpression rightExpression = process(node.getRight(), context);
        checkState(leftExpression.getType().getTypeSignature().getBase().equals(rightExpression.getType().getTypeSignature().getBase()),
                format("left side expression : %s return type should match with right side expression : %s", leftExpression, rightExpression));
        Form form = Form.valueOf(node.getOperator().name());
        checkState(form.equals(Form.AND) || form.equals(Form.OR), "Unknown logical operator: " + node.getOperator());
        return new SpecialFormExpression(form,
                leftExpression.getType(), ImmutableList.of(leftExpression, rightExpression));
    }

    @Override
    protected RowExpression visitComparisonExpression(ComparisonExpression node, Map<String, ColumnMetadata> context)
    {
        RowExpression leftRowExpression = process(node.getLeft(), context);
        RowExpression rightRowExpression = process(node.getRight(), context);
        RewrittenRowExpressions rewrittenRowExpressions = addRelevantCasts(leftRowExpression, rightRowExpression);
        checkState(rewrittenRowExpressions.leftRowExpression.getType().getTypeSignature().getBase()
                        .equals(rewrittenRowExpressions.rightRowExpression.getType().getTypeSignature().getBase()),
                format("Types on expression %s are not same %s != %s", node, leftRowExpression.getType(), rightRowExpression.getType()));
        Function<OperatorType, CallExpression> operatorMapper = (ops) -> new CallExpression(ops.name(),
                functionResolution.comparisonFunction(ops, rewrittenRowExpressions.leftRowExpression.getType(), rewrittenRowExpressions.rightRowExpression.getType()),
                BooleanType.BOOLEAN,
                List.of(rewrittenRowExpressions.leftRowExpression, rewrittenRowExpressions.rightRowExpression));
        if (COMPARISON_OPERATORS.containsKey(node.getOperator().getValue())) {
            OperatorType operatorType = COMPARISON_OPERATORS.get(node.getOperator().getValue());
            return operatorMapper.apply(operatorType);
        }
        throw new UnsupportedOperationException(format("Unknown comparison type found : %s", node.getOperator().getValue()));
    }

    @Override
    protected RowExpression visitExpression(Expression node, Map<String, ColumnMetadata> context)
    {
        throw new UnsupportedOperationException(format("Unsupported expression found : %s", node.toString()));
    }

    @Override
    protected RowExpression visitArrayConstructor(ArrayConstructor node, Map<String, ColumnMetadata> context)
    {
        List<RowExpression> arguments = node.getValues().stream()
                .map(value -> process(value, context))
                .collect(toImmutableList());
        List<Type> argumentTypes = arguments.stream()
                .map(RowExpression::getType)
                .collect(toImmutableList());
        FunctionHandle functionHandle = functionResolution.arrayConstructor(argumentTypes);
        return call("ARRAY", functionHandle, arguments);
    }

    @Override
    protected RowExpression visitRow(Row node, Map<String, ColumnMetadata> context)
    {
        List<RowExpression> arguments = node.getItems().stream()
                .map(value -> process(value, context))
                .collect(toImmutableList());
        List<Type> types = arguments.stream().map(RowExpression::getType).toList();
        RowType returnType = RowType.withDefaultFieldNames(types);
        return new SpecialFormExpression(ROW_CONSTRUCTOR, returnType, arguments);
    }

    @Override
    protected RowExpression visitNotExpression(NotExpression node, Map<String, ColumnMetadata> context)
    {
        return call("not", functionResolution.notFunction(), ImmutableList.of(process(node.getValue(), context)));
    }

    @Override
    protected RowExpression visitDoubleLiteral(DoubleLiteral node, Map<String, ColumnMetadata> context)
    {
        return new ConstantExpression(node.getValue(), typeManager.getType(DOUBLE.getTypeSignature()));
    }

    @Override
    protected RowExpression visitCharLiteral(CharLiteral node, Map<String, ColumnMetadata> context)
    {
        return new ConstantExpression(node.getSlice(), createVarcharType(node.getValue().length()));
    }

    @Override
    protected RowExpression visitLongLiteral(LongLiteral node, Map<String, ColumnMetadata> context)
    {
        if (node.getValue() >= Integer.MIN_VALUE && node.getValue() <= Integer.MAX_VALUE) {
            return new ConstantExpression(node.getValue(), INTEGER);
        }
        return new ConstantExpression(node.getValue(), BigintType.BIGINT);
    }

    @Override
    protected RowExpression visitDecimalLiteral(DecimalLiteral node, Map<String, ColumnMetadata> context)
    {
        DecimalParseResult parseResult = Decimals.parse(node.getValue());
        return new ConstantExpression(parseResult.getObject(), parseResult.getType());
    }

    @Override
    protected RowExpression visitStringLiteral(StringLiteral node, Map<String, ColumnMetadata> context)
    {
        return new ConstantExpression(node.getSlice(), createVarcharType(countCodePoints(node.getSlice())));
    }

    @Override
    protected RowExpression visitBinaryLiteral(BinaryLiteral node, Map<String, ColumnMetadata> context)
    {
        return new ConstantExpression(node.getValue(), VARBINARY);
    }

    @Override
    protected RowExpression visitAtTimeZone(AtTimeZone node, Map<String, ColumnMetadata> context)
    {
        RowExpression value = process(node.getValue(), context);
        RowExpression timeZone = process(node.getTimeZone(), context);
        Type valueType = value.getType();
        if (valueType.equals(TIME)) {
            value = call(
                    CAST_NAME,
                    functionResolution.lookupCast(CAST_NAME, valueType, TIME_WITH_TIME_ZONE),
                    ImmutableList.of(value));
        }
        else if (valueType.equals(TIMESTAMP)) {
            value = call(
                    CAST_NAME,
                    functionResolution.lookupCast(CAST_NAME, valueType, TIMESTAMP_WITH_TIME_ZONE),
                    ImmutableList.of(value));
        }

        return call("at_timezone", value, timeZone);
    }

    @Override
    protected RowExpression visitEnumLiteral(EnumLiteral node, Map<String, ColumnMetadata> context)
    {
        Type type;
        try {
            type = typeManager.getType(parseTypeSignature(node.getType()));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported type: " + node.getType());
        }

        return new ConstantExpression(node.getValue(), type);
    }

    @Override
    protected RowExpression visitGenericLiteral(GenericLiteral node, Map<String, ColumnMetadata> context)
    {
        Type type;
        TypeSignature typeSignature;
        try {
            typeSignature = parseTypeSignature(node.getType());
            type = typeManager.getType(typeSignature);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported type: " + node.getType());
        }

        ConstantExpression constant = constantExpression(node.getValue(), type);
        if (constant != null) {
            return constant;
        }

        if (JSON.equals(type)) {
            FunctionHandle functionHandle = functionResolution.lookupBuiltInFunction("json_parse", List.of(VARCHAR));
            if (functionHandle.getReturnType().isPresent()) {
                return new CallExpression(
                        Optional.empty(),
                        "json_parse",
                        functionHandle,
                        typeManager.getType(functionHandle.getReturnType().get()),
                        ImmutableList.of(new ConstantExpression(utf8Slice(node.getValue()), VARCHAR)));
            }
        }

        String base = typeSignature.getBase();
        // Iceberg does not have equivalent of presto's CHAR type
        if (base.equalsIgnoreCase(StandardTypes.VARCHAR) || base.equalsIgnoreCase(StandardTypes.CHAR)) {
            // this should work because, during evaluation of equivalence of 'variable types' - we tolerate the difference of type widths.
            return new ConstantExpression(utf8Slice(node.getValue()), VarcharType.VARCHAR);
        }
        throw new UnsupportedOperationException(format("unsupported literal type found: %s on %s", type, node));
    }

    private static @Nullable ConstantExpression constantExpression(String value, Type type)
    {
        if (TypeUtils.isExactNumericType(type)) {
            value = format("%.0f", Math.floor(Double.parseDouble(value))); // remove fractional part if exists
        }
        try {
            if (TINYINT.equals(type)) {
                return new ConstantExpression((long) Byte.parseByte(value), TINYINT);
            }
            else if (SMALLINT.equals(type)) {
                return new ConstantExpression((long) Short.parseShort(value), SMALLINT);
            }
            else if (INTEGER.equals(type)) {
                return new ConstantExpression((long) Integer.parseInt(value), INTEGER);
            }
            else if (BIGINT.equals(type)) {
                return new ConstantExpression(Long.parseLong(value), BIGINT);
            }
            else if (REAL.equals(type)) {
                return new ConstantExpression((long) floatToRawIntBits(Float.parseFloat(value)), REAL);
            }
            else if (DateType.DATE.equals(type)) {
                return new ConstantExpression((long) parseDate(value), DateType.DATE);
            }
        }
        catch (NumberFormatException e) {
            throw new UnsupportedOperationException(format("Invalid formatted generic %s literal: %s", type, value));
        }
        return null;
    }

    @Override
    protected RowExpression visitIdentifier(Identifier node, Map<String, ColumnMetadata> context)
    {
        if (context != null && context.containsKey(node.getValue())) {
            ColumnMetadata columnMetadata = context.get(node.getValue());
            checkArgument(columnMetadata.getDerivedColumnSpec().isEmpty(), "expression cannot make reference to derived column.");
            return new VariableReferenceExpression(Optional.empty(), node.getValue(), columnMetadata.getType());
        }
        throw new IllegalArgumentException(format("identifier %s is not found in table.", node.getValue()));
    }

    @Override
    protected RowExpression visitTimeLiteral(TimeLiteral node, Map<String, ColumnMetadata> context)
    {
        Type returnType = TIME;
        long value;
        if (timeHasTimeZone(node.getValue())) {
            returnType = TIME_WITH_TIME_ZONE;
            value = parseTimeWithTimeZone(node.getValue());
        }
        else {
            if (sqlFunctionProperties.isLegacyTimestamp()) {
                // parse in time zone of client
                value = parseTimeWithoutTimeZone(sqlFunctionProperties.getTimeZoneKey(), node.getValue());
            }
            else {
                value = parseTimeWithoutTimeZone(node.getValue());
            }
        }
        return new ConstantExpression(value, returnType);
    }

    @Override
    protected RowExpression visitTimestampLiteral(TimestampLiteral node, Map<String, ColumnMetadata> context)
    {
        Type returnType = TIMESTAMP;
        if (timestampHasTimeZone(node.getValue())) {
            returnType = TIMESTAMP_WITH_TIME_ZONE;
        }
        long value;
        if (sqlFunctionProperties.isLegacyTimestamp()) {
            value = parseTimestampLiteral(sqlFunctionProperties.getTimeZoneKey(), node.getValue());
        }
        else {
            value = parseTimestampLiteral(node.getValue());
        }
        return new ConstantExpression(value, returnType);
    }

    @Override
    protected RowExpression visitIntervalLiteral(IntervalLiteral node, Map<String, ColumnMetadata> context)
    {
        long value;
        if (node.isYearToMonth()) {
            value = node.getSign().multiplier() * parseYearMonthInterval(node.getValue(), node.getStartField(), node.getEndField());
        }
        else {
            value = node.getSign().multiplier() * parseDayTimeInterval(node.getValue(), node.getStartField(), node.getEndField());
        }
        return new ConstantExpression(value, typeManager.getType(parseTypeSignature(StandardTypes.INTERVAL_DAY_TO_SECOND)));
    }

    @Override
    protected RowExpression visitExtract(Extract node, Map<String, ColumnMetadata> context)
    {
        RowExpression value = process(node.getExpression(), context);
        switch (node.getField()) {
            case YEAR:
                return call("year", value);
            case QUARTER:
                return call("quarter", value);
            case MONTH:
                return call("month", value);
            case WEEK:
                return call("week", value);
            case DAY:
            case DAY_OF_MONTH:
                return call("day", value);
            case DAY_OF_WEEK:
            case DOW:
                return call("day_of_week", value);
            case DAY_OF_YEAR:
            case DOY:
                return call("day_of_year", value);
            case YEAR_OF_WEEK:
            case YOW:
                return call("year_of_week", value);
            case HOUR:
                return call("hour", value);
            case MINUTE:
                return call("minute", value);
            case SECOND:
                return call("second", value);
            case TIMEZONE_MINUTE:
                return call("timezone_minute", value);
            case TIMEZONE_HOUR:
                return call("timezone_hour", value);
        }

        throw new UnsupportedOperationException("not yet implemented: " + node.getField());
    }

    @Override
    protected RowExpression visitFieldReference(FieldReference node, Map<String, ColumnMetadata> context)
    {
        throw new UnsupportedOperationException("Derived columns do not support struct dereferencing." + node);
    }

    @Override
    protected RowExpression visitNullLiteral(NullLiteral node, Map<String, ColumnMetadata> context)
    {
        return new ConstantExpression(Optional.empty(), null, UnknownType.UNKNOWN);
    }

    @Override
    protected RowExpression visitBooleanLiteral(BooleanLiteral node, Map<String, ColumnMetadata> context)
    {
        return new ConstantExpression(node.getValue(), BooleanType.BOOLEAN);
    }

    @Override
    protected RowExpression visitLambdaExpression(LambdaExpression node, Map<String, ColumnMetadata> context)
    {
        RowExpression body = process(node.getBody(), context);

        Type type = body.getType();
        List<Type> typeParameters = type.getTypeParameters();
        List<Type> argumentTypes = typeParameters.subList(0, typeParameters.size() - 1);
        List<String> argumentNames = node.getArguments().stream()
                .map(LambdaArgumentDeclaration::getName)
                .map(Identifier::getValue)
                .collect(toImmutableList());

        return new LambdaDefinitionExpression(Optional.empty(), argumentTypes, argumentNames, body);
    }

    @Override
    protected RowExpression visitCoalesceExpression(CoalesceExpression node, Map<String, ColumnMetadata> context)
    {
        List<RowExpression> arguments = node.getOperands().stream()
                .map(value -> process(value, context))
                .collect(toImmutableList());
        if (arguments.stream().findFirst().isPresent()) {
            Type type = arguments.stream().findFirst().get().getType();
            return new SpecialFormExpression(COALESCE, type, arguments);
        }
        throw new IllegalStateException("Coalesce operation with missing arguments.");
    }

    @Override
    protected RowExpression visitArithmeticUnary(ArithmeticUnaryExpression node, Map<String, ColumnMetadata> context)
    {
        RowExpression expression = process(node.getValue(), context);

        switch (node.getSign()) {
            case PLUS:
                return expression;
            case MINUS:
                return new CallExpression(
                        Optional.empty(),
                        NEGATION.name(),
                        functionResolution.negateFunction(expression.getType()),
                        expression.getType(),
                        List.of(expression));
        }

        throw new UnsupportedOperationException("Unsupported unary operator: " + node.getSign());
    }

    @Override
    protected RowExpression visitSimpleCaseExpression(SimpleCaseExpression node, Map<String, ColumnMetadata> context)
    {
        return buildSwitch(process(node.getOperand(), context), node.getWhenClauses(), node.getDefaultValue(), context);
    }

    @Override
    protected RowExpression visitSearchedCaseExpression(SearchedCaseExpression node, Map<String, ColumnMetadata> context)
    {
        return buildSwitch(new ConstantExpression(Optional.empty(), true, BOOLEAN), node.getWhenClauses(), node.getDefaultValue(), context);
    }

    private RowExpression buildSwitch(RowExpression operand, List<WhenClause> whenClauses, Optional<Expression> defaultValue, Map<String, ColumnMetadata> context)
    {
        ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
        if (defaultValue.isPresent()) {
            arguments.add(operand);
            Type returnType = process(defaultValue.get(), context).getType();
            for (WhenClause clause : whenClauses) {
                arguments.add(new SpecialFormExpression(
                        WHEN,
                        process(clause.getOperand(), context).getType(),
                        process(clause.getOperand(), context),
                        process(clause.getResult(), context)));
            }

            arguments.add(defaultValue
                    .map((value) -> process(value, context))
                    .orElseGet(() -> new ConstantExpression(Optional.empty(), null, returnType)));

            return new SpecialFormExpression(SWITCH, returnType, arguments.build());
        }
        throw new UnsupportedOperationException("Only case statements with default are supported as expression on derived columns.");
    }

    @Override
    protected RowExpression visitInPredicate(InPredicate node, Map<String, ColumnMetadata> context)
    {
        ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
        RowExpression value = process(node.getValue(), context);
        if (!(node.getValueList() instanceof InListExpression)) {
            throw new UnsupportedOperationException(format("subquery : %s is not supported as expression on derived column. Full expression: %s", node.getValueList(), node));
        }
        InListExpression values = (InListExpression) node.getValueList();

        if (values.getValues().size() == 1) {
            return buildEquals(value, process(values.getValues().get(0), context));
        }

        arguments.add(value);
        for (Expression inValue : values.getValues()) {
            arguments.add(process(inValue, context));
        }

        return new SpecialFormExpression(IN, BOOLEAN, arguments.build());
    }

    @Override
    protected RowExpression visitExists(ExistsPredicate existsPredicate, Map<String, ColumnMetadata> context)
    {
        RowExpression subquery = process(existsPredicate.getSubquery(), context);
        return new ExistsExpression(subquery.getSourceLocation(), subquery);
    }

    @Override
    protected RowExpression visitValues(Values node, Map<String, ColumnMetadata> context)
    {
        // VALUES can be converted into a InList if all entries are constants, however presto replaces them with
        // a variable ref expression and then it becomes a tricky rewrite rule. e.g. lower(x) in (VALUES 'a', 'b', 'c') ->
        // lower(x) in expr1,
        // A conversion of type (VALUES 'a', 'b') -> ('a', 'b') is doable, however user expression may not be matched.
        // So, we throw exception instead.
        throw new UnsupportedOperationException("subqueries are not supported in expression for derived columns.");
    }

    private RowExpression buildEquals(RowExpression lhs, RowExpression rhs)
    {
        return new CallExpression(
                EQUAL.getOperator(),
                functionResolution.comparisonFunction(EQUAL, lhs.getType(), rhs.getType()),
                BOOLEAN,
                ImmutableList.of(lhs, rhs));
    }

    @Override
    protected RowExpression visitBetweenPredicate(BetweenPredicate node, Map<String, ColumnMetadata> context)
    {
        RowExpression value = process(node.getValue(), context);
        RowExpression min = process(node.getMin(), context);
        RowExpression max = process(node.getMax(), context);

        return new CallExpression(
                BETWEEN.name(),
                functionResolution.betweenFunction(value.getType(), min.getType(), max.getType()),
                BOOLEAN,
                ImmutableList.of(value, min, max));
    }

    @Override
    protected RowExpression visitIfExpression(IfExpression node, Map<String, ColumnMetadata> context)
    {
        ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
        RowExpression trueValueRowExpression = process(node.getTrueValue(), context);
        arguments.add(process(node.getCondition(), context))
                .add(trueValueRowExpression);

        if (node.getFalseValue().isPresent()) {
            arguments.add(process(node.getFalseValue().get(), context));
        }
        else {
            arguments.add(new ConstantExpression(Optional.empty(), trueValueRowExpression.getType()));
        }

        return new SpecialFormExpression(Form.IF, trueValueRowExpression.getType(), arguments.build());
    }

    @Override
    protected RowExpression visitFunctionCall(FunctionCall node, Map<String, ColumnMetadata> context)
    {
        List<RowExpression> argumentRowExpressions = node.getArguments().stream().map(expression -> process(expression, context)).toList();
        return call(node.getName().toString(), argumentRowExpressions);
    }

    private ConstantExpression nullConst(Type type)
    {
        return new ConstantExpression(Optional.empty(), null, type);
    }

    private CallExpression call(String name, RowExpression... arguments)
    {
        return call(name, Arrays.asList(arguments));
    }

    private CallExpression call(String name, List<RowExpression> arguments)
    {
        FunctionHandle functionHandle = functionResolution.lookupBuiltInFunction(name, arguments.stream().map(RowExpression::getType).collect(toImmutableList()));
        return call(name, functionHandle, arguments);
    }

    private CallExpression call(String name, FunctionHandle functionHandle, List<RowExpression> arguments)
    {
        checkArgument(functionMetadataManager.getFunctionMetadata(functionHandle).isDeterministic(), "Only deterministic functions are supported");
        checkArgument(functionMetadataManager.getFunctionMetadata(functionHandle).getFunctionKind() == FunctionKind.SCALAR, "Only scalar functions are supported");
        if (functionHandle.getReturnType().isPresent()) {
            Type returnType = functionHandle.getReturnType().map(typeManager::getType).get();
            return new CallExpression(name, functionHandle, returnType, arguments);
        }
        throw new IllegalArgumentException("builtin function not found, name: " + name);
    }

    private ConstantExpression translateLiteral(ConstantExpression expression, Type targetType)
    {
        if (targetType instanceof DecimalType) {
            DecimalParseResult parseResult = Decimals.parse(expression.getValue().toString());
            Object value;
            if (parseResult.getType().isShort()) {
                value = rescale((long) parseResult.getObject(), parseResult.getType().getScale(), ((DecimalType) targetType).getScale());
            }
            else {
                value = rescale(decodeUnscaledValue((Slice) parseResult.getObject()), parseResult.getType().getScale(), ((DecimalType) targetType).getScale());
            }
            return new ConstantExpression(Optional.empty(), value, targetType);
        }
        else {
            String value = expression.getValue().toString();
            if (expression.getType() instanceof DecimalType) {
                if (((DecimalType) expression.getType()).isShort()) {
                    value = Decimals.toString((Long) expression.getValue(), ((DecimalType) expression.getType()).getScale());
                }
                else {
                    value = Decimals.toString((Slice) expression.getValue(), ((DecimalType) expression.getType()).getScale());
                }
            }
            return constantExpression(value, targetType);
        }
    }

    private RewrittenRowExpressions addRelevantCasts(RowExpression leftRowExpression, RowExpression rightRowExpression)
    {
        if (leftRowExpression.getType().getTypeSignature().getBase().equals(rightRowExpression.getType().getTypeSignature().getBase())) {
            return new RewrittenRowExpressions(leftRowExpression, rightRowExpression);
        }
        if (!TypeUtils.isNumericType(leftRowExpression.getType()) || !TypeUtils.isNumericType(rightRowExpression.getType())) {
            return new RewrittenRowExpressions(leftRowExpression, rightRowExpression);
        }
        if (leftRowExpression instanceof ConstantExpression || rightRowExpression instanceof ConstantExpression) {
            return new RewrittenRowExpressions(leftRowExpression, rightRowExpression);
        }
        // DoubleType< RealType< DecimalType<BigIntType< Integer< small_int< tiny_int.
        Map<Type, Integer> typeCastPriority = ImmutableMap.of(DOUBLE, 200, REAL, 160, BIGINT, 10, INTEGER, 8, SMALLINT, 6, TINYINT, 4);
        // if the type of LHS != RHS, presto tries to add relevant CASTs, we try to emulate that as follows.
        if (!leftRowExpression.getType().equals(rightRowExpression.getType())) {
            int leftPriority = leftRowExpression.getType() instanceof DecimalType ? ((DecimalType) leftRowExpression.getType()).getPrecision() + 10 : typeCastPriority.get(leftRowExpression.getType());
            int rightPriority = rightRowExpression.getType() instanceof DecimalType ? ((DecimalType) rightRowExpression.getType()).getPrecision() + 10 : typeCastPriority.get(rightRowExpression.getType());
            if (leftPriority > rightPriority) {
                Type targetType = leftRowExpression.getType();
//                if (leftRowExpression instanceof ConstantExpression || rightRowExpression instanceof ConstantExpression) {
//                    if (rightRowExpression instanceof ConstantExpression) {
//                        // Do not apply casts to constant expression, directly translate to the target type literals.
//                        rightRowExpression = translateLiteral((ConstantExpression) rightRowExpression, targetType);
//                    }
//                    else {
//                        leftRowExpression = translateLiteral((ConstantExpression) leftRowExpression, rightRowExpression.getType());
//                    }
//                }
//                else {
                FunctionHandle functionHandle = functionResolution.lookupCast(CAST_NAME, rightRowExpression.getType(), targetType);
                rightRowExpression = new CallExpression(CAST_NAME, functionHandle, targetType, ImmutableList.of(rightRowExpression));
//                }
            }
            else {
                Type targetType = rightRowExpression.getType();
//                if (leftRowExpression instanceof ConstantExpression || rightRowExpression instanceof ConstantExpression) {
//                    if (leftRowExpression instanceof ConstantExpression) {
//                        leftRowExpression = translateLiteral((ConstantExpression) leftRowExpression, targetType);
//                    }
//                    else {
//                        rightRowExpression = translateLiteral((ConstantExpression) rightRowExpression, leftRowExpression.getType());
//                    }
//                }
//                else {
                FunctionHandle functionHandle = functionResolution.lookupCast(CAST_NAME, leftRowExpression.getType(), targetType);
                leftRowExpression = new CallExpression(CAST_NAME, functionHandle, targetType, ImmutableList.of(leftRowExpression));
//                }
            }
        }

        return new RewrittenRowExpressions(leftRowExpression, rightRowExpression);
    }

    private record RewrittenRowExpressions(RowExpression leftRowExpression, RowExpression rightRowExpression) {}
}
