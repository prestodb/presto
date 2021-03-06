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
package com.facebook.presto.pinot.query;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.pinot.PinotSessionProperties;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.pinot.PinotPushdownUtils.getLiteralAsString;
import static com.facebook.presto.pinot.query.PinotExpression.derived;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PinotAggregationProjectConverter
        extends PinotProjectExpressionConverter
{
    // Pinot does not support modulus yet
    private static final Map<String, String> PRESTO_TO_PINOT_OPERATORS = ImmutableMap.of(
            "-", "SUB",
            "+", "ADD",
            "*", "MULT",
            "/", "DIV");
    private static final String FROM_UNIXTIME = "from_unixtime";

    private static final Map<String, String> PRESTO_TO_PINOT_ARRAY_AGGREGATIONS = ImmutableMap.<String, String>builder()
            .put("array_min", "arrayMin")
            .put("array_max", "arrayMax")
            .put("array_average", "arrayAverage")
            .put("array_sum", "arraySum")
            .build();

    private final FunctionMetadataManager functionMetadataManager;
    private final ConnectorSession session;
    private final VariableReferenceExpression arrayVariableHint;

    public PinotAggregationProjectConverter(TypeManager typeManager, FunctionMetadataManager functionMetadataManager, StandardFunctionResolution standardFunctionResolution, ConnectorSession session)
    {
        this(typeManager, functionMetadataManager, standardFunctionResolution, session, null);
    }

    public PinotAggregationProjectConverter(TypeManager typeManager, FunctionMetadataManager functionMetadataManager, StandardFunctionResolution standardFunctionResolution, ConnectorSession session, VariableReferenceExpression arrayVariableHint)
    {
        super(typeManager, standardFunctionResolution);
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.session = requireNonNull(session, "session is null");
        this.arrayVariableHint = arrayVariableHint;
    }

    @Override
    public PinotExpression visitCall(
            CallExpression call,
            Map<VariableReferenceExpression, PinotQueryGeneratorContext.Selection> context)
    {
        Optional<PinotExpression> basicCallHandlingResult = basicCallHandling(call, context);
        if (basicCallHandlingResult.isPresent()) {
            return basicCallHandlingResult.get();
        }
        FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(call.getFunctionHandle());
        Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
        if (operatorTypeOptional.isPresent()) {
            OperatorType operatorType = operatorTypeOptional.get();
            if (operatorType.isArithmeticOperator()) {
                return handleArithmeticExpression(call, operatorType, context);
            }
            if (operatorType.isComparisonOperator()) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Comparison operator not supported: " + call);
            }
        }
        return handleFunction(call, context);
    }

    @Override
    public PinotExpression visitConstant(
            ConstantExpression literal,
            Map<VariableReferenceExpression, PinotQueryGeneratorContext.Selection> context)
    {
        return new PinotExpression(getLiteralAsString(literal), PinotQueryGeneratorContext.Origin.LITERAL);
    }

    private PinotExpression handleDateTruncationViaDateTimeConvert(
            CallExpression function,
            Map<VariableReferenceExpression, PinotQueryGeneratorContext.Selection> context)
    {
        // Convert SQL standard function `DATE_TRUNC(INTERVAL, DATE/TIMESTAMP COLUMN)` to
        // Pinot's equivalent function `dateTimeConvert(columnName, inputFormat, outputFormat, outputGranularity)`
        // Pinot doesn't have a DATE/TIMESTAMP type. That means the input column (second argument) has been converted from numeric type to DATE/TIMESTAMP using one of the
        // conversion functions in SQL. First step is find the function and find its input column units (seconds, secondsSinceEpoch etc.)
        RowExpression timeInputParameter = function.getArguments().get(1);
        String inputColumn;
        String inputFormat;

        CallExpression timeConversion = getExpressionAsFunction(timeInputParameter, timeInputParameter);
        switch (timeConversion.getDisplayName().toLowerCase(ENGLISH)) {
            case FROM_UNIXTIME:
                inputColumn = timeConversion.getArguments().get(0).accept(this, context).getDefinition();
                inputFormat = "'1:SECONDS:EPOCH'";
                break;
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "not supported: " + timeConversion.getDisplayName());
        }

        String outputFormat = "'1:MILLISECONDS:EPOCH'";
        String outputGranularity;

        RowExpression intervalParameter = function.getArguments().get(0);
        if (!(intervalParameter instanceof ConstantExpression)) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(),
                    "interval unit in date_trunc is not supported: " + intervalParameter);
        }

        String value = getStringFromConstant(intervalParameter);
        switch (value) {
            case "second":
                outputGranularity = "'1:SECONDS'";
                break;
            case "minute":
                outputGranularity = "'1:MINUTES'";
                break;
            case "hour":
                outputGranularity = "'1:HOURS'";
                break;
            case "day":
                outputGranularity = "'1:DAYS'";
                break;
            case "week":
                outputGranularity = "'1:WEEKS'";
                break;
            case "month":
                outputGranularity = "'1:MONTHS'";
                break;
            case "quarter":
                outputGranularity = "'1:QUARTERS'";
                break;
            case "year":
                outputGranularity = "'1:YEARS'";
                break;
            default:
                throw new PinotException(
                        PINOT_UNSUPPORTED_EXPRESSION,
                        Optional.empty(),
                        "interval in date_trunc is not supported: " + value);
        }

        return derived("dateTimeConvert(" + inputColumn + ", " + inputFormat + ", " + outputFormat + ", " + outputGranularity + ")");
    }

    private PinotExpression handleDateTruncationViaDateTruncation(
            CallExpression function,
            Map<VariableReferenceExpression, PinotQueryGeneratorContext.Selection> context)
    {
        RowExpression timeInputParameter = function.getArguments().get(1);
        String inputColumn;
        String inputTimeZone;
        String inputFormat;

        CallExpression timeConversion = getExpressionAsFunction(timeInputParameter, timeInputParameter);
        switch (timeConversion.getDisplayName().toLowerCase(ENGLISH)) {
            case FROM_UNIXTIME:
                inputColumn = timeConversion.getArguments().get(0).accept(this, context).getDefinition();
                inputTimeZone = timeConversion.getArguments().size() > 1 ? getStringFromConstant(timeConversion.getArguments().get(1)) : DateTimeZone.UTC.getID();
                inputFormat = "seconds";
                break;
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "not supported: " + timeConversion.getDisplayName());
        }

        RowExpression intervalParameter = function.getArguments().get(0);
        if (!(intervalParameter instanceof ConstantExpression)) {
            throw new PinotException(
                    PINOT_UNSUPPORTED_EXPRESSION,
                    Optional.empty(),
                    "interval unit in date_trunc is not supported: " + intervalParameter);
        }

        return derived("dateTrunc(" + inputColumn + "," + inputFormat + ", " + inputTimeZone + ", " + getStringFromConstant(intervalParameter) + ")");
    }

    private PinotExpression handleArithmeticExpression(
            CallExpression expression,
            OperatorType operatorType,
            Map<VariableReferenceExpression, PinotQueryGeneratorContext.Selection> context)
    {
        List<RowExpression> arguments = expression.getArguments();
        if (arguments.size() == 1) {
            String prefix = operatorType == OperatorType.NEGATION ? "-" : "";
            return derived(prefix + arguments.get(0).accept(this, context).getDefinition());
        }
        if (arguments.size() == 2) {
            PinotExpression left = arguments.get(0).accept(this, context);
            PinotExpression right = arguments.get(1).accept(this, context);
            String prestoOperator = operatorType.getOperator();
            String pinotOperator = PRESTO_TO_PINOT_OPERATORS.get(prestoOperator);
            if (pinotOperator == null) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Unsupported binary expression " + prestoOperator);
            }
            return derived(format("%s(%s, %s)", pinotOperator, left.getDefinition(), right.getDefinition()));
        }
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("Don't know how to interpret %s as an arithmetic expression", expression));
    }

    private PinotExpression handleFunction(
            CallExpression function,
            Map<VariableReferenceExpression, PinotQueryGeneratorContext.Selection> context)
    {
        String functionName = function.getDisplayName().toLowerCase(ENGLISH);
        switch (functionName) {
            case "date_trunc":
                boolean useDateTruncation = PinotSessionProperties.isUseDateTruncation(session);
                return useDateTruncation ?
                        handleDateTruncationViaDateTruncation(function, context) :
                        handleDateTruncationViaDateTimeConvert(function, context);
            case "array_max":
            case "array_min":
                String pinotArrayFunctionName = PRESTO_TO_PINOT_ARRAY_AGGREGATIONS.get(functionName);
                requireNonNull(pinotArrayFunctionName, "Converted Pinot array function is null for - " + functionName);
                return derived(String.format(
                        "%s(%s)",
                        pinotArrayFunctionName,
                        function.getArguments().get(0).accept(this, context).getDefinition()));
            // array_sum and array_reduce are translated to a reduce function with lambda functions, so we pass in
            // this arrayVariableHint to help determine which array function it is.
            case "reduce":
                if (arrayVariableHint != null) {
                    String arrayFunctionName = getArrayFunctionName(arrayVariableHint);
                    if (arrayFunctionName != null) {
                        String inputColumn = function.getArguments().get(0).accept(this, context).getDefinition();
                        return derived(String.format("%s(%s)", arrayFunctionName, inputColumn));
                    }
                }
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("function %s not supported yet", function.getDisplayName()));
        }
    }

    // The array function variable names are in the format of `array_sum`, `array_average_0`, `array_sum_1`.
    // So we can parse the array function name based on variable name.
    private String getArrayFunctionName(VariableReferenceExpression variable)
    {
        String[] variableNameSplits = variable.getName().split("_");
        if (variableNameSplits.length < 2 || variableNameSplits.length > 3) {
            return null;
        }
        String arrayFunctionName = String.format("%s_%s", variableNameSplits[0], variableNameSplits[1]);
        return PRESTO_TO_PINOT_ARRAY_AGGREGATIONS.get(arrayFunctionName);
    }

    private static String getStringFromConstant(RowExpression expression)
    {
        if (expression instanceof ConstantExpression) {
            Object value = ((ConstantExpression) expression).getValue();
            if (value instanceof String) {
                return (String) value;
            }
            if (value instanceof Slice) {
                return ((Slice) value).toStringUtf8();
            }
        }
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Expected string literal but found " + expression);
    }

    private CallExpression getExpressionAsFunction(
            RowExpression originalExpression,
            RowExpression expression)
    {
        if (expression instanceof CallExpression) {
            CallExpression call = (CallExpression) expression;
            if (standardFunctionResolution.isCastFunction(call.getFunctionHandle())) {
                if (isImplicitCast(call.getArguments().get(0).getType(), call.getType())) {
                    return getExpressionAsFunction(originalExpression, call.getArguments().get(0));
                }
            }
            else {
                return call;
            }
        }
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Could not dig function out of expression: " + originalExpression + ", inside of " + expression);
    }
}
