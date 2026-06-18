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
package com.facebook.presto.druid;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
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

import static com.facebook.presto.druid.DruidErrorCode.DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.druid.DruidExpression.derived;
import static com.facebook.presto.druid.DruidPushdownUtils.getLiteralAsString;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class DruidAggregationProjectConverter
        extends DruidProjectExpressionConverter
{
    private static final Map<String, String> PRESTO_TO_DRUID_OPERATORS = ImmutableMap.of(
            "-", "SUB",
            "+", "ADD",
            "*", "MULT",
            "/", "DIV");
    private static final String FROM_UNIXTIME = "from_unixtime";
    private static final String DATE_TRUNC = "date_trunc";

    private final FunctionMetadataManager functionMetadataManager;
    private final ConnectorSession session;

    public DruidAggregationProjectConverter(
            ConnectorSession session,
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution)
    {
        super(typeManager, standardFunctionResolution);
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public DruidExpression visitCall(
            CallExpression call,
            Map<VariableReferenceExpression, DruidQueryGeneratorContext.Selection> context)
    {
        Optional<DruidExpression> basicCallHandlingResult = basicCallHandling(call, context);
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
                throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Unsupported operator: " + call + " to pushdown for Druid connector.");
            }
        }
        return handleFunction(call, context);
    }

    @Override
    public DruidExpression visitConstant(
            ConstantExpression literal,
            Map<VariableReferenceExpression, DruidQueryGeneratorContext.Selection> context)
    {
        return new DruidExpression(getLiteralAsString(session, literal), DruidQueryGeneratorContext.Origin.LITERAL);
    }

    private DruidExpression handleDateTruncationViaDateTruncation(
            CallExpression function,
            Map<VariableReferenceExpression, DruidQueryGeneratorContext.Selection> context)
    {
        RowExpression timeInputParameter = function.getArguments().get(1);
        String inputColumn;
        String inputTimeZone;
        String inputFormat;

        CallExpression timeConversion = getExpressionAsFunction(timeInputParameter, timeInputParameter);
        if (!timeConversion.getDisplayName().toLowerCase(ENGLISH).equals(FROM_UNIXTIME)) {
            throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Unsupported time function: " + timeConversion.getDisplayName() + " to pushdown for Druid connector.");
        }

        inputColumn = timeConversion.getArguments().get(0).accept(this, context).getDefinition();
        inputTimeZone = timeConversion.getArguments().size() > 1 ? getStringFromConstant(timeConversion.getArguments().get(1)) : DateTimeZone.UTC.getID();
        inputFormat = "seconds";
        RowExpression intervalParameter = function.getArguments().get(0);
        if (!(intervalParameter instanceof ConstantExpression)) {
            throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Unsupported interval unit: " + intervalParameter + " to pushdown for Druid connector.");
        }

        return derived("dateTrunc(" + inputColumn + "," + inputFormat + ", " + inputTimeZone + ", " + getStringFromConstant(intervalParameter) + ")");
    }

    private DruidExpression handleArithmeticExpression(
            CallExpression expression,
            OperatorType operatorType,
            Map<VariableReferenceExpression, DruidQueryGeneratorContext.Selection> context)
    {
        List<RowExpression> arguments = expression.getArguments();
        if (arguments.size() == 1) {
            String prefix = operatorType == OperatorType.NEGATION ? "-" : "";
            return derived(prefix + arguments.get(0).accept(this, context).getDefinition());
        }
        if (arguments.size() == 2) {
            DruidExpression left = arguments.get(0).accept(this, context);
            DruidExpression right = arguments.get(1).accept(this, context);
            String prestoOperator = operatorType.getOperator();
            String druidOperator = PRESTO_TO_DRUID_OPERATORS.get(prestoOperator);
            if (druidOperator == null) {
                throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Unsupported binary expression: " + prestoOperator + " to pushdown for Druid connector.");
            }
            return derived(format("%s(%s, %s)", druidOperator, left.getDefinition(), right.getDefinition()));
        }
        throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Unsupported arithmetic expression: " + expression + " to pushdown for Druid connector.");
    }

    private DruidExpression handleFunction(
            CallExpression function,
            Map<VariableReferenceExpression, DruidQueryGeneratorContext.Selection> context)
    {
        if (function.getDisplayName().toLowerCase(ENGLISH).equals(DATE_TRUNC)) {
            return handleDateTruncationViaDateTruncation(function, context);
        }
        throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Unsupported function: " + function.getDisplayName() + " to pushdown for Druid connector.");
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
        throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Expected string literal but found: " + expression + " to pushdown for Druid connector.");
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
        throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Could not dig function out of expression: " + originalExpression + ", inside of: " + expression + " to pushdown for Druid connector.");
    }
}
