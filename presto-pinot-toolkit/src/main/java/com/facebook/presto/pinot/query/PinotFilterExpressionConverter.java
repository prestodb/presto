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
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Selection;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableSet;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.pinot.PinotPushdownUtils.getLiteralAsString;
import static com.facebook.presto.pinot.query.PinotExpression.derived;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Convert {@link RowExpression} in filter into Pinot complaint expression text
 */
public class PinotFilterExpressionConverter
        implements RowExpressionVisitor<PinotExpression, Function<VariableReferenceExpression, Selection>>
{
    private static final Set<String> LOGICAL_BINARY_OPS_FILTER = ImmutableSet.of("=", "<", "<=", ">", ">=", "<>");
    private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date().withZoneUTC();

    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;

    public PinotFilterExpressionConverter(
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution)
    {
        this.typeManager = requireNonNull(typeManager, "type manager is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "function metadata manager is null");
        this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standardFunctionResolution is null");
    }

    private PinotExpression handleIn(
            SpecialFormExpression specialForm,
            boolean isWhitelist,
            Function<VariableReferenceExpression, Selection> context)
    {
        return derived(format("(%s %s (%s))",
                specialForm.getArguments().get(0).accept(this, context).getDefinition(),
                isWhitelist ? "IN" : "NOT IN",
                specialForm.getArguments().subList(1, specialForm.getArguments().size()).stream()
                        .map(argument -> argument.accept(this, context).getDefinition())
                        .collect(Collectors.joining(", "))));
    }

    private PinotExpression handleLogicalBinary(
            String operator,
            CallExpression call,
            Function<VariableReferenceExpression, Selection> context)
    {
        if (!LOGICAL_BINARY_OPS_FILTER.contains(operator)) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("'%s' is not supported in filter", operator));
        }
        List<RowExpression> arguments = call.getArguments();
        if (arguments.size() == 2) {
            // Check if call compares a date/time column with a date/time constant. Otherwise just treat it like a regular binary operator.
            return handleDateOrTimestampBinaryExpression(operator, arguments, context).orElseGet(
                    () -> derived(format(
                            "(%s %s %s)",
                            arguments.get(0).accept(this, context).getDefinition(),
                            operator,
                            arguments.get(1).accept(this, context).getDefinition())));
        }
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("Unknown logical binary: '%s'", call));
    }

    private Optional<PinotExpression> handleDateOrTimestampBinaryExpression(String operator, List<RowExpression> arguments, Function<VariableReferenceExpression, Selection> context)
    {
        Optional<String> left = handleTimeValueCast(context, arguments.get(1), arguments.get(0));
        Optional<String> right = handleTimeValueCast(context, arguments.get(0), arguments.get(1));
        if (left.isPresent() && right.isPresent()) {
            return Optional.of(derived(format("(%s %s %s)", left.get(), operator, right.get())));
        }
        return Optional.empty();
    }

    private static boolean isDateTimeConstantType(Type type)
    {
        return type == DateType.DATE || type == TimestampType.TIMESTAMP || type == TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
    }

    private Optional<String> handleTimeValueCast(Function<VariableReferenceExpression, Selection> context, RowExpression timeFieldExpression, RowExpression timeValueExpression)
    {
        // Handle the binary comparison logic of <DATE/TIMESTAMP field> <binary op> <DATE/TIMESTAMP literal>.
        // Pinot stores time as:
        //   - `DATE`: Stored as `INT`/`LONG` `daysSinceEpoch` value
        //   - `TIMESTAMP`: Stored as `LONG` `millisSinceEpoch` value.
        // In order to push down this predicate, we need to convert the literal to the type of Pinot time field.
        // Below code compares the time type of both side:
        //   - if same, then directly push down.
        //   - if not same, then convert the literal time type to the field time type.
        //   - if not compatible time types, returns Optional.empty(), indicates no change has been made in this cast.
        // Take an example of comparing a `DATE` field to a `TIMESTAMP` literal:
        //   - Sample predicate: `WHERE eventDate < current_time`.
        //   - Input type is the `eventDate` field data type, which is `DATE`.
        //   - Expect type is the right side `literal type`, which means right side is `TIMESTAMP`.
        // The code below converts `current_time` from `millisSinceEpoch` value to `daysSinceEpoch` value, which is
        // comparable to values in `eventDate` column.
        Type inputType;
        Type expectedType;
        if (!isDateTimeConstantType(timeFieldExpression.getType()) || !isDateTimeConstantType(timeValueExpression.getType())) {
            return Optional.empty();
        }
        String timeValueString = timeValueExpression.accept(this, context).getDefinition();
        if (timeFieldExpression instanceof CallExpression) {
            // Handles cases like: `cast(eventDate as TIMESTAMP) <  DATE '2014-01-31'`
            // For cast function,
            // - inputType is the argument type,
            // - expectedType is the cast function return type.
            CallExpression callExpression = (CallExpression) timeFieldExpression;
            if (!standardFunctionResolution.isCastFunction(callExpression.getFunctionHandle())) {
                return Optional.empty();
            }
            if (callExpression.getArguments().size() != 1) {
                return Optional.empty();
            }
            inputType = callExpression.getArguments().get(0).getType();
            expectedType = callExpression.getType();
        }
        else if (timeFieldExpression instanceof VariableReferenceExpression) {
            // For VariableReferenceExpression,
            // Handles queries like: `eventDate <  TIMESTAMP '2014-01-31 00:00:00 UTC'`
            // - inputType is timeFieldExpression type,
            // - expectedType is the timeValueExpression type.
            inputType = timeFieldExpression.getType();
            expectedType = timeValueExpression.getType();
        }
        else if (timeFieldExpression instanceof ConstantExpression) {
            // timeFieldExpression is a ConstantExpression, directly return.
            return Optional.of(timeValueString);
        }
        else {
            return Optional.empty();
        }
        if (inputType == DateType.DATE && (expectedType == TimestampType.TIMESTAMP || expectedType == TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE)) {
            // time field is `DATE`, try to convert time value from `TIMESTAMP` to `DATE`
            try {
                return Optional.of(Long.toString(TimeUnit.MILLISECONDS.toDays(Long.parseLong(timeValueString))));
            }
            catch (NumberFormatException e) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("Unable to parse timestamp string: '%s'", timeValueString), e);
            }
        }
        if ((inputType == TimestampType.TIMESTAMP || inputType == TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE) && expectedType == DateType.DATE) {
            // time field is `TIMESTAMP`, try to convert time value from `DATE` to `TIMESTAMP`
            try {
                return Optional.of(Long.toString(TimeUnit.DAYS.toMillis(Long.parseLong(timeValueString))));
            }
            catch (NumberFormatException e) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("Unable to parse date string: '%s'", timeValueString), e);
            }
        }
        // Vacuous cast from variable to same type: cast(eventDate, DAYS). Already handled by handleCast.
        return Optional.of(timeValueString);
    }

    private PinotExpression handleContains(
            CallExpression contains,
            Function<VariableReferenceExpression, Selection> context)
    {
        if (contains.getArguments().size() != 2) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("Contains operator not supported: %s", contains));
        }
        RowExpression left = contains.getArguments().get(0);
        RowExpression right = contains.getArguments().get(1);
        if (!(right instanceof ConstantExpression)) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("Contains operator can not push down non-literal value: %s", right));
        }
        return derived(format(
                "(%s = %s)",
                left.accept(this, context).getDefinition(),
                right.accept(this, context).getDefinition()));
    }

    private PinotExpression handleBetween(
            CallExpression between,
            Function<VariableReferenceExpression, Selection> context)
    {
        if (between.getArguments().size() == 3) {
            RowExpression value = between.getArguments().get(0);
            RowExpression min = between.getArguments().get(1);
            RowExpression max = between.getArguments().get(2);

            return derived(format(
                    "(%s BETWEEN %s AND %s)",
                    value.accept(this, context).getDefinition(),
                    min.accept(this, context).getDefinition(),
                    max.accept(this, context).getDefinition()));
        }

        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("Between operator not supported: %s", between));
    }

    private PinotExpression handleNot(CallExpression not, Function<VariableReferenceExpression, Selection> context)
    {
        if (not.getArguments().size() == 1) {
            RowExpression input = not.getArguments().get(0);
            if (input instanceof SpecialFormExpression) {
                SpecialFormExpression specialFormExpression = (SpecialFormExpression) input;
                // NOT operator is only supported on top of the IN expression
                if (specialFormExpression.getForm() == SpecialFormExpression.Form.IN) {
                    return handleIn(specialFormExpression, false, context);
                }
            }
        }

        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("NOT operator is supported only on top of IN operator. Received: %s", not));
    }

    private PinotExpression handleCast(CallExpression cast, Function<VariableReferenceExpression, Selection> context)
    {
        if (cast.getArguments().size() == 1) {
            RowExpression input = cast.getArguments().get(0);
            Type expectedType = cast.getType();
            if (typeManager.canCoerce(input.getType(), expectedType)) {
                return input.accept(this, context);
            }
            // Expression like `DATE '2014-01-31'` is not cast to a constant number (like days since epoch) and thus it needs to be specifically handled here.
            // `TIMESTAMP` type is cast correctly to milliseconds value.
            if (expectedType == DateType.DATE) {
                try {
                    PinotExpression expression = input.accept(this, context);
                    if (input.getType() == TimestampType.TIMESTAMP || input.getType() == TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE) {
                        return expression;
                    }
                    // Special handling for Predicate like: "WHERE eventDate < DATE '2014-01-31'".
                    // It converts ISO DateTime to daysSinceEpoch value so Pinot could understand this.
                    if (input.getType() == VarcharType.VARCHAR) {
                        // Remove the leading & trailing quote then parse
                        Integer daysSinceEpoch = (int) TimeUnit.MILLISECONDS.toDays(DATE_FORMATTER.parseMillis(expression.getDefinition().substring(1, expression.getDefinition().length() - 1)));
                        return new PinotExpression(daysSinceEpoch.toString(), expression.getOrigin());
                    }
                }
                catch (Exception e) {
                    throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Cast date value expression is not supported: " + cast);
                }
            }
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Non implicit casts not supported: " + cast);
        }

        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("This type of CAST operator not supported. Received: %s", cast));
    }

    @Override
    public PinotExpression visitCall(CallExpression call, Function<VariableReferenceExpression, Selection> context)
    {
        FunctionHandle functionHandle = call.getFunctionHandle();
        if (standardFunctionResolution.isNotFunction(functionHandle)) {
            return handleNot(call, context);
        }
        if (standardFunctionResolution.isCastFunction(functionHandle)) {
            return handleCast(call, context);
        }
        if (standardFunctionResolution.isBetweenFunction(functionHandle)) {
            return handleBetween(call, context);
        }
        FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(call.getFunctionHandle());
        Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
        if (operatorTypeOptional.isPresent()) {
            OperatorType operatorType = operatorTypeOptional.get();
            if (operatorType.isArithmeticOperator()) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Arithmetic expressions are not supported in filter: " + call);
            }
            if (operatorType.isComparisonOperator()) {
                return handleLogicalBinary(operatorType.getOperator(), call, context);
            }
        }
        if ("contains".equals(functionMetadata.getName().getObjectName())) {
            return handleContains(call, context);
        }
        // Handle queries like `eventTimestamp < 1391126400000`.
        // Otherwise TypeManager.canCoerce(...) will return false and directly fail this query.
        if (functionMetadata.getName().getObjectName().equalsIgnoreCase("$literal$timestamp") ||
                    functionMetadata.getName().getObjectName().equalsIgnoreCase("$literal$date")) {
            return handleDateAndTimestampMagicLiteralFunction(call, context);
        }
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("function %s not supported in filter", call));
    }

    private PinotExpression handleDateAndTimestampMagicLiteralFunction(CallExpression timestamp, Function<VariableReferenceExpression, Selection> context)
    {
        if (timestamp.getArguments().size() == 1) {
            RowExpression input = timestamp.getArguments().get(0);
            Type expectedType = timestamp.getType();
            if (typeManager.canCoerce(input.getType(), expectedType) || input.getType() == BigintType.BIGINT || input.getType() == IntegerType.INTEGER) {
                return input.accept(this, context);
            }
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Non implicit Date/Timestamp Literal is not supported: " + timestamp);
        }
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("The Date/Timestamp Literal is not supported. Received: %s", timestamp));
    }

    @Override
    public PinotExpression visitInputReference(InputReferenceExpression reference, Function<VariableReferenceExpression, Selection> context)
    {
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Pinot does not support struct dereferencing " + reference);
    }

    @Override
    public PinotExpression visitConstant(ConstantExpression literal, Function<VariableReferenceExpression, Selection> context)
    {
        return new PinotExpression(getLiteralAsString(literal), Origin.LITERAL);
    }

    @Override
    public PinotExpression visitLambda(LambdaDefinitionExpression lambda, Function<VariableReferenceExpression, Selection> context)
    {
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Pinot does not support lambda " + lambda);
    }

    @Override
    public PinotExpression visitVariableReference(VariableReferenceExpression reference, Function<VariableReferenceExpression, Selection> context)
    {
        Selection input = requireNonNull(context.apply(reference), format("Input column %s does not exist in the input: %s", reference, context));
        return new PinotExpression(input.getDefinition(), input.getOrigin());
    }

    @Override
    public PinotExpression visitSpecialForm(SpecialFormExpression specialForm, Function<VariableReferenceExpression, Selection> context)
    {
        switch (specialForm.getForm()) {
            case IF:
            case NULL_IF:
            case SWITCH:
            case WHEN:
            case IS_NULL:
            case COALESCE:
            case DEREFERENCE:
            case ROW_CONSTRUCTOR:
            case BIND:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Pinot does not support the special form " + specialForm);
            case IN:
                return handleIn(specialForm, true, context);
            case AND:
            case OR:
                return derived(format(
                        "(%s %s %s)",
                        specialForm.getArguments().get(0).accept(this, context).getDefinition(),
                        specialForm.getForm().toString(),
                        specialForm.getArguments().get(1).accept(this, context).getDefinition()));
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Unexpected special form: " + specialForm);
        }
    }
}
