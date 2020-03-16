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

import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Selection;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;
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
            return derived(format(
                    "(%s %s %s)",
                    arguments.get(0).accept(this, context).getDefinition(),
                    operator,
                    arguments.get(1).accept(this, context).getDefinition()));
        }
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("Unknown logical binary: '%s'", call));
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
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("function %s not supported in filter", call));
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
