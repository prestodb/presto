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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.pinot.PinotSessionProperties;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Selection;
import com.facebook.presto.spi.ConnectorSession;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.pinot.PinotPushdownUtils.getLiteralAsString;
import static com.facebook.presto.pinot.query.PinotExpression.derived;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class PinotProjectExpressionConverter
        implements RowExpressionVisitor<PinotExpression, Map<VariableReferenceExpression, Selection>>
{
    private static final Set<String> LOGICAL_BINARY_OPS_FILTER = ImmutableSet.of("=", "<", "<=", ">", ">=", "<>");
    // Pinot does not support modulus yet
    private static final Map<String, String> PRESTO_TO_PINOT_OPERATORS = ImmutableMap.of(
            "-", "SUB",
            "+", "ADD",
            "*", "MULT",
            "/", "DIV");
    private static final Set<String> TIME_EQUIVALENT_TYPES = ImmutableSet.of(StandardTypes.BIGINT, StandardTypes.INTEGER, StandardTypes.TINYINT, StandardTypes.SMALLINT);

    protected final TypeManager typeManager;
    protected final FunctionMetadataManager functionMetadataManager;
    protected final StandardFunctionResolution standardFunctionResolution;
    protected final ConnectorSession session;

    public PinotProjectExpressionConverter(
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution,
            ConnectorSession session)
    {
        this.typeManager = requireNonNull(typeManager, "type manager");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager");
        this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standardFunctionResolution is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public PinotExpression visitVariableReference(
            VariableReferenceExpression reference,
            Map<VariableReferenceExpression, Selection> context)
    {
        Selection input = requireNonNull(context.get(reference), format("Input column %s does not exist in the input", reference));
        return new PinotExpression(input.getDefinition(), input.getOrigin());
    }

    @Override
    public PinotExpression visitLambda(
            LambdaDefinitionExpression lambda,
            Map<VariableReferenceExpression, Selection> context)
    {
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Pinot does not support lambda " + lambda);
    }

    protected boolean isImplicitCast(Type inputType, Type resultType)
    {
        if (typeManager.canCoerce(inputType, resultType)) {
            return true;
        }
        return resultType.getTypeSignature().getBase().equals(StandardTypes.TIMESTAMP) && TIME_EQUIVALENT_TYPES.contains(inputType.getTypeSignature().getBase());
    }
    protected PinotExpression handleArithmeticExpression(
            CallExpression expression,
            OperatorType operatorType,
            Map<VariableReferenceExpression, PinotQueryGeneratorContext.Selection> context)
    {
        List<RowExpression> arguments = expression.getArguments();
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

    protected PinotExpression handleCast(
            CallExpression cast,
            Map<VariableReferenceExpression, Selection> context)
    {
        if (cast.getArguments().size() == 1) {
            RowExpression input = cast.getArguments().get(0);
            Type expectedType = cast.getType();
            if (isImplicitCast(input.getType(), expectedType)) {
                return input.accept(this, context);
            }
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Non implicit casts not supported: " + cast);
        }

        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("This type of CAST operator not supported. Received: %s", cast));
    }

    // Borrowed from filter expr; doesn't handle date/time column and constant comparison
    private PinotExpression handleLogicalBinary(
            CallExpression call,
            String operator,
            Map<VariableReferenceExpression, Selection> context)
    {
        if (!LOGICAL_BINARY_OPS_FILTER.contains(operator)) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("'%s' is not supported in filter", operator));
        }
        List<RowExpression> arguments = call.getArguments();
        if (arguments.size() == 2) {
            return derived(format(
                    "(%s %s %s)",
                    getExpressionOrConstantString(arguments.get(0), context),
                    operator,
                    getExpressionOrConstantString(arguments.get(1), context)));
        }
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("Unknown logical binary: '%s'", call));
    }

    protected String getExpressionOrConstantString(
            RowExpression expression,
            Map<VariableReferenceExpression, Selection> context)
    {
        if (expression instanceof ConstantExpression) {
            return new PinotExpression(getLiteralAsString((ConstantExpression) expression),
                    PinotQueryGeneratorContext.Origin.LITERAL).getDefinition();
        }
        return expression.accept(this, context).getDefinition();
    }

    @Override
    public PinotExpression visitInputReference(
            InputReferenceExpression reference,
            Map<VariableReferenceExpression, Selection> context)
    {
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Pinot does not support struct dereferencing: " + reference);
    }

    @Override
    public PinotExpression visitSpecialForm(
            SpecialFormExpression specialForm,
            Map<VariableReferenceExpression, Selection> context)
    {
        if (!PinotSessionProperties.getPushdownProjectExpressions(session)) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Special form not supported: " + specialForm);
        }
        switch (specialForm.getForm()) {
            // (SWITCH <expr> (WHEN <expr> <expr>) (WHEN <expr> <expr>) <expr>)
            // Presto generates "simple" CASE expressions from the "searched" form with true as pattern to match
            case SWITCH:
                int numArguments = specialForm.getArguments().size();
                String searchExpression = getExpressionOrConstantString(specialForm.getArguments().get(0), context);

                return derived(format(
                        "CASE %s %s ELSE %s END",
                        searchExpression,
                        specialForm.getArguments().subList(1, numArguments - 1).stream()
                                .map(argument -> argument.accept(this, context).getDefinition())
                                .collect(Collectors.joining(" ")),
                        getExpressionOrConstantString(specialForm.getArguments().get(numArguments - 1), context)));
            case WHEN:
                return derived(format(
                        "%s %s THEN %s",
                        specialForm.getForm().toString(),
                        getExpressionOrConstantString(specialForm.getArguments().get(0), context),
                        getExpressionOrConstantString(specialForm.getArguments().get(1), context)));
            case IF:
            case NULL_IF:
            case DEREFERENCE:
            case ROW_CONSTRUCTOR:
            case BIND:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Pinot does not support the special form" + specialForm);
            case IN:
            case AND:
            case OR:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Special form not supported: " + specialForm);
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Unexpected special form: " + specialForm);
        }
    }

    @Override
    public PinotExpression visitCall(CallExpression call, Map<VariableReferenceExpression, Selection> context)
    {
        FunctionHandle functionHandle = call.getFunctionHandle();
        if (standardFunctionResolution.isCastFunction(functionHandle)) {
            return handleCast(call, context);
        }
        if (!PinotSessionProperties.getPushdownProjectExpressions(session)) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Call not supported: " + call);
        }
        FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(call.getFunctionHandle());
        Optional<OperatorType> operatorType = functionMetadata.getOperatorType();
        if (standardFunctionResolution.isComparisonFunction(functionHandle) && operatorType.isPresent()) {
            return handleLogicalBinary(call, operatorType.get().getOperator(), context);
        }
        if (standardFunctionResolution.isArithmeticFunction(functionHandle) && operatorType.isPresent()) {
            return handleArithmeticExpression(call, operatorType.get(), context);
        }
        if (standardFunctionResolution.isNegateFunction(functionHandle)) {
            return derived('-' + call.getArguments().get(0).accept(this, context).getDefinition());
        }
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Call not supported: " + call);
    }

    @Override
    public PinotExpression visitConstant(ConstantExpression literal, Map<VariableReferenceExpression, Selection> context)
    {
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Constant not supported: " + literal);
    }
}
