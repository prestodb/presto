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

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Selection;
import com.facebook.presto.spi.function.FunctionHandle;
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

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class PinotProjectExpressionConverter
        implements RowExpressionVisitor<PinotExpression, Map<VariableReferenceExpression, Selection>>
{
    private static final Set<String> TIME_EQUIVALENT_TYPES = ImmutableSet.of(StandardTypes.BIGINT, StandardTypes.INTEGER, StandardTypes.TINYINT, StandardTypes.SMALLINT);

    protected final TypeManager typeManager;
    protected final StandardFunctionResolution standardFunctionResolution;

    public PinotProjectExpressionConverter(
            TypeManager typeManager,
            StandardFunctionResolution standardFunctionResolution)
    {
        this.typeManager = requireNonNull(typeManager, "type manager");
        this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standardFunctionResolution is null");
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

    private PinotExpression handleCast(
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

    protected Optional<PinotExpression> basicCallHandling(CallExpression call, Map<VariableReferenceExpression, Selection> context)
    {
        FunctionHandle functionHandle = call.getFunctionHandle();
        if (standardFunctionResolution.isNotFunction(functionHandle) || standardFunctionResolution.isBetweenFunction(functionHandle)) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Unsupported function in pinot aggregation: " + functionHandle);
        }
        if (standardFunctionResolution.isCastFunction(functionHandle)) {
            return Optional.of(handleCast(call, context));
        }
        return Optional.empty();
    }

    @Override
    public PinotExpression visitInputReference(
            InputReferenceExpression reference,
            Map<VariableReferenceExpression, Selection> context)
    {
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Input reference not supported: " + reference);
    }

    @Override
    public PinotExpression visitSpecialForm(
            SpecialFormExpression specialForm,
            Map<VariableReferenceExpression, Selection> context)
    {
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Special form not supported: " + specialForm);
    }

    @Override
    public PinotExpression visitCall(CallExpression call, Map<VariableReferenceExpression, Selection> context)
    {
        return basicCallHandling(call, context).orElseThrow(() -> new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Call not supported: " + call));
    }

    @Override
    public PinotExpression visitConstant(ConstantExpression literal, Map<VariableReferenceExpression, Selection> context)
    {
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Constant not supported: " + literal);
    }
}
