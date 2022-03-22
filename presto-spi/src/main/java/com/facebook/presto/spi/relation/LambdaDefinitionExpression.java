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
package com.facebook.presto.spi.relation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.FunctionType;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

@Immutable
public final class LambdaDefinitionExpression
        extends RowExpression
{
    private final List<Type> argumentTypes;
    private final List<String> arguments;
    private final RowExpression body;
    private final String canonicalizedBody;

    @JsonCreator
    public LambdaDefinitionExpression(
            @JsonProperty("argumentTypes") List<Type> argumentTypes,
            @JsonProperty("arguments") List<String> arguments,
            @JsonProperty("body") RowExpression body)
    {
        this.argumentTypes = unmodifiableList(new ArrayList<>(requireNonNull(argumentTypes, "argumentTypes is null")));
        this.arguments = unmodifiableList(new ArrayList<>(requireNonNull(arguments, "arguments is null")));
        checkArgument(argumentTypes.size() == arguments.size(), "Number of argument types does not match number of arguments");
        this.body = requireNonNull(body, "body is null");
        this.canonicalizedBody = body.accept(new CanonicalizeExpression(arguments, argumentTypes), null);
    }

    @JsonProperty
    public List<Type> getArgumentTypes()
    {
        return argumentTypes;
    }

    @JsonProperty
    public List<String> getArguments()
    {
        return arguments;
    }

    @JsonProperty
    public RowExpression getBody()
    {
        return body;
    }

    @Override
    public Type getType()
    {
        return new FunctionType(argumentTypes, body.getType());
    }

    @Override
    public String toString()
    {
        return "(" + String.join(",", arguments) + ") -> " + body;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LambdaDefinitionExpression that = (LambdaDefinitionExpression) o;
        return Objects.equals(argumentTypes, that.argumentTypes) &&
                Objects.equals(canonicalizedBody, that.canonicalizedBody);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(argumentTypes, canonicalizedBody);
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitLambda(this, context);
    }

    private static void checkArgument(boolean condition, String message, Object... messageArgs)
    {
        if (!condition) {
            throw new IllegalArgumentException(format(message, messageArgs));
        }
    }
    private static class CanonicalizeExpression
            implements RowExpressionVisitor<String, Void>
    {
        private final Map<String, String> canonicalizedArguments = new HashMap<>();

        public CanonicalizeExpression(List<String> arguments, List<Type> argumentTypes)
        {
            for (int i = 0; i < arguments.size(); i++) {
                canonicalizedArguments.put(arguments.get(i), format("%s_%d", argumentTypes.get(i).toString(), i));
            }
        }

        @Override
        public String visitCall(CallExpression call, Void context)
        {
            return format("%s.%s(%s):%s", call.getFunctionHandle().getCatalogSchemaName(), call.getDisplayName(), String.join(", ", call.getArguments().stream().map(e -> e.accept(this, null)).collect(Collectors.toList())), call.getType());
        }

        @Override
        public String visitInputReference(InputReferenceExpression reference, Void context)
        {
            return reference.toString();
        }

        @Override
        public String visitConstant(ConstantExpression literal, Void context)
        {
            // Handle varchar and varbinary constant specifically. We canonicalize Slice the same way as in LiteralEncoder by using the same logic as function from_base64.
            if (literal.getValue() instanceof Slice) {
                Slice slice = (Slice) literal.getValue();
                if (slice.hasByteArray()) {
                    return Slices.wrappedBuffer(Base64.getEncoder().encode(slice.toByteBuffer())).toStringUtf8();
                }
                return Slices.wrappedBuffer(Base64.getEncoder().encode(slice.getBytes())).toStringUtf8();
            }
            if (literal.getValue() instanceof Block) {
                return format("%d", literal.hashCode());
            }
            // This would convert number constant to string but for other Object constant (e.g. MAP, ARRAY, ROW) it will not convert to the actual value.
            return literal.toString();
        }

        @Override
        public String visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return format("(%s) -> %s", String.join(", ", lambda.argumentTypes.stream().map(Type::toString).collect(Collectors.toList())), lambda.body.accept(this, null));
        }

        @Override
        public String visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            if (canonicalizedArguments.containsKey(reference.getName())) {
                return canonicalizedArguments.get(reference.getName());
            }
            return reference.getName();
        }

        @Override
        public String visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            return format("%s(%s)", specialForm.getForm(), String.join(", ", specialForm.getArguments().stream().map(e -> e.accept(this, null)).collect(Collectors.toList())));
        }
    }
}
