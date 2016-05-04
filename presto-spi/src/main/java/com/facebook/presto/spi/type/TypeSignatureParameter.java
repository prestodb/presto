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
package com.facebook.presto.spi.type;

import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TypeSignatureParameter
{
    private final ParameterKind kind;
    private final Object value;

    public static TypeSignatureParameter of(TypeSignature typeSignature)
    {
        return new TypeSignatureParameter(ParameterKind.TYPE, typeSignature);
    }

    public static TypeSignatureParameter of(long longLiteral)
    {
        return new TypeSignatureParameter(ParameterKind.LONG, longLiteral);
    }

    public static TypeSignatureParameter of(NamedTypeSignature namedTypeSignature)
    {
        return new TypeSignatureParameter(ParameterKind.NAMED_TYPE, namedTypeSignature);
    }

    public static TypeSignatureParameter of(String variable)
    {
        return new TypeSignatureParameter(ParameterKind.VARIABLE, variable);
    }

    private TypeSignatureParameter(ParameterKind kind, Object value)
    {
        this.kind = requireNonNull(kind, "kind is null");
        this.value = requireNonNull(value, "value is null");
    }

    @Override
    public String toString()
    {
        return value.toString();
    }

    public ParameterKind getKind()
    {
        return kind;
    }

    public boolean isTypeSignature()
    {
        return kind == ParameterKind.TYPE;
    }

    public boolean isLongLiteral()
    {
        return kind == ParameterKind.LONG;
    }

    public boolean isNamedTypeSignature()
    {
        return kind == ParameterKind.NAMED_TYPE;
    }

    public boolean isVariable()
    {
        return kind == ParameterKind.VARIABLE;
    }

    private <A> A getValue(ParameterKind expectedParameterKind, Class<A> target)
    {
        if (kind != expectedParameterKind) {
            throw new IllegalArgumentException(format("ParameterKind is [%s] but expected [%s]", kind, expectedParameterKind));
        }
        return target.cast(value);
    }

    public TypeSignature getTypeSignature()
    {
        return getValue(ParameterKind.TYPE, TypeSignature.class);
    }

    public Long getLongLiteral()
    {
        return getValue(ParameterKind.LONG, Long.class);
    }

    public NamedTypeSignature getNamedTypeSignature()
    {
        return getValue(ParameterKind.NAMED_TYPE, NamedTypeSignature.class);
    }

    public String getVariable()
    {
        return getValue(ParameterKind.VARIABLE, String.class);
    }

    public Optional<TypeSignature> getTypeSignatureOrNamedTypeSignature()
    {
        switch (kind) {
            case TYPE:
                return Optional.of(getTypeSignature());
            case NAMED_TYPE:
                return Optional.of(getNamedTypeSignature().getTypeSignature());
            default:
                return Optional.empty();
        }
    }

    public boolean isCalculated()
    {
        switch (kind) {
            case TYPE:
                return getTypeSignature().isCalculated();
            case NAMED_TYPE:
                return getNamedTypeSignature().getTypeSignature().isCalculated();
            case LONG:
                return false;
            case VARIABLE:
                return true;
            default:
                throw new IllegalArgumentException("Unexpected parameter kind: " + kind);
        }
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

        TypeSignatureParameter other = (TypeSignatureParameter) o;

        return Objects.equals(this.kind, other.kind) &&
                Objects.equals(this.value, other.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind, value);
    }
}
