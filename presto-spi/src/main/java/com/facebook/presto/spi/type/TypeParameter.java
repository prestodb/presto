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

import static java.lang.String.format;

public class TypeParameter
{
    private final ParameterKind kind;
    private final Object value;

    private TypeParameter(ParameterKind kind, Object value)
    {
        this.kind = kind;
        this.value = value;
    }

    public static TypeParameter of(Type type)
    {
        return new TypeParameter(ParameterKind.TYPE_SIGNATURE, type);
    }

    public static TypeParameter of(long longLiteral)
    {
        return new TypeParameter(ParameterKind.LONG_LITERAL, longLiteral);
    }

    public static TypeParameter of(NamedType namedType)
    {
        return new TypeParameter(ParameterKind.NAMED_TYPE_SIGNATURE, namedType);
    }

    public static TypeParameter of(TypeSignatureParameter parameter, TypeManager typeManager)
    {
        switch (parameter.getKind()) {
            case TYPE_SIGNATURE: {
                Type type = typeManager.getType(parameter.getTypeSignature());
                if (type == null) {
                    return null;
                }
                return of(type);
            }
            case LONG_LITERAL:
                return of(parameter.getLongLiteral());
            case NAMED_TYPE_SIGNATURE: {
                Type type = typeManager.getType(parameter.getNamedTypeSignature().getTypeSignature());
                if (type == null) {
                    return null;
                }
                return of(new NamedType(
                        parameter.getNamedTypeSignature().getName(),
                        type));
            }
            default:
                throw new UnsupportedOperationException(format("Unsupported parameter [%s]", parameter));
        }
    }

    public ParameterKind getKind()
    {
        return kind;
    }

    public <A> A getValue(ParameterKind expectedParameterKind, Class<A> target)
    {
        if (kind != expectedParameterKind) {
            throw new AssertionError(format("ParameterKind is [%s] but expected [%s]", kind, expectedParameterKind));
        }
        return target.cast(value);
    }

    public Type getType()
    {
        return getValue(ParameterKind.TYPE_SIGNATURE, Type.class);
    }

    public Long getLongLiteral()
    {
        return getValue(ParameterKind.LONG_LITERAL, Long.class);
    }

    public NamedType getNamedType()
    {
        return getValue(ParameterKind.NAMED_TYPE_SIGNATURE, NamedType.class);
    }

    @Override
    public String toString()
    {
        return value.toString();
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

        TypeParameter other = (TypeParameter) o;

        return Objects.equals(this.kind, other.kind) &&
                Objects.equals(this.value, other.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind, value);
    }
}
