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
package com.facebook.presto.client;

import com.facebook.presto.spi.type.ParameterKind;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static java.lang.String.format;

@Immutable
public class ClientTypeSignatureParameter
{
    private final ParameterKind kind;
    private final Object value;

    public ClientTypeSignatureParameter(TypeSignatureParameter typeParameterSignature)
    {
        this.kind = typeParameterSignature.getKind();
        switch (kind) {
            case TYPE_SIGNATURE:
                value = new ClientTypeSignature(typeParameterSignature.getTypeSignature());
                break;
            case LONG_LITERAL:
                value = typeParameterSignature.getLongLiteral();
                break;
            default:
                throw new UnsupportedOperationException(format("Unknown kind [%s]", kind));
        }
    }

    @JsonCreator
    public ClientTypeSignatureParameter(
            @JsonProperty("kind") ParameterKind kind,
            @JsonProperty("value") Object value)
    {
        this.kind = kind;
        this.value = value;
    }

    @JsonProperty
    public ParameterKind getKind()
    {
        return kind;
    }

    @JsonProperty
    public Object getValue()
    {
        return value;
    }

    private <A> A getValue(ParameterKind expectedParameterKind, Class<A> target)
    {
        if (kind != expectedParameterKind) {
            throw new IllegalArgumentException(format("ParameterKind is [%s] but expected [%s]", kind, expectedParameterKind));
        }
        return target.cast(value);
    }

    public ClientTypeSignature getTypeSignature()
    {
        return getValue(ParameterKind.TYPE_SIGNATURE, ClientTypeSignature.class);
    }

    public Long getLongLiteral()
    {
        return getValue(ParameterKind.LONG_LITERAL, Long.class);
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

        ClientTypeSignatureParameter other = (ClientTypeSignatureParameter) o;

        return Objects.equals(this.kind, other.kind) &&
                Objects.equals(this.value, other.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind, value);
    }
}
