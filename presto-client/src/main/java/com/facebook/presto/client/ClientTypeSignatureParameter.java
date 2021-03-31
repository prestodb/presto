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

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.type.BigintEnumType.LongEnumMap;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.ParameterKind;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarcharEnumType.VarcharEnumMap;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import javax.annotation.concurrent.Immutable;

import java.io.IOException;
import java.util.Objects;

import static java.lang.String.format;

@Immutable
@JsonDeserialize(using = ClientTypeSignatureParameter.ClientTypeSignatureParameterDeserializer.class)
public class ClientTypeSignatureParameter
{
    private final ParameterKind kind;
    private final Object value;

    public ClientTypeSignatureParameter(TypeSignatureParameter typeParameterSignature)
    {
        this.kind = typeParameterSignature.getKind();
        switch (kind) {
            case TYPE:
                value = new ClientTypeSignature(typeParameterSignature.getTypeSignature());
                break;
            case LONG:
                value = typeParameterSignature.getLongLiteral();
                break;
            case NAMED_TYPE:
                value = typeParameterSignature.getNamedTypeSignature();
                break;
            case LONG_ENUM:
                value = typeParameterSignature.getLongEnumMap();
                break;
            case VARCHAR_ENUM:
                value = typeParameterSignature.getVarcharEnumMap();
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
        return getValue(ParameterKind.TYPE, ClientTypeSignature.class);
    }

    public Long getLongLiteral()
    {
        return getValue(ParameterKind.LONG, Long.class);
    }

    public NamedTypeSignature getNamedTypeSignature()
    {
        return getValue(ParameterKind.NAMED_TYPE, NamedTypeSignature.class);
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

    public static class ClientTypeSignatureParameterDeserializer
            extends JsonDeserializer<ClientTypeSignatureParameter>
    {
        private static final ObjectMapper MAPPER = new JsonObjectMapperProvider().get();

        @Override
        public ClientTypeSignatureParameter deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException
        {
            JsonNode node = jp.getCodec().readTree(jp);
            ParameterKind kind = MAPPER.readValue(MAPPER.treeAsTokens(node.get("kind")), ParameterKind.class);
            JsonParser jsonValue = MAPPER.treeAsTokens(node.get("value"));
            Object value;
            switch (kind) {
                case TYPE:
                    value = MAPPER.readValue(jsonValue, ClientTypeSignature.class);
                    break;
                case NAMED_TYPE:
                    value = MAPPER.readValue(jsonValue, NamedTypeSignature.class);
                    break;
                case LONG:
                    value = MAPPER.readValue(jsonValue, Long.class);
                    break;
                case LONG_ENUM:
                    value = MAPPER.readValue(jsonValue, LongEnumMap.class);
                    break;
                case VARCHAR_ENUM:
                    value = MAPPER.readValue(jsonValue, VarcharEnumMap.class);
                    break;
                default:
                    throw new UnsupportedOperationException(format("Unsupported kind [%s]", kind));
            }
            return new ClientTypeSignatureParameter(kind, value);
        }
    }
}
