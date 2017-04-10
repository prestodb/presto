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

import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.ParameterKind;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@Immutable
public class ClientTypeSignature
{
    private static final Pattern PATTERN = Pattern.compile(".*[<>,].*");
    private final String rawType;
    private final List<ClientTypeSignatureParameter> arguments;

    public ClientTypeSignature(TypeSignature typeSignature)
    {
        this(
                typeSignature.getBase(),
                Lists.transform(typeSignature.getParameters(), ClientTypeSignatureParameter::new));
    }

    public ClientTypeSignature(String rawType, List<ClientTypeSignatureParameter> arguments)
    {
        this(rawType, ImmutableList.of(), ImmutableList.of(), arguments);
    }

    @JsonCreator
    public ClientTypeSignature(
            @JsonProperty("rawType") String rawType,
            @JsonProperty("typeArguments") List<ClientTypeSignature> typeArguments,
            @JsonProperty("literalArguments") List<Object> literalArguments,
            @JsonProperty("arguments") List<ClientTypeSignatureParameter> arguments)
    {
        requireNonNull(rawType, "rawType is null");
        this.rawType = rawType;
        checkArgument(!rawType.isEmpty(), "rawType is empty");
        checkArgument(!PATTERN.matcher(rawType).matches(), "Bad characters in rawType type: %s", rawType);
        if (arguments != null) {
            this.arguments = unmodifiableList(new ArrayList<>(arguments));
        }
        else {
            requireNonNull(typeArguments, "typeArguments is null");
            requireNonNull(literalArguments, "literalArguments is null");
            ImmutableList.Builder<ClientTypeSignatureParameter> convertedArguments = ImmutableList.builder();
            // Talking to a legacy server (< 0.133)
            if (rawType.equals(StandardTypes.ROW)) {
                checkArgument(typeArguments.size() == literalArguments.size());
                for (int i = 0; i < typeArguments.size(); i++) {
                    Object value = literalArguments.get(i);
                    checkArgument(value instanceof String, "Expected literalArgument %d in %s to be a string", i, literalArguments);
                    convertedArguments.add(new ClientTypeSignatureParameter(TypeSignatureParameter.of(new NamedTypeSignature((String) value, toTypeSignature(typeArguments.get(i))))));
                }
            }
            else {
                checkArgument(literalArguments.isEmpty(), "Unexpected literal arguments from legacy server");
                for (ClientTypeSignature typeArgument : typeArguments) {
                    convertedArguments.add(new ClientTypeSignatureParameter(ParameterKind.TYPE, typeArgument));
                }
            }
            this.arguments = convertedArguments.build();
        }
    }

    private static TypeSignature toTypeSignature(ClientTypeSignature signature)
    {
        List<TypeSignatureParameter> parameters = signature.getArguments().stream()
                .map(ClientTypeSignature::legacyClientTypeSignatureParameterToTypeSignatureParameter)
                .collect(toList());
        return new TypeSignature(signature.getRawType(), parameters);
    }

    private static TypeSignatureParameter legacyClientTypeSignatureParameterToTypeSignatureParameter(ClientTypeSignatureParameter parameter)
    {
        switch (parameter.getKind()) {
            case LONG:
                throw new UnsupportedOperationException("Unexpected long type literal returned by legacy server");
            case TYPE:
                return TypeSignatureParameter.of(toTypeSignature(parameter.getTypeSignature()));
            case NAMED_TYPE:
                return TypeSignatureParameter.of(parameter.getNamedTypeSignature());
            default:
                throw new UnsupportedOperationException("Unknown parameter kind " + parameter.getKind());
        }
    }

    @JsonProperty
    public String getRawType()
    {
        return rawType;
    }

    @JsonProperty
    public List<ClientTypeSignatureParameter> getArguments()
    {
        return arguments;
    }

    /**
     * This field is deprecated and clients should switch to {@link #getArguments()}
     */
    @Deprecated
    @JsonProperty
    public List<ClientTypeSignature> getTypeArguments()
    {
        List<ClientTypeSignature> result = new ArrayList<>();
        for (ClientTypeSignatureParameter argument : arguments) {
            switch (argument.getKind()) {
                case TYPE:
                    result.add(argument.getTypeSignature());
                    break;
                case NAMED_TYPE:
                    result.add(new ClientTypeSignature(argument.getNamedTypeSignature().getTypeSignature()));
                    break;
                default:
                    return new ArrayList<>();
            }
        }
        return result;
    }

    /**
     * This field is deprecated and clients should switch to {@link #getArguments()}
     */
    @Deprecated
    @JsonProperty
    public List<Object> getLiteralArguments()
    {
        List<Object> result = new ArrayList<>();
        for (ClientTypeSignatureParameter argument : arguments) {
            switch (argument.getKind()) {
                case NAMED_TYPE:
                    result.add(argument.getNamedTypeSignature().getName());
                    break;
                default:
                    return new ArrayList<>();
            }
        }
        return result;
    }

    @Override
    public String toString()
    {
        if (rawType.equals(StandardTypes.ROW)) {
            return rowToString();
        }
        else {
            StringBuilder typeName = new StringBuilder(rawType);
            if (!arguments.isEmpty()) {
                typeName.append("(");
                boolean first = true;
                for (ClientTypeSignatureParameter argument : arguments) {
                    if (!first) {
                        typeName.append(",");
                    }
                    first = false;
                    typeName.append(argument.toString());
                }
                typeName.append(")");
            }
            return typeName.toString();
        }
    }

    @Deprecated
    private String rowToString()
    {
        String fields = arguments.stream()
                .map(ClientTypeSignatureParameter::getNamedTypeSignature)
                .map(parameter -> format("%s %s", parameter.getName(), parameter.getTypeSignature().toString()))
                .collect(Collectors.joining(","));

        return format("row(%s)", fields);
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

        ClientTypeSignature other = (ClientTypeSignature) o;

        return Objects.equals(this.rawType.toLowerCase(Locale.ENGLISH), other.rawType.toLowerCase(Locale.ENGLISH)) &&
                Objects.equals(this.arguments, other.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rawType.toLowerCase(Locale.ENGLISH), arguments);
    }
}
