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
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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

    @JsonCreator
    public ClientTypeSignature(
            @JsonProperty("rawType") String rawType,
            @JsonProperty("arguments") List<ClientTypeSignatureParameter> arguments)
    {
        checkArgument(rawType != null, "rawType is null");
        this.rawType = rawType;
        checkArgument(!rawType.isEmpty(), "rawType is empty");
        checkArgument(!PATTERN.matcher(rawType).matches(), "Bad characters in rawType type: %s", rawType);
        checkArgument(arguments != null, "arguments is null");
        this.arguments = unmodifiableList(new ArrayList<>(arguments));
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
                case TYPE_SIGNATURE:
                    result.add(argument.getTypeSignature());
                    break;
                case NAMED_TYPE_SIGNATURE:
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
                case NAMED_TYPE_SIGNATURE:
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
        String types = arguments.stream()
                .map(ClientTypeSignatureParameter::getNamedTypeSignature)
                .map(NamedTypeSignature::getTypeSignature)
                .map(TypeSignature::toString)
                .collect(Collectors.joining(","));

        String fieldNames = arguments.stream()
                .map(ClientTypeSignatureParameter::getNamedTypeSignature)
                .map(NamedTypeSignature::getName)
                .map(name -> format("'%s'", name))
                .collect(Collectors.joining(","));

        return format("row<%s>(%s)", types, fieldNames);
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
