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
    private final List<ClientTypeSignatureParameter> typeArguments;

    public ClientTypeSignature(TypeSignature typeSignature)
    {
        this(
                typeSignature.getBase(),
                Lists.transform(typeSignature.getParameters(), ClientTypeSignatureParameter::new));
    }

    @JsonCreator
    public ClientTypeSignature(
            @JsonProperty("rawType") String rawType,
            @JsonProperty("typeArguments") List<ClientTypeSignatureParameter> typeArguments)
    {
        checkArgument(rawType != null, "rawType is null");
        this.rawType = rawType;
        checkArgument(!rawType.isEmpty(), "rawType is empty");
        checkArgument(!PATTERN.matcher(rawType).matches(), "Bad characters in rawType type: %s", rawType);
        checkArgument(typeArguments != null, "typeArguments is null");
        this.typeArguments = unmodifiableList(new ArrayList<>(typeArguments));
    }

    @JsonProperty
    public String getRawType()
    {
        return rawType;
    }

    @JsonProperty
    public List<ClientTypeSignatureParameter> getTypeArguments()
    {
        return typeArguments;
    }

    @Override
    public String toString()
    {
        if (rawType.equals(StandardTypes.ROW)) {
            return rowToString();
        }
        else {
            StringBuilder typeName = new StringBuilder(rawType);
            if (!typeArguments.isEmpty()) {
                typeName.append("(");
                boolean first = true;
                for (ClientTypeSignatureParameter argument : typeArguments) {
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
        String types = typeArguments.stream()
                .map(ClientTypeSignatureParameter::getNamedTypeSignature)
                .map(NamedTypeSignature::getTypeSignature)
                .map(TypeSignature::toString)
                .collect(Collectors.joining(","));

        String fieldNames = typeArguments.stream()
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
                Objects.equals(this.typeArguments, other.typeArguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rawType.toLowerCase(Locale.ENGLISH), typeArguments);
    }
}
