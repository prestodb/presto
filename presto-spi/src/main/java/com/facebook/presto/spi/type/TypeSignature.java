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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;

public class TypeSignature
{
    private final String base;
    private final List<TypeSignature> parameters;

    public TypeSignature(String base, List<TypeSignature> parameters)
    {
        checkArgument(base != null, "base is null");
        this.base = base;
        checkArgument(!base.isEmpty(), "base is empty");
        checkArgument(!Pattern.matches(".*[<>,].*", base), "Bad characters in base type: %s", base);
        checkArgument(parameters != null, "parameters is null");
        this.parameters = unmodifiableList(new ArrayList<>(parameters));
    }

    @Override
    @JsonValue
    public String toString()
    {
        String typeName = base;
        if (!parameters.isEmpty()) {
            typeName += "<";
            boolean first = true;
            for (TypeSignature parameter : parameters) {
                if (!first) {
                    typeName += ",";
                }
                first = false;
                typeName += parameter.toString();
            }
            typeName += ">";
        }

        return typeName;
    }

    public String getBase()
    {
        return base;
    }

    public List<TypeSignature> getParameters()
    {
        return parameters;
    }

    @JsonCreator
    public static TypeSignature parseTypeSignature(String signature)
    {
        if (!signature.contains("<")) {
            return new TypeSignature(signature, unmodifiableList(new ArrayList<TypeSignature>()));
        }

        String baseName = null;
        List<TypeSignature> parameters = new ArrayList<>();
        int parameterStart = -1;
        int bracketCount = 0;

        for (int i = 0; i < signature.length(); i++) {
            char c = signature.charAt(i);
            if (c == '<') {
                if (bracketCount == 0) {
                    checkState(baseName == null, "Expected baseName to be null");
                    checkState(parameterStart == -1, "Expected parameter start to be -1");
                    baseName = signature.substring(0, i);
                    parameterStart = i + 1;
                }
                bracketCount++;
            }
            else if (c == '>') {
                bracketCount--;
                checkArgument(bracketCount >= 0, "Bad type signature: '%s'", signature);
                if (bracketCount == 0) {
                    checkArgument(i == signature.length() - 1, "Bad type signature: '%s'", signature);
                    checkArgument(parameterStart >= 0, "Bad type signature: '%s'", signature);
                    parameters.add(parseTypeSignature(signature.substring(parameterStart, i)));
                    return new TypeSignature(baseName, parameters);
                }
            }
            else if (c == ',') {
                if (bracketCount == 1) {
                    checkArgument(parameterStart >= 0, "Bad type signature: '%s'", signature);
                    parameters.add(parseTypeSignature(signature.substring(parameterStart, i)));
                    parameterStart = i + 1;
                }
            }
        }

        throw new IllegalArgumentException(format("Bad type signature: '%s'", signature));
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

        TypeSignature other = (TypeSignature) o;

        return Objects.equals(this.base.toLowerCase(Locale.ENGLISH), other.base.toLowerCase(Locale.ENGLISH)) &&
                Objects.equals(this.parameters, other.parameters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(base.toLowerCase(Locale.ENGLISH), parameters);
    }

    private static void checkArgument(boolean argument, String format, Object...args)
    {
        if (!argument) {
            throw new IllegalArgumentException(format(format, args));
        }
    }

    private static void checkState(boolean argument, String format, Object...args)
    {
        if (!argument) {
            throw new IllegalStateException(format(format, args));
        }
    }
}
