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

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;

public class TypeSignature
{
    private final String base;
    private final List<TypeSignature> parameters;
    private final List<Object> literalParameters;
    private final boolean calculated;

    public TypeSignature(String base, List<TypeSignature> parameters, List<Object> literalParameters)
    {
        checkArgument(base != null, "base is null");
        this.base = base;
        checkArgument(!base.isEmpty(), "base is empty");
        checkArgument(validateName(base), "Bad characters in base type: %s", base);
        checkArgument(parameters != null, "parameters is null");
        checkArgument(literalParameters != null, "literalParameters is null");
        for (Object literal : literalParameters) {
            checkArgument(literal instanceof String || literal instanceof Long || literal instanceof TypeLiteralCalculation, "Unsupported literal type: %s", literal.getClass());
        }
        this.parameters = unmodifiableList(new ArrayList<>(parameters));
        this.literalParameters = unmodifiableList(new ArrayList<>(literalParameters));

        this.calculated = parameters.stream().anyMatch(TypeSignature::isCalculated) ||
                literalParameters.stream().anyMatch(TypeLiteralCalculation.class::isInstance);
    }

    private static boolean validateName(String name)
    {
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c == '<' || c == '>' || c == ',') {
                return false;
            }
        }
        return true;
    }

    @Override
    @JsonValue
    public String toString()
    {
        StringBuilder typeName = new StringBuilder(base);
        if (!parameters.isEmpty()) {
            typeName.append("<");
            boolean first = true;
            for (TypeSignature parameter : parameters) {
                if (!first) {
                    typeName.append(",");
                }
                first = false;
                typeName.append(parameter.toString());
            }
            typeName.append(">");
        }
        if (!literalParameters.isEmpty()) {
            typeName.append("(");
            boolean first = true;
            for (Object parameter : literalParameters) {
                if (!first) {
                    typeName.append(",");
                }
                first = false;
                if (parameter instanceof String) {
                    typeName.append("'").append(parameter).append("'");
                }
                else {
                    typeName.append(parameter.toString());
                }
            }
            typeName.append(")");
        }

        return typeName.toString();
    }

    public String getBase()
    {
        return base;
    }

    public List<TypeSignature> getParameters()
    {
        return parameters;
    }

    public List<Object> getLiteralParameters()
    {
        return literalParameters;
    }

    public boolean isCalculated()
    {
        return calculated;
    }

    @JsonCreator
    public static TypeSignature parseTypeSignature(String signature)
    {
        if (!signature.contains("<") && !signature.contains("(")) {
            return new TypeSignature(signature, new ArrayList<TypeSignature>(), new ArrayList<>());
        }

        String baseName = null;
        List<TypeSignature> parameters = new ArrayList<>();
        List<Object> literalParameters = new ArrayList<>();
        int parameterStart = -1;
        int angleBracketDepth = 0;
        int roundBracketDepth = 0;

        for (int i = 0; i < signature.length(); i++) {
            char c = signature.charAt(i);
            if (c == '<') {
                if (angleBracketDepth == 0) {
                    verify(baseName == null, "Expected baseName to be null");
                    verify(parameterStart == -1, "Expected parameter start to be -1");
                    baseName = signature.substring(0, i);
                    parameterStart = i + 1;
                }
                angleBracketDepth++;
            }
            else if (c == '>') {
                angleBracketDepth--;
                checkArgument(angleBracketDepth >= 0, "Bad type signature: '%s'", signature);
                if (angleBracketDepth == 0) {
                    checkArgument(parameterStart >= 0, "Bad type signature: '%s'", signature);
                    parameters.add(parseTypeSignature(signature.substring(parameterStart, i)));
                    parameterStart = i + 1;
                    if (i == signature.length() - 1) {
                        return new TypeSignature(baseName, parameters, literalParameters);
                    }
                }
            }
            else if (c == ',' && roundBracketDepth <= 1) {
                if (angleBracketDepth == 1 && roundBracketDepth == 0) {
                    checkArgument(parameterStart >= 0, "Bad type signature: '%s'", signature);
                    parameters.add(parseTypeSignature(signature.substring(parameterStart, i)));
                    parameterStart = i + 1;
                }
                else if (angleBracketDepth == 0 && roundBracketDepth == 1) {
                    checkArgument(parameterStart >= 0, "Bad type signature: '%s'", signature);
                    literalParameters.add(parseLiteral(signature.substring(parameterStart, i)));
                    parameterStart = i + 1;
                }
            }
            else if (c == '(' && roundBracketDepth++ == 0) {
                if (angleBracketDepth == 0) {
                    if (baseName == null) {
                        verify(parameters.isEmpty(), "Expected no parameters");
                        verify(parameterStart == -1, "Expected parameter start to be -1");
                        baseName = signature.substring(0, i);
                    }
                    parameterStart = i + 1;
                }
            }
            else if (c == ')' && --roundBracketDepth == 0) {
                if (angleBracketDepth == 0) {
                    checkArgument(i == signature.length() - 1, "Bad type signature: '%s'", signature);
                    checkArgument(parameterStart >= 0, "Bad type signature: '%s'", signature);
                    literalParameters.add(parseLiteral(signature.substring(parameterStart, i)));
                    return new TypeSignature(baseName, parameters, literalParameters);
                }
            }
        }

        throw new IllegalArgumentException(format("Bad type signature: '%s'", signature));
    }

    private static Object parseLiteral(String literal)
    {
        checkArgument(literal != null && !literal.isEmpty(), "Bad literal: '%s'", literal);

        if (literal.startsWith("'") || literal.endsWith("'")) {
            checkArgument(literal.startsWith("'") && literal.endsWith("'"), "Bad literal: '%s'", literal);
            return literal.substring(1, literal.length() - 1);
        }
        try {
            return Long.parseLong(literal);
        }
        catch (NumberFormatException ignored) {
        }
        return new TypeLiteralCalculation(literal);
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
                Objects.equals(this.parameters, other.parameters) &&
                Objects.equals(this.literalParameters, other.literalParameters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(base.toLowerCase(Locale.ENGLISH), parameters, literalParameters);
    }

    private static void checkArgument(boolean argument, String format, Object...args)
    {
        if (!argument) {
            throw new IllegalArgumentException(format(format, args));
        }
    }

    private static void verify(boolean argument, String message)
    {
        if (!argument) {
            throw new AssertionError(message);
        }
    }
}
