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
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

public class TypeSignature
{
    private final String base;
    private final List<TypeSignatureParameter> parameters;
    private final List<Object> literalParameters;

    public TypeSignature(String base, List<TypeSignatureParameter> parameters)
    {
        checkArgument(base != null, "base is null");
        this.base = base;
        checkArgument(!base.isEmpty(), "base is empty");
        checkArgument(validateName(base), "Bad characters in base type: %s", base);
        checkArgument(parameters != null, "parameters is null");
        this.parameters = unmodifiableList(new ArrayList<>(parameters));
        this.literalParameters = unmodifiableList(new ArrayList<>());
    }

    // TODO: merge literalParameters for Row with TypeSignatureParameter
    @Deprecated
    public TypeSignature(String base, List<TypeSignature> typeSignatureParameters, List<Object> literalParameters)
    {
        checkArgument(base != null, "base is null");
        this.base = base;
        checkArgument(!base.isEmpty(), "base is empty");
        checkArgument(validateName(base), "Bad characters in base type: %s", base);
        checkArgument(typeSignatureParameters != null, "parameters is null");
        checkArgument(literalParameters != null, "literalParameters is null");
        for (Object literal : literalParameters) {
            checkArgument(literal instanceof String || literal instanceof Long, "Unsupported literal type: %s", literal.getClass());
        }

        List<TypeSignatureParameter> typeParameters =
                typeSignatureParameters.stream().map(TypeSignatureParameter::of).collect(toList());

        this.parameters = unmodifiableList(typeParameters);
        this.literalParameters = unmodifiableList(new ArrayList<>(literalParameters));
    }

    public TypeSignature bindParameters(Map<String, Type> boundParameters)
    {
        if (boundParameters.containsKey(base)) {
            if (!getLiteralParameters().isEmpty() || !getParameters().isEmpty()) {
                throw new IllegalStateException("Type parameters cannot have parameters");
            }
            return boundParameters.get(base).getTypeSignature();
        }

        if (base.equals(StandardTypes.ROW)) {
            List<TypeSignature> parameters = getTypeParametersAsTypeSignatures().stream()
                    .map(signature -> signature.bindParameters(boundParameters))
                    .collect(toList());
            return new TypeSignature(base, parameters, getLiteralParameters());
        }
        else {
            List<TypeSignatureParameter> parameters = getParameters().stream()
                    .map(signature -> signature.bindParameters(boundParameters))
                    .collect(toList());
            return new TypeSignature(base, parameters);
        }
    }

    @Override
    @JsonValue
    public String toString()
    {
        if (base.equals(StandardTypes.ROW)) {
            return rowToString();
        }
        else {
            return toString("(", ")");
        }
    }

    public String getBase()
    {
        return base;
    }

    public List<TypeSignatureParameter> getParameters()
    {
        return parameters;
    }

    public List<TypeSignature> getTypeParametersAsTypeSignatures()
    {
        List<TypeSignature> result = new ArrayList<>();
        for (TypeSignatureParameter parameter : parameters) {
            if (parameter.getKind() != ParameterKind.TYPE_SIGNATURE) {
                throw new IllegalStateException(
                        format("Expected all parameters to be TypeSignatures but [%s] was found", parameter.toString()));
            }
            result.add(parameter.getTypeSignature());
        }
        return result;
    }

    public List<Object> getLiteralParameters()
    {
        return literalParameters;
    }

    @JsonCreator
    public static TypeSignature parseTypeSignature(String signature)
    {
        if (!signature.contains("<") && !signature.contains("(")) {
            return new TypeSignature(signature, new ArrayList<>());
        }
        if (signature.startsWith(StandardTypes.ROW)) {
            return parseRowTypeSignature(signature);
        }

        String baseName = null;
        List<TypeSignatureParameter> parameters = new ArrayList<>();
        int parameterStart = -1;
        int bracketCount = 0;

        for (int i = 0; i < signature.length(); i++) {
            char c = signature.charAt(i);
            // TODO: remove angle brackets support once ROW<TYPE>(name) will be dropped
            // Angle brackets here are checked not for the support of ARRAY<> and MAP<>
            // but to correctly parse ARRAY(row<BIGINT, BIGINT>('a','b'))
            if (c == '(' || c == '<') {
                if (bracketCount == 0) {
                    verify(baseName == null, "Expected baseName to be null");
                    verify(parameterStart == -1, "Expected parameter start to be -1");
                    baseName = signature.substring(0, i);
                    parameterStart = i + 1;
                }
                bracketCount++;
            }
            else if (c == ')' || c == '>') {
                bracketCount--;
                checkArgument(bracketCount >= 0, "Bad type signature: '%s'", signature);
                if (bracketCount == 0) {
                    checkArgument(parameterStart >= 0, "Bad type signature: '%s'", signature);
                    parseTypeSignatureParameter(signature, parameterStart, i, parameters);
                    parameterStart = i + 1;
                    if (i == signature.length() - 1) {
                        return new TypeSignature(baseName, parameters);
                    }
                }
            }
            else if (c == ',') {
                if (bracketCount == 1) {
                    checkArgument(parameterStart >= 0, "Bad type signature: '%s'", signature);
                    parseTypeSignatureParameter(signature, parameterStart, i, parameters);
                    parameterStart = i + 1;
                }
            }
        }

        throw new IllegalArgumentException(format("Bad type signature: '%s'", signature));
    }

    @Deprecated
    private static TypeSignature parseRowTypeSignature(String signature)
    {
        String baseName = null;
        List<TypeSignature> parameters = new ArrayList<>();
        List<Object> literalParameters = new ArrayList<>();
        int parameterStart = -1;
        int bracketCount = 0;
        boolean inLiteralParameters = false;

        for (int i = 0; i < signature.length(); i++) {
            char c = signature.charAt(i);
            if (c == '<') {
                if (bracketCount == 0) {
                    verify(baseName == null, "Expected baseName to be null");
                    verify(parameterStart == -1, "Expected parameter start to be -1");
                    baseName = signature.substring(0, i);
                    parameterStart = i + 1;
                }
                bracketCount++;
            }
            else if (c == '>') {
                bracketCount--;
                checkArgument(bracketCount >= 0, "Bad type signature: '%s'", signature);
                if (bracketCount == 0) {
                    checkArgument(parameterStart >= 0, "Bad type signature: '%s'", signature);
                    parameters.add(parseTypeSignature(signature.substring(parameterStart, i)));
                    parameterStart = i + 1;
                    if (i == signature.length() - 1) {
                        return new TypeSignature(baseName, parameters, literalParameters);
                    }
                }
            }
            else if (c == ',') {
                if (bracketCount == 1) {
                    if (!inLiteralParameters) {
                        checkArgument(parameterStart >= 0, "Bad type signature: '%s'", signature);
                        parameters.add(parseTypeSignature(signature.substring(parameterStart, i)));
                        parameterStart = i + 1;
                    }
                    else {
                        checkArgument(parameterStart >= 0, "Bad type signature: '%s'", signature);
                        literalParameters.add(parseLiteral(signature.substring(parameterStart, i)));
                        parameterStart = i + 1;
                    }
                }
            }
            else if (c == '(') {
                if (bracketCount == 0) {
                    inLiteralParameters = true;
                    if (baseName == null) {
                        verify(parameters.isEmpty(), "Expected no parameters");
                        verify(parameterStart == -1, "Expected parameter start to be -1");
                        baseName = signature.substring(0, i);
                    }
                    parameterStart = i + 1;
                }
                bracketCount++;
            }
            else if (c == ')') {
                bracketCount--;
                if (bracketCount == 0) {
                    checkArgument(inLiteralParameters, "Bad type signature: '%s'", signature);
                    inLiteralParameters = false;
                    checkArgument(i == signature.length() - 1, "Bad type signature: '%s'", signature);
                    checkArgument(parameterStart >= 0, "Bad type signature: '%s'", signature);
                    literalParameters.add(parseLiteral(signature.substring(parameterStart, i)));
                    return new TypeSignature(baseName, parameters, literalParameters);
                }
            }
        }

        throw new IllegalArgumentException(format("Bad type signature: '%s'", signature));
    }

    private static void parseTypeSignatureParameter(
            String signature,
            int begin,
            int end,
            List<TypeSignatureParameter> parameters)
    {
        if (Character.isDigit(signature.charAt(begin))) {
            parameters.add(TypeSignatureParameter.of(Long.parseLong(signature.substring(begin, end))));
        }
        else {
            parameters.add(TypeSignatureParameter.of(parseTypeSignature(signature.substring(begin, end))));
        }
    }

    private static Object parseLiteral(String literal)
    {
        if (literal.startsWith("'") || literal.endsWith("'")) {
            checkArgument(literal.startsWith("'") && literal.endsWith("'"), "Bad literal: '%s'", literal);
            return literal.substring(1, literal.length() - 1);
        }
        else {
            return Long.parseLong(literal);
        }
    }

    @Deprecated
    private String rowToString()
    {
        return toString("<", ">");
    }

    private String toString(String leftParameterBracket, String rightParameterBracket)
    {
        StringBuilder typeName = new StringBuilder(base);
        if (!parameters.isEmpty()) {
            typeName.append(leftParameterBracket);
            boolean first = true;
            for (TypeSignatureParameter parameter : parameters) {
                if (!first) {
                    typeName.append(",");
                }
                first = false;
                typeName.append(parameter.toString());
            }
            typeName.append(rightParameterBracket);
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

    private static void checkArgument(boolean argument, String format, Object... args)
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

    private static boolean validateName(String name)
    {
        return name.chars().noneMatch(c -> c == '<' || c == '>' || c == ',');
    }
}
