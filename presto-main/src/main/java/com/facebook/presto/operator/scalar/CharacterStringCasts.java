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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.metadata.SqlScalarFunctionBuilder.SpecializeContext;
import com.facebook.presto.spi.type.Chars;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.OperatorType.SATURATED_FLOOR_CAST;
import static com.facebook.presto.spi.type.Chars.trimSpacesAndTruncateToLength;
import static com.facebook.presto.spi.type.StandardTypes.CHAR;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static java.lang.Math.min;

public final class CharacterStringCasts
{
    public static final SqlScalarFunction VARCHAR_TO_VARCHAR_CAST = truncateCast(VARCHAR, VARCHAR);
    public static final SqlScalarFunction CHAR_TO_CHAR_CAST = truncateCast(CHAR, CHAR);
    public static final SqlScalarFunction VARCHAR_TO_CHAR_CAST = trimCast(VARCHAR, CHAR);
    public static final SqlScalarFunction CHAR_TO_VARCHAR_CAST = padSpacesCast(CHAR, VARCHAR);
    public static final SqlScalarFunction VARCHAR_TO_CHAR_SATURATED_FLOOR_CAST = varcharToCharSaturatedFloorCast();

    private CharacterStringCasts() {}

    private static SqlScalarFunction truncateCast(String sourceType, String resultType)
    {
        Signature signature = getCastSignature(CAST, sourceType, resultType);
        return SqlScalarFunction.builder(CharacterStringCasts.class)
                .signature(signature)
                .implementation(b -> b
                        .methods("truncate")
                        .withPredicate(context -> getArgumentTypeLength(context) > getReturnTypeLength(context))
                        .withExtraParameters(context -> ImmutableList.of(getReturnTypeLength(context))))
                .implementation(b -> b
                        .methods("identity"))
                .build();
    }

    private static SqlScalarFunction padSpacesCast(String sourceType, String resultType)
    {
        Signature signature = getCastSignature(CAST, sourceType, resultType);
        return SqlScalarFunction.builder(CharacterStringCasts.class)
                .signature(signature)
                .implementation(b -> b
                        .methods("padSpaces")
                        .withExtraParameters(context -> ImmutableList.of(min(getArgumentTypeLength(context), getReturnTypeLength(context)))))
                .build();
    }

    private static SqlScalarFunction trimCast(String sourceType, String resultType)
    {
        Signature signature = getCastSignature(CAST, sourceType, resultType);
        return SqlScalarFunction.builder(CharacterStringCasts.class)
                .signature(signature)
                .implementation(b -> b
                        .methods("trimSpaces")
                        .withExtraParameters(context -> ImmutableList.of(getReturnTypeLength(context))))
                .build();
    }

    private static SqlScalarFunction varcharToCharSaturatedFloorCast()
    {
        Signature signature = getCastSignature(SATURATED_FLOOR_CAST, VARCHAR, CHAR);
        return SqlScalarFunction.builder(CharacterStringCasts.class)
                .signature(signature)
                .implementation(b -> b
                        .methods("varcharToCharSaturatedFloorCast")
                        .withExtraParameters(context -> ImmutableList.of(getReturnTypeLength(context))))
                .build();
    }

    private static Signature getCastSignature(OperatorType operatorType, String sourceType, String resultType)
    {
        return Signature.builder()
                .kind(SCALAR)
                .operatorType(operatorType)
                .argumentTypes(new TypeSignature(sourceType, TypeSignatureParameter.of("s")))
                .returnType(new TypeSignature(resultType, TypeSignatureParameter.of("r")))
                .build();
    }

    private static int getArgumentTypeLength(SpecializeContext context)
    {
        return context.getLiteral("s").intValue();
    }

    private static int getReturnTypeLength(SpecializeContext context)
    {
        return context.getLiteral("r").intValue();
    }

    @UsedByGeneratedCode
    public static Slice identity(Slice slice)
    {
        return slice;
    }

    @UsedByGeneratedCode
    public static Slice truncate(Slice slice, int resultLength)
    {
        return truncateToLength(slice, resultLength);
    }

    @UsedByGeneratedCode
    public static Slice padSpaces(Slice slice, int resultLength)
    {
        int textLength = countCodePoints(slice);

        // if our target length is the same as our string then return our string
        if (textLength == resultLength) {
            return slice;
        }

        // if our string is bigger than requested then truncate
        if (textLength > resultLength) {
            return SliceUtf8.substring(slice, 0, resultLength);
        }

        // preallocate the result
        int bufferSize = slice.length() + resultLength - textLength;
        Slice buffer = Slices.allocate(bufferSize);

        // fill in the existing string
        buffer.setBytes(0, slice);

        // fill padding spaces
        for (int i = slice.length(); i < bufferSize; ++i) {
            buffer.setByte(i, ' ');
        }

        return buffer;
    }

    @UsedByGeneratedCode
    public static Slice trimSpaces(Slice slice, int resultLength)
    {
        return trimSpacesAndTruncateToLength(slice, resultLength);
    }

    @UsedByGeneratedCode
    // TODO: This does return smaller value, but not the nearest. It is fine though for domain translation. Implement proper saturated floor cast.
    public static Slice varcharToCharSaturatedFloorCast(Slice slice, int resultLength)
    {
        Slice trimmedSlice = Chars.trimSpaces(slice);
        int trimmedTextLength = countCodePoints(trimmedSlice);
        int numberOfTrailingSpaces = slice.length() - trimmedSlice.length();

        if (trimmedTextLength + numberOfTrailingSpaces >= resultLength) {
            return truncateToLength(trimmedSlice, resultLength);
        }
        if (trimmedTextLength == 0) {
            return EMPTY_SLICE;
        }

        return trimmedSlice.slice(0, offsetOfCodePoint(trimmedSlice, trimmedTextLength - 1));
    }
}
