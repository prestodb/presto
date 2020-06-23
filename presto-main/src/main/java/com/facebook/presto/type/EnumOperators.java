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
package com.facebook.presto.type;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.AbstractLongType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.EnumType;
import com.facebook.presto.common.type.IntegerEnumType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.StringEnumType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.PolymorphicScalarFunctionBuilder;
import com.facebook.presto.metadata.SignatureBuilder;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.USE_NULL_FLAG;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static java.util.Objects.requireNonNull;

public final class EnumOperators
{

    public static final TypeSignature BOOLEAN_TYPE_SIGNATURE = BooleanType.BOOLEAN.getTypeSignature();

    private EnumOperators() {}

    private static final Set<TypeSignature> INTEGER_TYPES = Sets.newHashSet(
            Stream.of(TinyintType.TINYINT, SmallintType.SMALLINT, IntegerType.INTEGER, BigintType.BIGINT)
                    .map(Type::getTypeSignature).collect(Collectors.toSet())
    );

    // TODO add cast functions from enum to base types too?

    public static Optional<SqlFunction> makeOperator(OperatorType operatorType, List<Type> argTypes, @Nullable Type returnType)
    {
        if (!(returnType instanceof EnumType) && argTypes.stream().noneMatch(t -> t instanceof EnumType)) {
            return Optional.empty();
        }

        boolean isInt = !argTypes.isEmpty() && argTypes.get(0) instanceof IntegerEnumType;
        List<TypeSignature> argTypeSignatures = argTypes.stream()
                .map(Type::getTypeSignature)
                .collect(Collectors.toList());

        switch (operatorType) {
            case CAST:
                requireNonNull(returnType);  // return type must be provided for CAST function lookup
                if (returnType instanceof EnumType) {
                    return makeCastToEnum(argTypes.get(0).getTypeSignature(), (EnumType) returnType);
                }
                if (argTypes.get(0) instanceof EnumType) {
                    return makeCastFromEnum((EnumType) argTypes.get(0), returnType.getTypeSignature());
                }
                return Optional.empty();
            case EQUAL:
            case NOT_EQUAL:
                if (!(argTypes.get(0) instanceof EnumType) || !argTypes.get(0).equals(argTypes.get(1))) {
                    return Optional.empty();
                }
                return Optional.of(buildFunction(
                        operatorType,
                        argTypeSignatures,
                        BOOLEAN_TYPE_SIGNATURE,
                        true,
                        isInt ? "integerEnumCompare" : "stringEnumCompare",
                        ImmutableList.of(OperatorType.NOT_EQUAL.equals(operatorType))));
            case IS_DISTINCT_FROM:
                if (!(argTypes.get(0) instanceof EnumType) || !argTypes.get(0).equals(argTypes.get(1))) {
                    return Optional.empty();
                }
                return Optional.of(buildFunction(
                        operatorType,
                        argTypeSignatures,
                        BOOLEAN_TYPE_SIGNATURE,
                        false,
                        isInt ? "integerEnumIsDistinct" : "stringEnumIsDistinct",
                        ImmutableList.of(),
                        new BuiltInScalarFunctionImplementation.ArgumentProperty[] {
                                valueTypeArgumentProperty(USE_NULL_FLAG),
                                valueTypeArgumentProperty(USE_NULL_FLAG)
                        }
                ));
            case HASH_CODE:
                return Optional.of(buildFunction(
                        operatorType,
                        argTypeSignatures,
                        BigintType.BIGINT.getTypeSignature(),
                        false,
                        isInt ? "integerEnumHash" : "stringEnumHash",
                        ImmutableList.of()
                ));
            case INDETERMINATE:
                return Optional.of(buildFunction(
                        operatorType,
                        argTypeSignatures,
                        BOOLEAN_TYPE_SIGNATURE,
                        false,
                        isInt ? "integerEnumIndeterminate" : "stringEnumIndeterminate",
                        ImmutableList.of(),
                        new BuiltInScalarFunctionImplementation.ArgumentProperty[] {
                                valueTypeArgumentProperty(USE_NULL_FLAG)
                        }));
            default:
                return Optional.empty();
        }
    }

    private static Optional<SqlFunction> makeCastToEnum(TypeSignature fromType, EnumType toType)
    {
        if (toType instanceof IntegerEnumType) {
            if (INTEGER_TYPES.contains(fromType)) {
                return Optional.of(buildCastToEnum(fromType, toType, "integerEnumValueLookup"));
            }
            if (StandardTypes.VARCHAR.equals(fromType.getBase())) {
                return Optional.of(buildCastToEnum(fromType, toType, "integerEnumKeyLookup"));
            }
        }
        if (toType instanceof StringEnumType) {
            if (StandardTypes.VARCHAR.equals(fromType.getBase())) {
                return Optional.of(buildCastToEnum(fromType, toType, "stringEnumKeyOrValueLookup"));
            }
        }
        return Optional.empty();
    }

    private static Optional<SqlFunction> makeCastFromEnum(EnumType fromType, TypeSignature toType)
    {
        if (fromType instanceof IntegerEnumType) {
            if (BigintType.BIGINT.getTypeSignature().equals(toType)) {
                return Optional.of(buildCastFromEnum(fromType, toType, "integerEnumAsPrimitive"));
            }
        }
        if (fromType instanceof StringEnumType) {
            if (StandardTypes.VARCHAR.equals(toType.getBase())) {
                return Optional.of(buildCastFromEnum(fromType, toType, "stringEnumAsPrimitive"));
            }
        }
        return Optional.empty();
    }

    private static SqlScalarFunction buildCastToEnum(TypeSignature fromType, EnumType toType, String methodName)
    {
        return buildFunction(
                CAST,
                ImmutableList.of(fromType),
                toType.getTypeSignature(),
                false,
                methodName,
                ImmutableList.of(toType));
    }

    private static SqlScalarFunction buildCastFromEnum(EnumType fromType, TypeSignature toType, String methodName)
    {
        return buildFunction(
                CAST,
                ImmutableList.of(fromType.getTypeSignature()),
                toType,
                false,
                methodName,
                ImmutableList.of());
    }

    private static SqlScalarFunction buildFunction(OperatorType operatorType, List<TypeSignature> argTypes, TypeSignature returnType,
            boolean nullableResult, String methodName, List<Object> extraParams)
    {
        return buildFunction(operatorType, argTypes, returnType, nullableResult, methodName, extraParams, null);
    }

    private static SqlScalarFunction buildFunction(OperatorType operatorType, List<TypeSignature> argTypes, TypeSignature returnType,
            boolean nullableResult, String methodName, List<Object> extraParams, BuiltInScalarFunctionImplementation.ArgumentProperty[] argumentProperties)
    {
        Signature signature = SignatureBuilder.builder()
                .kind(SCALAR)
                .operatorType(operatorType)
                .argumentTypes(argTypes)
                .returnType(returnType)
                .build();

        return SqlScalarFunction.builder(EnumOperators.class)
                .calledOnNullInput(true)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> {
                    PolymorphicScalarFunctionBuilder.ChoiceBuilder builder = choice
                            .nullableResult(nullableResult);
                    if (argumentProperties != null) {
                        builder.argumentProperties(argumentProperties);
                    }
                    return builder.implementation(
                            methodsGroup -> methodsGroup.methods(methodName)
                                    .withExtraParameters(ctx -> extraParams));
                })
                .build();
    }

    // -------------- Methods used by generated functions --------------------

    @UsedByGeneratedCode
    public static Slice stringEnumKeyOrValueLookup(Slice value, StringEnumType enumType)
    {
        final String enumValue = enumType.getEntries().get(value.toStringUtf8());
        if (enumValue == null) {
            if (!enumType.getEntries().values().contains(value.toStringUtf8())) {
                throw new PrestoException(
                        INVALID_CAST_ARGUMENT,
                        String.format(
                                "No key or value '%s' in enum '%s'",
                                value.toStringUtf8(),
                                enumType.getTypeSignature().getBase()));
            }
            return Slices.copyOf(value);
        }
        return Slices.utf8Slice(enumValue);
    }

    @UsedByGeneratedCode
    public static long integerEnumKeyLookup(Slice value, IntegerEnumType enumType)
    {
        final Long enumValue = enumType.getEntries().get(value.toStringUtf8());
        if (enumValue == null) {
            throw new PrestoException(
                    INVALID_CAST_ARGUMENT,
                    String.format(
                            "No key '%s' in enum '%s'",
                            value.toStringUtf8(),
                            enumType.getTypeSignature().getBase()));
        }
        return enumValue;
    }

    @UsedByGeneratedCode
    public static long integerEnumValueLookup(long value, IntegerEnumType enumType)
    {
        if (!enumType.getEntries().values().contains(value)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT,
                    String.format(
                            "No value '%d' in enum '%s'",
                            value,
                            enumType.getTypeSignature().getBase()));
        }
        return value;
    }

    @UsedByGeneratedCode
    public static Boolean integerEnumCompare(long v1, long v2, boolean notEquals)
    {
        return notEquals == (v1 != v2);
    }

    @UsedByGeneratedCode
    public static Boolean stringEnumCompare(Slice v1, Slice v2, boolean notEquals)
    {
        return notEquals != v1.equals(v2);
    }

    @UsedByGeneratedCode
    public static long stringEnumHash(Slice value)
    {
        return XxHash64.hash(value);
    }

    @UsedByGeneratedCode
    public static long integerEnumHash(long value)
    {
        return AbstractLongType.hash(value);
    }

    @UsedByGeneratedCode
    public static boolean integerEnumIndeterminate(long value, boolean isNull)
    {
        return isNull;
    }

    @UsedByGeneratedCode
    public static boolean stringEnumIndeterminate(Slice value, boolean isNull)
    {
        return isNull;
    }

    @UsedByGeneratedCode
    public static long integerEnumAsPrimitive(long value)
    {
        return value;
    }

    @UsedByGeneratedCode
    public static Slice stringEnumAsPrimitive(Slice value)
    {
        return value;
    }

    @UsedByGeneratedCode
    public static boolean integerEnumIsDistinct(long left, boolean leftNull, long right, boolean rightNull)
    {
        return IntegerOperators.IntegerDistinctFromOperator.isDistinctFrom(left, leftNull, right, rightNull);
    }

    @UsedByGeneratedCode
    public static boolean stringEnumIsDistinct(Slice left, boolean leftNull, Slice right, boolean rightNull)
    {
        return VarcharOperators.VarcharDistinctFromOperator.isDistinctFrom(left, leftNull, right, rightNull);
    }
}
