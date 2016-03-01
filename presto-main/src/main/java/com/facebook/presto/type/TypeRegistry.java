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

import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeParameter;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.spi.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static com.facebook.presto.spi.type.P4HyperLogLogType.P4_HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.StandardTypes.DECIMAL;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.type.ArrayParametricType.ARRAY;
import static com.facebook.presto.type.ColorType.COLOR;
import static com.facebook.presto.type.FunctionParametricType.FUNCTION;
import static com.facebook.presto.type.JsonPathType.JSON_PATH;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.type.LikePatternType.LIKE_PATTERN;
import static com.facebook.presto.type.MapParametricType.MAP;
import static com.facebook.presto.type.RegexpType.REGEXP;
import static com.facebook.presto.type.RowParametricType.ROW;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@ThreadSafe
public final class TypeRegistry
        implements TypeManager
{
    private final ConcurrentMap<TypeSignature, Type> types = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ParametricType> parametricTypes = new ConcurrentHashMap<>();

    public TypeRegistry()
    {
        this(ImmutableSet.<Type>of());
    }

    @Inject
    public TypeRegistry(Set<Type> types)
    {
        requireNonNull(types, "types is null");

        // Manually register UNKNOWN type without a verifyTypeClass call since it is a special type that can not be used by functions
        this.types.put(UNKNOWN.getTypeSignature(), UNKNOWN);

        // always add the built-in types; Presto will not function without these
        addType(BOOLEAN);
        addType(BIGINT);
        addType(INTEGER);
        addType(DOUBLE);
        addType(VARBINARY);
        addType(DATE);
        addType(TIME);
        addType(TIME_WITH_TIME_ZONE);
        addType(TIMESTAMP);
        addType(TIMESTAMP_WITH_TIME_ZONE);
        addType(INTERVAL_YEAR_MONTH);
        addType(INTERVAL_DAY_TIME);
        addType(HYPER_LOG_LOG);
        addType(P4_HYPER_LOG_LOG);
        addType(REGEXP);
        addType(LIKE_PATTERN);
        addType(JSON_PATH);
        addType(COLOR);
        addType(JSON);
        addParametricType(VarcharParametricType.VARCHAR);
        addParametricType(DecimalParametricType.DECIMAL);
        addParametricType(ROW);
        addParametricType(ARRAY);
        addParametricType(MAP);
        addParametricType(FUNCTION);

        for (Type type : types) {
            addType(type);
        }
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        Type type = types.get(signature);
        if (type == null) {
            return instantiateParametricType(signature);
        }
        return type;
    }

    @Override
    public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
    {
        return getType(new TypeSignature(baseTypeName, typeParameters));
    }

    @Override
    @Deprecated
    public Type getParameterizedType(String baseTypeName, List<TypeSignature> typeParameters, List<String> literalParameters)
    {
        if (baseTypeName.equals(StandardTypes.ROW)) {
            return getType(new TypeSignature(baseTypeName, typeParameters, literalParameters));
        }
        return getParameterizedType(
                baseTypeName,
                typeParameters.stream().map(TypeSignatureParameter::of).collect(toList()));
    }

    private Type instantiateParametricType(TypeSignature signature)
    {
        List<TypeParameter> parameters = new ArrayList<>();

        for (TypeSignatureParameter parameter : signature.getParameters()) {
            TypeParameter typeParameter = TypeParameter.of(parameter, this);
            if (typeParameter == null) {
                return null;
            }
            parameters.add(typeParameter);
        }

        ParametricType parametricType = parametricTypes.get(signature.getBase().toLowerCase(Locale.ENGLISH));
        if (parametricType == null) {
            return null;
        }

        try {
            Type instantiatedType = parametricType.createType(parameters);

            // TODO: reimplement this check? Currently "varchar(Integer.MAX_VALUE)" fails with "varchar"
            //checkState(instantiatedType.equalsSignature(signature), "Instantiated parametric type name (%s) does not match expected name (%s)", instantiatedType, signature);
            return instantiatedType;
        }
        catch (IllegalArgumentException e) {
            // TODO: check whether a type constructor actually exists rather than failing when it doesn't. This will be possible in the next version of the type system
            return null;
        }
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.copyOf(types.values());
    }

    public void addType(Type type)
    {
        verifyTypeClass(type);
        Type existingType = types.putIfAbsent(type.getTypeSignature(), type);
        checkState(existingType == null || existingType.equals(type), "Type %s is already registered", type);
    }

    public void addParametricType(ParametricType parametricType)
    {
        String name = parametricType.getName().toLowerCase(Locale.ENGLISH);
        checkArgument(!parametricTypes.containsKey(name), "Parametric type already registered: %s", name);
        parametricTypes.putIfAbsent(name, parametricType);
    }

    public static void verifyTypeClass(Type type)
    {
        requireNonNull(type, "type is null");
    }

    public static boolean canCoerce(List<? extends Type> actualTypes, List<Type> expectedTypes)
    {
        if (actualTypes.size() != expectedTypes.size()) {
            return false;
        }
        for (int i = 0; i < expectedTypes.size(); i++) {
            Type expectedType = expectedTypes.get(i);
            Type actualType = actualTypes.get(i);
            if (!canCoerce(actualType, expectedType)) {
                return false;
            }
        }
        return true;
    }

    private static boolean canCastTypeBase(String fromTypeBase, String toTypeBase)
    {
        // canCastTypeBase and isCovariantParameterPosition defines all hand-coded rules for type coercion.
        // Other methods should reference these two functions instead of hand-code new rules.

        if (UnknownType.NAME.equals(fromTypeBase)) {
            return true;
        }
        if (toTypeBase.equals(fromTypeBase)) {
            return true;
        }
        switch (fromTypeBase) {
            case StandardTypes.INTEGER:
                return StandardTypes.BIGINT.equals(toTypeBase) || StandardTypes.DOUBLE.equals(toTypeBase);
            case StandardTypes.BIGINT:
                return StandardTypes.DOUBLE.equals(toTypeBase);
            case StandardTypes.DATE:
                return StandardTypes.TIMESTAMP.equals(toTypeBase) || StandardTypes.TIMESTAMP_WITH_TIME_ZONE.equals(toTypeBase);
            case StandardTypes.TIME:
                return StandardTypes.TIME_WITH_TIME_ZONE.equals(toTypeBase);
            case StandardTypes.TIMESTAMP:
                return StandardTypes.TIMESTAMP_WITH_TIME_ZONE.equals(toTypeBase);
            case StandardTypes.VARCHAR:
                return RegexpType.NAME.equals(toTypeBase) || LikePatternType.NAME.equals(toTypeBase) || JsonPathType.NAME.equals(toTypeBase);
            case StandardTypes.P4_HYPER_LOG_LOG:
                return StandardTypes.HYPER_LOG_LOG.equals(toTypeBase);
        }
        return false;
    }

    private static boolean isCovariantParameterPosition(String firstTypeBase, int position)
    {
        // canCastTypeBase and isCovariantParameterPosition defines all hand-coded rules for type coercion.
        // Other methods should reference these two functions instead of hand-code new rules.

        // if we ever introduce contravariant, this function should be changed to return an enumeration: INVARIANT, COVARIANT, CONTRAVARIANT
        return firstTypeBase.equals(StandardTypes.ARRAY) || firstTypeBase.equals(StandardTypes.MAP);
    }

    public static boolean isTypeOnlyCoercion(Type actualType, Type expectedType)
    {
        return isTypeOnlyCoercion(actualType.getTypeSignature(), expectedType.getTypeSignature());
    }

    /*
     * Return true if actualType can be coerced to expectedType AND they are both binary compatible (so it's only type coercion)
     */
    public static boolean isTypeOnlyCoercion(TypeSignature actualType, TypeSignature expectedType)
    {
        if (!canCoerce(actualType, expectedType)) {
            return false;
        }

        if (actualType.equals(expectedType)) {
            return true;
        }
        else if (actualType.getBase().equals(StandardTypes.VARCHAR) && expectedType.getBase().equals(StandardTypes.VARCHAR)) {
            return true;
        }

        if (actualType.getBase().equals(DECIMAL) && expectedType.getBase().equals(DECIMAL)) {
            long actualPrecision = actualType.getParameters().get(0).getLongLiteral();
            long expectedPrecision = (long) expectedType.getParameters().get(0).getLongLiteral();
            long actualScale = (long) actualType.getParameters().get(1).getLongLiteral();
            long expectedScale = (long) expectedType.getParameters().get(1).getLongLiteral();

            if (actualPrecision <= Decimals.MAX_SHORT_PRECISION ^ expectedPrecision <= Decimals.MAX_SHORT_PRECISION) {
                return false;
            }

            return actualScale == expectedScale && actualPrecision <= expectedPrecision;
        }

        if (actualType.getBase().equals(expectedType.getBase()) &&
                actualType.getParameters().size() == expectedType.getParameters().size()) {
            for (int i = 0; i < actualType.getParameters().size(); i++) {
                if (!isCovariantParameterPosition(actualType.getBase(), i)) {
                    return false;
                }

                TypeSignatureParameter actualParameter = actualType.getParameters().get(i);
                TypeSignatureParameter expectedParameter = expectedType.getParameters().get(i);
                if (actualParameter.equals(expectedParameter)) {
                    continue;
                }

                Optional<TypeSignature> actualParameterSignature = actualParameter.getTypeSignatureOrNamedTypeSignature();
                Optional<TypeSignature> expectedParameterSignature = expectedParameter.getTypeSignatureOrNamedTypeSignature();
                if (!actualParameterSignature.isPresent() || !expectedParameterSignature.isPresent()) {
                    return false;
                }

                if (!isTypeOnlyCoercion(actualParameterSignature.get(), expectedParameterSignature.get())) {
                    return false;
                }
            }
            return true;
        }

        return false;
    }

    public static boolean canCoerce(Type actualType, Type expectedType)
    {
        return canCoerce(actualType.getTypeSignature(), expectedType.getTypeSignature());
    }

    public static boolean canCoerce(TypeSignature actualType, TypeSignature expectedType)
    {
        Optional<TypeSignature> commonSuperTypeSignature = getCommonSuperTypeSignature(actualType, expectedType);
        return commonSuperTypeSignature.isPresent() && commonSuperTypeSignature.get().equals(expectedType);
    }

    @Override
    public Optional<Type> getCommonSuperType(List<? extends Type> types)
    {
        checkArgument(!types.isEmpty(), "types is empty");
        Optional<TypeSignature> commonSuperTypeSignature = getCommonSuperTypeSignature(
                types.stream()
                        .map(Type::getTypeSignature)
                        .collect(toImmutableList()));
        return commonSuperTypeSignature.map(this::getType);
    }

    @Override
    public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
    {
        return getCommonSuperTypeSignature(firstType.getTypeSignature(), secondType.getTypeSignature()).map(this::getType);
    }

    private static Optional<String> getCommonSuperTypeBase(String firstTypeBase, String secondTypeBase)
    {
        if (canCastTypeBase(firstTypeBase, secondTypeBase)) {
            return Optional.of(secondTypeBase);
        }
        if (canCastTypeBase(secondTypeBase, firstTypeBase)) {
            return Optional.of(firstTypeBase);
        }

        return Optional.empty();
    }

    public static Optional<TypeSignature> getCommonSuperTypeSignature(List<? extends TypeSignature> typeSignatures)
    {
        checkArgument(!typeSignatures.isEmpty(), "typeSignatures is empty");
        TypeSignature superTypeSignature = UNKNOWN.getTypeSignature();
        for (TypeSignature typeSignature : typeSignatures) {
            Optional<TypeSignature> commonSuperTypeSignature = getCommonSuperTypeSignature(superTypeSignature, typeSignature);
            if (!commonSuperTypeSignature.isPresent()) {
                return Optional.empty();
            }
            superTypeSignature = commonSuperTypeSignature.get();
        }
        return Optional.of(superTypeSignature);
    }

    public static Optional<TypeSignature> getCommonSuperTypeSignature(TypeSignature firstType, TypeSignature secondType)
    {
        // Special handling for UnknownType is necessary because we forbid cast between types with different number of type parameters.
        // Without this, cast from null to map(bigint, bigint) will not be allowed.
        if (UnknownType.NAME.equals(firstType.getBase())) {
            return Optional.of(secondType);
        }
        if (UnknownType.NAME.equals(secondType.getBase())) {
            return Optional.of(firstType);
        }
        for (String varcharSubType : new String[] {RegexpType.NAME, LikePatternType.NAME, JsonPathType.NAME}) {
            if (firstType.getBase().equals(StandardTypes.VARCHAR) && secondType.getBase().equals(varcharSubType)) {
                return Optional.of(secondType);
            }
            else if (secondType.getBase().equals(StandardTypes.VARCHAR) && firstType.getBase().equals(varcharSubType)) {
                return Optional.of(firstType);
            }
        }

        Optional<String> commonSuperTypeBase = getCommonSuperTypeBase(firstType.getBase(), secondType.getBase());
        if (!commonSuperTypeBase.isPresent()) {
            return Optional.empty();
        }

        List<TypeSignatureParameter> firstTypeTypeParameters = firstType.getParameters();
        List<TypeSignatureParameter> secondTypeTypeParameters = secondType.getParameters();
        checkArgument(
                firstTypeTypeParameters.size() == secondTypeTypeParameters.size(),
                "Can not compare types [%s, %s] with different number of parameters. All default parameters should be expanded",
                firstType,
                secondType);

        // TODO: special case for decimal for now, this needs be moved somehow to DecimalType class
        if (firstType.getBase().equals(DECIMAL) && secondType.getBase().equals(DECIMAL)) {
            long firstPrecision = firstType.getParameters().get(0).getLongLiteral();
            long secondPrecision = secondType.getParameters().get(0).getLongLiteral();
            long firstScale = firstType.getParameters().get(1).getLongLiteral();
            long secondScale = secondType.getParameters().get(1).getLongLiteral();
            long targetScale = Math.max(firstScale, secondScale);
            long targetPrecision = Math.max(firstPrecision - firstScale, secondPrecision - secondScale) + targetScale;
            targetPrecision = Math.min(38, targetPrecision); //we allow potential loss of precision here. Overflow checking is done in operators.
            return Optional.of(new TypeSignature(
                    DECIMAL,
                    ImmutableList.of(TypeSignatureParameter.of(targetPrecision), TypeSignatureParameter.of(targetScale))));
        }

        if (firstTypeTypeParameters.size() != secondTypeTypeParameters.size()) {
            return Optional.empty();
        }

        ImmutableList.Builder<TypeSignatureParameter> typeParameters = ImmutableList.builder();
        for (int i = 0; i < firstTypeTypeParameters.size(); i++) {
            TypeSignatureParameter firstParameter = firstTypeTypeParameters.get(i);
            TypeSignatureParameter secondParameter = secondTypeTypeParameters.get(i);

            if (firstParameter.isLongLiteral() && secondParameter.isLongLiteral()) {
                // TODO: this Math.max should be moved to VarcharType?
                typeParameters.add(TypeSignatureParameter.of(Math.max(
                        firstParameter.getLongLiteral(),
                        secondParameter.getLongLiteral())));
            }
            else if (isCovariantParameterPosition(commonSuperTypeBase.get(), i)) {
                Optional<TypeSignature> firstParameterSignature = firstParameter.getTypeSignatureOrNamedTypeSignature();
                Optional<TypeSignature> secondParameterSignature = secondParameter.getTypeSignatureOrNamedTypeSignature();
                if (!firstParameterSignature.isPresent() || !secondParameterSignature.isPresent()) {
                    return Optional.empty();
                }

                Optional<TypeSignature> commonSuperType = getCommonSuperTypeSignature(
                        firstParameterSignature.get(), secondParameterSignature.get());
                if (!commonSuperType.isPresent()) {
                    return Optional.empty();
                }
                typeParameters.add(TypeSignatureParameter.of(commonSuperType.get()));
            }
            else {
                if (!firstParameter.equals(secondParameter)) {
                    return Optional.empty();
                }
                typeParameters.add(firstParameter);
            }
        }

        return Optional.of(new TypeSignature(commonSuperTypeBase.get(), typeParameters.build()));
    }

    /*
     * Given signature with unmatched parameters return filtered one.
     */
    public static TypeSignature getUnmatchedSignature(TypeSignature signature)
    {
        if (signature.getParameters().stream().allMatch(
                parameter -> parameter.isLongLiteral() || parameter.isTypeSignature() || parameter.isNamedTypeSignature()
        )) {
            return signature;
        }
        return new TypeSignature(signature.getBase(), emptyList());
    }
}
