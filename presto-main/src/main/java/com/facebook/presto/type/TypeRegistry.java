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

import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

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
import static com.facebook.presto.spi.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.spi.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static com.facebook.presto.spi.type.P4HyperLogLogType.P4_HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
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
import static java.util.Objects.requireNonNull;

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
        addType(DOUBLE);
        addType(VARCHAR);
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
    public Type getParameterizedType(String baseTypeName, List<TypeSignature> typeParameters, List<Object> literalParameters)
    {
        return getType(new TypeSignature(baseTypeName, typeParameters, literalParameters));
    }

    private Type instantiateParametricType(TypeSignature signature)
    {
        ImmutableList.Builder<Type> parameterTypes = ImmutableList.builder();
        for (TypeSignature parameter : signature.getParameters()) {
            Type parameterType = getType(parameter);
            if (parameterType == null) {
                return null;
            }
            parameterTypes.add(parameterType);
        }

        ParametricType parametricType = parametricTypes.get(signature.getBase().toLowerCase(Locale.ENGLISH));
        if (parametricType == null) {
            return null;
        }
        Type instantiatedType = parametricType.createType(parameterTypes.build(), signature.getLiteralParameters());
        checkState(instantiatedType.getTypeSignature().equals(signature), "Instantiated parametric type name (%s) does not match expected name (%s)", instantiatedType, signature);
        return instantiatedType;
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
        return firstTypeBase.equals(StandardTypes.ARRAY);
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
        // Without this, cast from null to map<bigint, bigint> will not be allowed.
        if (UnknownType.NAME.equals(firstType.getBase())) {
            return Optional.of(secondType);
        }
        if (UnknownType.NAME.equals(secondType.getBase())) {
            return Optional.of(firstType);
        }

        List<TypeSignature> firstTypeTypeParameters = firstType.getParameters();
        List<TypeSignature> secondTypeTypeParameters = secondType.getParameters();
        if (firstTypeTypeParameters.size() != secondTypeTypeParameters.size()) {
            return Optional.empty();
        }
        if (!firstType.getLiteralParameters().equals(secondType.getLiteralParameters())) {
            return Optional.empty();
        }

        Optional<String> commonSuperTypeBase = getCommonSuperTypeBase(firstType.getBase(), secondType.getBase());
        if (!commonSuperTypeBase.isPresent()) {
            return Optional.empty();
        }

        ImmutableList.Builder<TypeSignature> typeParameters = ImmutableList.builder();
        for (int i = 0; i < firstTypeTypeParameters.size(); i++) {
            if (isCovariantParameterPosition(commonSuperTypeBase.get(), i)) {
                Optional<TypeSignature> commonSuperType = getCommonSuperTypeSignature(firstTypeTypeParameters.get(i), secondTypeTypeParameters.get(i));
                if (!commonSuperType.isPresent()) {
                    return Optional.empty();
                }
                typeParameters.add(commonSuperType.get());
            }
            else {
                if (!firstTypeTypeParameters.get(i).equals(secondTypeTypeParameters.get(i))) {
                    return Optional.empty();
                }
                typeParameters.add(firstTypeTypeParameters.get(i));
            }
        }

        return Optional.of(new TypeSignature(commonSuperTypeBase.get(), typeParameters.build(), firstType.getLiteralParameters()));
    }
}
