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

import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeParameter;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.spi.type.VarcharType;
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
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.P4HyperLogLogType.P4_HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.type.ArrayParametricType.ARRAY;
import static com.facebook.presto.type.CodePointsType.CODE_POINTS;
import static com.facebook.presto.type.ColorType.COLOR;
import static com.facebook.presto.type.FunctionParametricType.FUNCTION;
import static com.facebook.presto.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static com.facebook.presto.type.JoniRegexpType.JONI_REGEXP;
import static com.facebook.presto.type.JsonPathType.JSON_PATH;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.type.LikePatternType.LIKE_PATTERN;
import static com.facebook.presto.type.MapParametricType.MAP;
import static com.facebook.presto.type.Re2JRegexpType.RE2J_REGEXP;
import static com.facebook.presto.type.RowParametricType.ROW;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.facebook.presto.util.Types.checkType;
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
        addType(INTEGER);
        addType(SMALLINT);
        addType(TINYINT);
        addType(DOUBLE);
        addType(REAL);
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
        addType(JONI_REGEXP);
        addType(RE2J_REGEXP);
        addType(LIKE_PATTERN);
        addType(JSON_PATH);
        addType(COLOR);
        addType(JSON);
        addType(CODE_POINTS);
        addParametricType(VarcharParametricType.VARCHAR);
        addParametricType(CharParametricType.CHAR);
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

    @Override
    public boolean isTypeOnlyCoercion(Type source, Type result)
    {
        if (source.equals(result)) {
            return true;
        }

        if (!canCoerce(source, result)) {
            return false;
        }

        if (source instanceof VarcharType && result instanceof VarcharType) {
            return true;
        }

        if (source instanceof DecimalType && result instanceof DecimalType) {
            DecimalType sourceDecimal = (DecimalType) source;
            DecimalType resultDecimal = (DecimalType) result;
            boolean sameDecimalSubtype = (sourceDecimal.isShort() && resultDecimal.isShort())
                    || (!sourceDecimal.isShort() && !resultDecimal.isShort());
            boolean sameScale = sourceDecimal.getScale() == resultDecimal.getScale();
            boolean sourcePrecisionIsLessOrEqualToResultPrecision = sourceDecimal.getPrecision() <= resultDecimal.getPrecision();
            return sameDecimalSubtype && sameScale && sourcePrecisionIsLessOrEqualToResultPrecision;
        }

        String sourceTypeBase = source.getTypeSignature().getBase();
        String resultTypeBase = result.getTypeSignature().getBase();

        if (sourceTypeBase.equals(resultTypeBase) && isCovariantParametrizedType(source)) {
            List<Type> sourceTypeParameters = source.getTypeParameters();
            List<Type> resultTypeParameters = result.getTypeParameters();
            checkState(sourceTypeParameters.size() == resultTypeParameters.size());
            for (int i = 0; i < sourceTypeParameters.size(); i++) {
                if (!isTypeOnlyCoercion(sourceTypeParameters.get(i), resultTypeParameters.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
    {
        if (firstType.equals(secondType)) {
            return Optional.of(secondType);
        }

        if (firstType.equals(UnknownType.UNKNOWN)) {
            return Optional.of(secondType);
        }

        if (secondType.equals(UnknownType.UNKNOWN)) {
            return Optional.of(firstType);
        }

        String firstTypeBaseName = firstType.getTypeSignature().getBase();
        String secondTypeBaseName = secondType.getTypeSignature().getBase();
        if (firstTypeBaseName.equals(secondTypeBaseName)) {
            if (firstTypeBaseName.equals(StandardTypes.DECIMAL)) {
                return Optional.of(getCommonSuperTypeForDecimal(
                        checkType(firstType, DecimalType.class, "firstType"), checkType(secondType, DecimalType.class, "secondType")));
            }
            if (firstTypeBaseName.equals(StandardTypes.VARCHAR)) {
                return Optional.of(getCommonSuperTypeForVarchar(
                        checkType(firstType, VarcharType.class, "firstType"), checkType(secondType, VarcharType.class, "secondType")));
            }

            if (isCovariantParametrizedType(firstType)) {
                return getCommonSuperTypeForCovariantParametrizedType(firstType, secondType);
            }
            return Optional.empty();
        }

        Optional<Type> coercedType = coerceTypeBase(firstType, secondType.getTypeSignature().getBase());
        if (coercedType.isPresent()) {
            return getCommonSuperType(coercedType.get(), secondType);
        }

        coercedType = coerceTypeBase(secondType, firstType.getTypeSignature().getBase());
        if (coercedType.isPresent()) {
            return getCommonSuperType(firstType, coercedType.get());
        }

        return Optional.empty();
    }

    private static Type getCommonSuperTypeForDecimal(DecimalType firstType, DecimalType secondType)
    {
        int targetScale = Math.max(firstType.getScale(), secondType.getScale());
        int targetPrecision = Math.max(firstType.getPrecision() - firstType.getScale(), secondType.getPrecision() - secondType.getScale()) + targetScale;
        //we allow potential loss of precision here. Overflow checking is done in operators.
        targetPrecision = Math.min(38, targetPrecision);
        return createDecimalType(targetPrecision, targetScale);
    }

    private static Type getCommonSuperTypeForVarchar(VarcharType firstType, VarcharType secondType)
    {
        return createVarcharType(Math.max(firstType.getLength(), secondType.getLength()));
    }

    private Optional<Type> getCommonSuperTypeForCovariantParametrizedType(Type firstType, Type secondType)
    {
        checkState(firstType.getClass().equals(secondType.getClass()));
        ImmutableList.Builder<TypeSignatureParameter> commonParameterTypes = ImmutableList.builder();
        List<Type> firstTypeParameters = firstType.getTypeParameters();
        List<Type> secondTypeParameters = secondType.getTypeParameters();
        checkState(firstTypeParameters.size() == secondTypeParameters.size());
        for (int i = 0; i < firstTypeParameters.size(); i++) {
            Optional<Type> commonParameterType = getCommonSuperType(firstTypeParameters.get(i), secondTypeParameters.get(i));
            if (!commonParameterType.isPresent()) {
                return Optional.empty();
            }
            commonParameterTypes.add(TypeSignatureParameter.of(commonParameterType.get().getTypeSignature()));
        }
        String typeName = firstType.getTypeSignature().getBase();
        return Optional.of(getType(new TypeSignature(typeName, commonParameterTypes.build())));
    }

    public void addType(Type type)
    {
        requireNonNull(type, "type is null");
        Type existingType = types.putIfAbsent(type.getTypeSignature(), type);
        checkState(existingType == null || existingType.equals(type), "Type %s is already registered", type);
    }

    public void addParametricType(ParametricType parametricType)
    {
        String name = parametricType.getName().toLowerCase(Locale.ENGLISH);
        checkArgument(!parametricTypes.containsKey(name), "Parametric type already registered: %s", name);
        parametricTypes.putIfAbsent(name, parametricType);
    }

    /**
     * coerceTypeBase and isCovariantParametrizedType defines all hand-coded rules for type coercion.
     * Other methods should reference these two functions instead of hand-code new rules.
     */
    @Override
    public Optional<Type> coerceTypeBase(Type sourceType, String resultTypeBase)
    {
        String sourceTypeName = sourceType.getTypeSignature().getBase();
        if (sourceTypeName.equals(resultTypeBase)) {
            return Optional.of(sourceType);
        }

        switch (sourceTypeName) {
            case UnknownType.NAME: {
                switch (resultTypeBase) {
                    case StandardTypes.BOOLEAN:
                    case StandardTypes.BIGINT:
                    case StandardTypes.INTEGER:
                    case StandardTypes.DOUBLE:
                    case StandardTypes.REAL:
                    case StandardTypes.VARBINARY:
                    case StandardTypes.DATE:
                    case StandardTypes.TIME:
                    case StandardTypes.TIME_WITH_TIME_ZONE:
                    case StandardTypes.TIMESTAMP:
                    case StandardTypes.TIMESTAMP_WITH_TIME_ZONE:
                    case StandardTypes.HYPER_LOG_LOG:
                    case StandardTypes.P4_HYPER_LOG_LOG:
                    case StandardTypes.JSON:
                    case StandardTypes.INTERVAL_YEAR_TO_MONTH:
                    case StandardTypes.INTERVAL_DAY_TO_SECOND:
                    case JoniRegexpType.NAME:
                    case LikePatternType.NAME:
                    case JsonPathType.NAME:
                    case ColorType.NAME:
                    case CodePointsType.NAME:
                        return Optional.of(getType(new TypeSignature(resultTypeBase)));
                    case StandardTypes.VARCHAR:
                        return Optional.of(createVarcharType(0));
                    case StandardTypes.CHAR:
                        return Optional.of(createCharType(0));
                    case StandardTypes.DECIMAL:
                        return Optional.of(createDecimalType(1, 0));
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.TINYINT: {
                switch (resultTypeBase) {
                    case StandardTypes.SMALLINT:
                        return Optional.of(SMALLINT);
                    case StandardTypes.INTEGER:
                        return Optional.of(INTEGER);
                    case StandardTypes.BIGINT:
                        return Optional.of(BIGINT);
                    case StandardTypes.REAL:
                        return Optional.of(REAL);
                    case StandardTypes.DOUBLE:
                        return Optional.of(DOUBLE);
                    case StandardTypes.DECIMAL:
                        return Optional.of(createDecimalType(3, 0));
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.SMALLINT: {
                switch (resultTypeBase) {
                    case StandardTypes.INTEGER:
                        return Optional.of(INTEGER);
                    case StandardTypes.BIGINT:
                        return Optional.of(BIGINT);
                    case StandardTypes.REAL:
                        return Optional.of(REAL);
                    case StandardTypes.DOUBLE:
                        return Optional.of(DOUBLE);
                    case StandardTypes.DECIMAL:
                        return Optional.of(createDecimalType(5, 0));
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.INTEGER: {
                switch (resultTypeBase) {
                    case StandardTypes.BIGINT:
                        return Optional.of(BIGINT);
                    case StandardTypes.REAL:
                        return Optional.of(REAL);
                    case StandardTypes.DOUBLE:
                        return Optional.of(DOUBLE);
                    case StandardTypes.DECIMAL:
                        return Optional.of(createDecimalType(10, 0));
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.BIGINT: {
                switch (resultTypeBase) {
                    case StandardTypes.REAL:
                        return Optional.of(REAL);
                    case StandardTypes.DOUBLE:
                        return Optional.of(DOUBLE);
                    case StandardTypes.DECIMAL:
                        return Optional.of(createDecimalType(19, 0));
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.DECIMAL: {
                switch (resultTypeBase) {
                    case StandardTypes.REAL:
                        return Optional.of(REAL);
                    case StandardTypes.DOUBLE:
                        return Optional.of(DOUBLE);
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.REAL: {
                switch (resultTypeBase) {
                    case StandardTypes.DOUBLE:
                        return Optional.of(DOUBLE);
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.DATE: {
                switch (resultTypeBase) {
                    case StandardTypes.TIMESTAMP:
                        return Optional.of(TIMESTAMP);
                    case StandardTypes.TIMESTAMP_WITH_TIME_ZONE:
                        return Optional.of(TIMESTAMP_WITH_TIME_ZONE);
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.TIME: {
                switch (resultTypeBase) {
                    case StandardTypes.TIME_WITH_TIME_ZONE:
                        return Optional.of(TIME_WITH_TIME_ZONE);
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.TIMESTAMP: {
                switch (resultTypeBase) {
                    case StandardTypes.TIMESTAMP_WITH_TIME_ZONE:
                        return Optional.of(TIMESTAMP_WITH_TIME_ZONE);
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.VARCHAR: {
                switch (resultTypeBase) {
                    case JoniRegexpType.NAME:
                        return Optional.of(JONI_REGEXP);
                    case Re2JRegexpType.NAME:
                        return Optional.of(RE2J_REGEXP);
                    case LikePatternType.NAME:
                        return Optional.of(LIKE_PATTERN);
                    case JsonPathType.NAME:
                        return Optional.of(JSON_PATH);
                    case CodePointsType.NAME:
                        return Optional.of(CODE_POINTS);
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.CHAR: {
                switch (resultTypeBase) {
                    case StandardTypes.VARCHAR:
                        CharType charType = (CharType) sourceType;
                        return Optional.of(createVarcharType(charType.getLength()));
                    case JoniRegexpType.NAME:
                        return Optional.of(JONI_REGEXP);
                    case Re2JRegexpType.NAME:
                        return Optional.of(RE2J_REGEXP);
                    case LikePatternType.NAME:
                        return Optional.of(LIKE_PATTERN);
                    case JsonPathType.NAME:
                        return Optional.of(JSON_PATH);
                    case CodePointsType.NAME:
                        return Optional.of(CODE_POINTS);
                    default:
                        return Optional.empty();
                }
            }
            case StandardTypes.P4_HYPER_LOG_LOG: {
                switch (resultTypeBase) {
                    case StandardTypes.HYPER_LOG_LOG:
                        return Optional.of(HYPER_LOG_LOG);
                    default:
                        return Optional.empty();
                }
            }
            default:
                return Optional.empty();
        }
    }

    private static boolean isCovariantParametrizedType(Type type)
    {
        // if we ever introduce contravariant, this function should be changed to return an enumeration: INVARIANT, COVARIANT, CONTRAVARIANT
        return type instanceof MapType || type instanceof ArrayType;
    }

    public static boolean isCovariantTypeBase(String typeBase)
    {
        return typeBase.equals(StandardTypes.ARRAY) || typeBase.equals(StandardTypes.MAP);
    }
}
