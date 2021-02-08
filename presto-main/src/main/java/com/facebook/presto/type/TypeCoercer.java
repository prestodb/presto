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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.UnknownType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.khyperloglog.KHyperLogLogType;
import com.facebook.presto.type.setdigest.SetDigestType;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.type.CodePointsType.CODE_POINTS;
import static com.facebook.presto.type.JoniRegexpType.JONI_REGEXP;
import static com.facebook.presto.type.JsonPathType.JSON_PATH;
import static com.facebook.presto.type.LikePatternType.LIKE_PATTERN;
import static com.facebook.presto.type.Re2JRegexpType.RE2J_REGEXP;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TypeCoercer
{
    private final FeaturesConfig featuresConfig;
    private final FunctionAndTypeManager functionAndTypeManager;

    public TypeCoercer(FeaturesConfig featuresConfig, FunctionAndTypeManager functionAndTypeManager)
    {
        this.featuresConfig = requireNonNull(featuresConfig, "featuresConfig is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
    {
        TypeCompatibility compatibility = compatibility(firstType, secondType);
        if (!compatibility.isCompatible()) {
            return Optional.empty();
        }
        return Optional.of(compatibility.getCommonSuperType());
    }

    public boolean canCoerce(Type fromType, Type toType)
    {
        TypeCompatibility typeCompatibility = compatibility(fromType, toType);
        return typeCompatibility.isCoercible();
    }

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

    /**
     * coerceTypeBase and isCovariantParametrizedType defines all hand-coded rules for type coercion.
     * Other methods should reference these two functions instead of hand-code new rules.
     */
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
                    case SetDigestType.NAME:
                    case StandardTypes.P4_HYPER_LOG_LOG:
                    case StandardTypes.JSON:
                    case StandardTypes.INTERVAL_YEAR_TO_MONTH:
                    case StandardTypes.INTERVAL_DAY_TO_SECOND:
                    case KHyperLogLogType.NAME:
                    case JoniRegexpType.NAME:
                    case LikePatternType.NAME:
                    case JsonPathType.NAME:
                    case ColorType.NAME:
                    case CodePointsType.NAME:
                        return Optional.of(functionAndTypeManager.getType(new TypeSignature(resultTypeBase)));
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
                    case StandardTypes.CHAR:
                        if (featuresConfig.isLegacyCharToVarcharCoercion()) {
                            return Optional.empty();
                        }

                        VarcharType varcharType = (VarcharType) sourceType;
                        if (varcharType.isUnbounded()) {
                            return Optional.of(CharType.createCharType(CharType.MAX_LENGTH));
                        }

                        return Optional.of(createCharType(Math.min(CharType.MAX_LENGTH, varcharType.getLengthSafe())));
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
                        if (!featuresConfig.isLegacyCharToVarcharCoercion()) {
                            return Optional.empty();
                        }

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

    private TypeCompatibility compatibility(Type fromType, Type toType)
    {
        if (fromType.equals(toType)) {
            return TypeCompatibility.compatible(toType, true);
        }

        if (fromType.equals(UnknownType.UNKNOWN)) {
            return TypeCompatibility.compatible(toType, true);
        }

        if (toType.equals(UnknownType.UNKNOWN)) {
            return TypeCompatibility.compatible(fromType, false);
        }

        String fromTypeBaseName = fromType.getTypeSignature().getBase();
        String toTypeBaseName = toType.getTypeSignature().getBase();

        if (featuresConfig.isLegacyDateTimestampToVarcharCoercion()) {
            if ((fromTypeBaseName.equals(StandardTypes.DATE) || fromTypeBaseName.equals(StandardTypes.TIMESTAMP)) && toTypeBaseName.equals(StandardTypes.VARCHAR)) {
                return TypeCompatibility.compatible(toType, true);
            }

            if (fromTypeBaseName.equals(StandardTypes.VARCHAR) && (toTypeBaseName.equals(StandardTypes.DATE) || toTypeBaseName.equals(StandardTypes.TIMESTAMP))) {
                return TypeCompatibility.compatible(fromType, true);
            }
        }

        if (fromTypeBaseName.equals(toTypeBaseName)) {
            if (fromTypeBaseName.equals(StandardTypes.DECIMAL)) {
                Type commonSuperType = getCommonSuperTypeForDecimal((DecimalType) fromType, (DecimalType) toType);
                return TypeCompatibility.compatible(commonSuperType, commonSuperType.equals(toType));
            }
            if (fromTypeBaseName.equals(StandardTypes.VARCHAR)) {
                Type commonSuperType = getCommonSuperTypeForVarchar((VarcharType) fromType, (VarcharType) toType);
                return TypeCompatibility.compatible(commonSuperType, commonSuperType.equals(toType));
            }
            if (fromTypeBaseName.equals(StandardTypes.CHAR) && !featuresConfig.isLegacyCharToVarcharCoercion()) {
                Type commonSuperType = getCommonSuperTypeForChar((CharType) fromType, (CharType) toType);
                return TypeCompatibility.compatible(commonSuperType, commonSuperType.equals(toType));
            }
            if (fromTypeBaseName.equals(StandardTypes.ROW)) {
                return typeCompatibilityForRow((RowType) fromType, (RowType) toType);
            }

            if (isCovariantParametrizedType(fromType)) {
                return typeCompatibilityForCovariantParametrizedType(fromType, toType);
            }
            return TypeCompatibility.incompatible();
        }

        Optional<Type> coercedType = coerceTypeBase(fromType, toType.getTypeSignature().getBase());
        if (coercedType.isPresent()) {
            return compatibility(coercedType.get(), toType);
        }

        coercedType = coerceTypeBase(toType, fromType.getTypeSignature().getBase());
        if (coercedType.isPresent()) {
            TypeCompatibility typeCompatibility = compatibility(fromType, coercedType.get());
            if (!typeCompatibility.isCompatible()) {
                return TypeCompatibility.incompatible();
            }
            return TypeCompatibility.compatible(typeCompatibility.getCommonSuperType(), false);
        }

        return TypeCompatibility.incompatible();
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
        if (firstType.isUnbounded() || secondType.isUnbounded()) {
            return createUnboundedVarcharType();
        }

        return createVarcharType(Math.max(firstType.getLength(), secondType.getLength()));
    }

    private static Type getCommonSuperTypeForChar(CharType firstType, CharType secondType)
    {
        return createCharType(Math.max(firstType.getLength(), secondType.getLength()));
    }

    private TypeCompatibility typeCompatibilityForRow(RowType firstType, RowType secondType)
    {
        List<RowType.Field> firstFields = firstType.getFields();
        List<RowType.Field> secondFields = secondType.getFields();
        if (firstFields.size() != secondFields.size()) {
            return TypeCompatibility.incompatible();
        }

        ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
        boolean coercible = true;
        for (int i = 0; i < firstFields.size(); i++) {
            Type firstFieldType = firstFields.get(i).getType();
            Type secondFieldType = secondFields.get(i).getType();
            TypeCompatibility typeCompatibility = compatibility(firstFieldType, secondFieldType);
            if (!typeCompatibility.isCompatible()) {
                return TypeCompatibility.incompatible();
            }
            Type commonParameterType = typeCompatibility.getCommonSuperType();

            Optional<String> firstParameterName = firstFields.get(i).getName();
            Optional<String> secondParameterName = secondFields.get(i).getName();
            Optional<String> commonName = firstParameterName.equals(secondParameterName) ? firstParameterName : Optional.empty();

            // ignore parameter name for coercible
            coercible &= typeCompatibility.isCoercible();
            fields.add(new RowType.Field(commonName, commonParameterType));
        }

        return TypeCompatibility.compatible(RowType.from(fields.build()), coercible);
    }

    private TypeCompatibility typeCompatibilityForCovariantParametrizedType(Type fromType, Type toType)
    {
        checkState(fromType.getClass().equals(toType.getClass()));
        ImmutableList.Builder<TypeSignatureParameter> commonParameterTypes = ImmutableList.builder();
        List<Type> fromTypeParameters = fromType.getTypeParameters();
        List<Type> toTypeParameters = toType.getTypeParameters();

        if (fromTypeParameters.size() != toTypeParameters.size()) {
            return TypeCompatibility.incompatible();
        }

        boolean coercible = true;
        for (int i = 0; i < fromTypeParameters.size(); i++) {
            TypeCompatibility compatibility = compatibility(fromTypeParameters.get(i), toTypeParameters.get(i));
            if (!compatibility.isCompatible()) {
                return TypeCompatibility.incompatible();
            }
            coercible &= compatibility.isCoercible();
            commonParameterTypes.add(TypeSignatureParameter.of(compatibility.getCommonSuperType().getTypeSignature()));
        }
        String typeBase = fromType.getTypeSignature().getBase();
        return TypeCompatibility.compatible(functionAndTypeManager.getType(new TypeSignature(typeBase, commonParameterTypes.build())), coercible);
    }

    private static class TypeCompatibility
    {
        private final Optional<Type> commonSuperType;
        private final boolean coercible;

        // Do not call constructor directly. Use factory methods.
        private TypeCompatibility(Optional<Type> commonSuperType, boolean coercible)
        {
            // Assert that: coercible => commonSuperType.isPresent
            // The factory API is designed such that this is guaranteed.
            checkArgument(!coercible || commonSuperType.isPresent());

            this.commonSuperType = commonSuperType;
            this.coercible = coercible;
        }

        private static TypeCompatibility compatible(Type commonSuperType, boolean coercible)
        {
            return new TypeCompatibility(Optional.of(commonSuperType), coercible);
        }

        private static TypeCompatibility incompatible()
        {
            return new TypeCompatibility(Optional.empty(), false);
        }

        private boolean isCompatible()
        {
            return commonSuperType.isPresent();
        }

        private Type getCommonSuperType()
        {
            checkState(commonSuperType.isPresent(), "Types are not compatible");
            return commonSuperType.get();
        }

        private boolean isCoercible()
        {
            return coercible;
        }
    }
}
