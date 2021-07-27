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
package com.facebook.presto.maxcompute.util;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.ParameterKind;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowFieldName;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeParameter;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.MapParametricType.MAP;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public class DataTypes
{
    public static final int MAXCOMPUTE_DEFAULT_SCALE = 18;
    public static final int MAXCOMPUTE_DEFAULT_PRECISION = 38;
    private DataTypes() {}

    public static Type convertToPrestoType(String columnType, TypeManager typeManager)
    {
        TypeSignature ts = parseTypeSignature(columnType.replace(':', ' ').toLowerCase(Locale.ENGLISH));
        switch (ts.getBase()) {
            case StandardTypes.BOOLEAN:
                return BooleanType.BOOLEAN;

            case StandardTypes.SMALLINT:
                return SmallintType.SMALLINT;

            case StandardTypes.TINYINT:
                return TinyintType.TINYINT;

            case StandardTypes.INTEGER:
            case "int":
            case "mediumint":
                return IntegerType.INTEGER;

            case StandardTypes.BIGINT:
                return BigintType.BIGINT;

            case StandardTypes.REAL:
            case "float":
                return RealType.REAL;

            case StandardTypes.DOUBLE:
                return DoubleType.DOUBLE;
            case StandardTypes.DECIMAL:
                if (ts.getParameters().isEmpty()) {
                    return DecimalType.createDecimalType(MAXCOMPUTE_DEFAULT_PRECISION, MAXCOMPUTE_DEFAULT_SCALE);
                }
                else {
                    if (ts.getParameters().size() == 2) {
                        int precision = getIntValue(ts.getParameters().get(0));
                        int scale = getIntValue(ts.getParameters().get(1));
                        return DecimalType.createDecimalType(precision, scale);
                    }
                    else {
                        break;
                    }
                }
            case StandardTypes.CHAR:
            case StandardTypes.VARCHAR:
                if (ts.getParameters() != null
                        && ts.getParameters().size() == 1
                        && ts.getParameters().get(0).isLongLiteral()
                        && ts.getParameters().get(0).getLongLiteral() > 0
                        && ts.getParameters().get(0).getLongLiteral() <= VarcharType.MAX_LENGTH) {
                    return VarcharType.createVarcharType(ts.getParameters().get(0).getLongLiteral().intValue());
                }
                else {
                    return VarcharType.VARCHAR;
                }
            case "string":
            case "text":
            case "mediumtext":
            case "longtext":
            case "json":
                return VarcharType.VARCHAR;

            case StandardTypes.VARBINARY:
                return VarbinaryType.VARBINARY;

            case StandardTypes.DATE:
                return DateType.DATE;

            case StandardTypes.TIME:
                return TimeType.TIME;

            case StandardTypes.TIMESTAMP:
            case "datetime":
                return TimestampType.TIMESTAMP;

            case StandardTypes.ARRAY:
                TypeSignatureParameter typeSignatureParameter = ts.getParameters().get(0);
                String base = typeSignatureParameter.getTypeSignature().getBase();
                List<TypeSignatureParameter> typeParameters = typeSignatureParameter.getTypeSignature().getParameters();
                String sonType = makeTypeStr(base, typeParameters);
                Type elementType = convertToPrestoType(sonType, typeManager);
                return new ArrayType(elementType);

            case StandardTypes.MAP:
                List<TypeParameter> parameters = ts.getParameters().stream().map(t -> {
                    String tempBase = t.getTypeSignature().getBase();
                    List<TypeSignatureParameter> typeParameters1 = t.getTypeSignature().getParameters();
                    String sonType1 = makeTypeStr(tempBase, typeParameters1);
                    Type mapElementType = convertToPrestoType(sonType1, typeManager);
                    TypeSignatureParameter tsp = TypeSignatureParameter.of(mapElementType.getTypeSignature());
                    return TypeParameter.of(tsp, typeManager);
                }).collect(Collectors.toList());

                return createMapType(typeManager, parameters);

            case "struct":
                List<TypeSignatureParameter> rowParameters = ts.getParameters().stream().map(t -> {
                    String tempBase = t.getTypeSignature().getBase().trim();
                    /*
                    The MaxCompute struct type string is like STRUCT<a:INT, b:BOOLEAN, c:STRING>.
                    Since ':' is used to express named types in presto, so the ':' was replaced to space
                    before calling parseTypeSignature. So the type parameters is like "a INT"...
                    */
                    int space = tempBase.indexOf(' ');
                    checkArgument(space != -1, "Expected format is fieldName fieldType, eg: x INT, got %s", tempBase);
                    String rowFieldName = tempBase.substring(0, space).trim().toLowerCase(Locale.ENGLISH);
                    String rowFieldType = tempBase.substring(space + 1).trim().toLowerCase(Locale.ENGLISH);

                    List<TypeSignatureParameter> typeSignatureParameters = t.getTypeSignature().getParameters();
                    String structSonType = makeTypeStr(rowFieldType, typeSignatureParameters);
                    Type mapElementType = convertToPrestoType(structSonType, typeManager);
                    TypeSignature typeSignature = mapElementType.getTypeSignature();
                    RowFieldName rfn = new RowFieldName(rowFieldName, false);
                    NamedTypeSignature namedTypeSignature = new NamedTypeSignature(Optional.of(rfn), typeSignature);

                    return TypeSignatureParameter.of(namedTypeSignature);
                }).collect(toList());

                return createRowType(rowParameters, typeManager);
        }
        throw new IllegalArgumentException("Can't convert to MPP type, columnType=" + columnType);
    }

    static int getIntValue(TypeSignatureParameter parameter)
    {
        switch (parameter.getKind()) {
            case LONG:
                return parameter.getLongLiteral().intValue();
            case TYPE:
                return Integer.parseInt(parameter.getTypeSignature().getBase());
        }
        throw new IllegalArgumentException("expected a int value, found" + parameter.getKind());
    }

    private static Type createMapType(TypeManager typeManager, List<TypeParameter> parameters)
    {
        if (typeManager instanceof FunctionAndTypeManager) {
            return MAP.createType((FunctionAndTypeManager) typeManager, parameters);
        }
        throw new IllegalArgumentException("map type is not supported");
    }

    private static Type createRowType(List<TypeSignatureParameter> parameters, TypeManager typeManager)
    {
        checkArgument(!parameters.isEmpty(), "Row type must have at least one parameter");
        checkArgument(
                parameters.stream().allMatch(parameter -> parameter.getKind() == ParameterKind.NAMED_TYPE),
                "Expected only named types as a parameters, got %s",
                parameters);

        List<RowType.Field> fields = parameters.stream()
                .map(tsp -> TypeParameter.of(tsp, typeManager))
                .map(TypeParameter::getNamedType)
                .map(parameter -> new RowType.Field(parameter.getName().map(RowFieldName::getName), parameter.getType()))
                .collect(toList());

        return RowType.createWithTypeSignature(new TypeSignature(StandardTypes.ROW, parameters), fields);
    }

    private static String makeTypeStr(String base, List<TypeSignatureParameter> parameters)
    {
        if (parameters.isEmpty()) {
            return base;
        }

        if (base.equalsIgnoreCase(StandardTypes.VARCHAR) &&
                (parameters.size() == 1) &&
                parameters.get(0).isLongLiteral() &&
                parameters.get(0).getLongLiteral() == VarcharType.UNBOUNDED_LENGTH) {
            return base;
        }

        StringBuilder typeName = new StringBuilder(base);
        typeName.append("<").append(parameters.get(0));
        for (int i = 1; i < parameters.size(); i++) {
            typeName.append(",").append(parameters.get(i));
        }
        typeName.append(">");
        return typeName.toString();
    }
}
