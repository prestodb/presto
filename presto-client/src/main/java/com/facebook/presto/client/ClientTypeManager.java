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
package com.facebook.presto.client;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.HyperLogLogType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.JsonType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.ParameterKind;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowFieldName;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimeWithTimeZoneType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.UuidType;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;

public class ClientTypeManager
        implements TypeManager
{
    private final Map<TypeSignature, Type> typeCache = new ConcurrentHashMap<>();

    @Override
    public Type getType(TypeSignature signature)
    {
        Type cachedType = typeCache.get(signature);
        if (cachedType != null) {
            return cachedType;
        }

        Type type = getTypeInternal(signature);
        typeCache.put(signature, type);
        return type;
    }

    private Type getTypeInternal(TypeSignature signature)
    {
        String baseType = signature.getBase();

        switch (baseType) {
            case StandardTypes.BOOLEAN:
                return BooleanType.BOOLEAN;
            case StandardTypes.TINYINT:
                return TinyintType.TINYINT;
            case StandardTypes.SMALLINT:
                return SmallintType.SMALLINT;
            case StandardTypes.INTEGER:
                return IntegerType.INTEGER;
            case StandardTypes.BIGINT:
                return BigintType.BIGINT;
            case StandardTypes.REAL:
                return RealType.REAL;
            case StandardTypes.DOUBLE:
                return DoubleType.DOUBLE;
            case StandardTypes.VARCHAR:
                return createVarcharType(signature);
            case StandardTypes.CHAR:
                return createCharType(signature);
            case StandardTypes.VARBINARY:
                return VarbinaryType.VARBINARY;
            case StandardTypes.DATE:
                return DateType.DATE;
            case StandardTypes.TIME:
                return TimeType.TIME;
            case StandardTypes.TIME_WITH_TIME_ZONE:
                return TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
            case StandardTypes.TIMESTAMP:
                return TimestampType.TIMESTAMP;
            case StandardTypes.TIMESTAMP_WITH_TIME_ZONE:
                return TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
            case StandardTypes.DECIMAL:
                return createDecimalType(signature);
            case StandardTypes.JSON:
                return JsonType.JSON;
            case StandardTypes.UUID:
                return UuidType.UUID;
            case StandardTypes.HYPER_LOG_LOG:
                return HyperLogLogType.HYPER_LOG_LOG;
            case StandardTypes.ARRAY:
                return createArrayType(signature);
            case StandardTypes.MAP:
                return createMapType(signature);
            case StandardTypes.ROW:
                return createRowType(signature);
            default:
                throw new IllegalArgumentException("Unknown type: " + signature);
        }
    }

    private Type createVarcharType(TypeSignature signature)
    {
        if (signature.getParameters().isEmpty()) {
            return VarcharType.VARCHAR;
        }
        List<TypeSignatureParameter> parameters = signature.getParameters();
        checkArgument(parameters.size() == 1, "VARCHAR type must have at most one parameter");
        TypeSignatureParameter parameter = parameters.get(0);
        checkArgument(parameter.getKind() == ParameterKind.LONG, "VARCHAR length must be a number");
        long length = parameter.getLongLiteral();
        if (length == Integer.MAX_VALUE) {
            return VarcharType.createUnboundedVarcharType();
        }
        return VarcharType.createVarcharType((int) length);
    }

    private Type createCharType(TypeSignature signature)
    {
        if (signature.getParameters().isEmpty()) {
            return CharType.createCharType(1);
        }
        List<TypeSignatureParameter> parameters = signature.getParameters();
        checkArgument(parameters.size() == 1, "CHAR type must have at most one parameter");
        TypeSignatureParameter parameter = parameters.get(0);
        checkArgument(parameter.getKind() == ParameterKind.LONG, "CHAR length must be a number");
        long length = parameter.getLongLiteral();
        return CharType.createCharType((int) length);
    }

    private Type createDecimalType(TypeSignature signature)
    {
        List<TypeSignatureParameter> parameters = signature.getParameters();
        checkArgument(parameters.size() == 2, "DECIMAL type must have exactly two parameters");

        TypeSignatureParameter precisionParam = parameters.get(0);
        TypeSignatureParameter scaleParam = parameters.get(1);

        checkArgument(precisionParam.getKind() == ParameterKind.LONG, "DECIMAL precision must be a number");
        checkArgument(scaleParam.getKind() == ParameterKind.LONG, "DECIMAL scale must be a number");

        long precision = precisionParam.getLongLiteral();
        long scale = scaleParam.getLongLiteral();

        return DecimalType.createDecimalType((int) precision, (int) scale);
    }

    private Type createArrayType(TypeSignature signature)
    {
        List<TypeSignatureParameter> parameters = signature.getParameters();
        checkArgument(parameters.size() == 1, "ARRAY type must have exactly one parameter");

        TypeSignatureParameter elementParam = parameters.get(0);
        checkArgument(elementParam.getKind() == ParameterKind.TYPE, "ARRAY element must be a type");

        Type elementType = getType(elementParam.getTypeSignature());
        return new ArrayType(elementType);
    }

    private Type createMapType(TypeSignature signature)
    {
        List<TypeSignatureParameter> parameters = signature.getParameters();
        checkArgument(parameters.size() == 2, "MAP type must have exactly two parameters");

        TypeSignatureParameter keyParam = parameters.get(0);
        TypeSignatureParameter valueParam = parameters.get(1);

        checkArgument(keyParam.getKind() == ParameterKind.TYPE, "MAP key must be a type");
        checkArgument(valueParam.getKind() == ParameterKind.TYPE, "MAP value must be a type");

        Type keyType = getType(keyParam.getTypeSignature());
        Type valueType = getType(valueParam.getTypeSignature());

        MethodHandle keyEquals = generateKeyEqualsHandle(keyType);
        MethodHandle keyHashCode = generateKeyHashCodeHandle(keyType);

        return new MapType(keyType, valueType, keyEquals, keyHashCode);
    }

    private Type createRowType(TypeSignature signature)
    {
        List<TypeSignatureParameter> parameters = signature.getParameters();
        checkArgument(!parameters.isEmpty(), "ROW type must have at least one field");

        List<RowType.Field> fields = new ArrayList<>();
        for (TypeSignatureParameter param : parameters) {
            checkArgument(param.getKind() == ParameterKind.NAMED_TYPE, "ROW parameters must be named types");

            NamedTypeSignature namedType = param.getNamedTypeSignature();
            Type fieldType = getType(namedType.getTypeSignature());

            Optional<String> fieldName;
            boolean isDelimited = false;

            if (namedType.getFieldName().isPresent()) {
                RowFieldName rowFieldName = namedType.getFieldName().get();
                fieldName = Optional.of(rowFieldName.getName());
                isDelimited = rowFieldName.isDelimited();
            }
            else {
                fieldName = Optional.empty();
            }

            fields.add(new RowType.Field(fieldName, fieldType, isDelimited));
        }

        return RowType.from(fields);
    }

    private static class MapOperators
    {
        public static boolean blockEquals(Type keyType, Block leftBlock, int leftPos, Block rightBlock, int rightPos)
        {
            return keyType.equalTo(leftBlock, leftPos, rightBlock, rightPos);
        }

        public static long blockHashCode(Type keyType, Block block, int position)
        {
            return keyType.hash(block, position);
        }
    }

    private MethodHandle generateKeyEqualsHandle(Type keyType)
    {
        try {
            MethodHandle base = MethodHandles.lookup().findStatic(
                    MapOperators.class,
                    "blockEquals",
                    MethodType.methodType(boolean.class, Type.class, Block.class, int.class, Block.class, int.class));
            return MethodHandles.insertArguments(base, 0, keyType);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create key equals handle for type: " + keyType, e);
        }
    }

    private MethodHandle generateKeyHashCodeHandle(Type keyType)
    {
        try {
            MethodHandle base = MethodHandles.lookup().findStatic(
                    MapOperators.class,
                    "blockHashCode",
                    MethodType.methodType(long.class, Type.class, Block.class, int.class));
            return MethodHandles.insertArguments(base, 0, keyType);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create key hash code handle for type: " + keyType, e);
        }
    }

    @Override
    public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
    {
        return getType(new TypeSignature(baseTypeName, typeParameters));
    }

    @Override
    public boolean canCoerce(Type actualType, Type expectedType)
    {
        return false;
    }

    @Override
    public List<Type> getTypes()
    {
        throw new UnsupportedOperationException("getTypes not supported");
    }

    @Override
    public boolean hasType(TypeSignature signature)
    {
        try {
            return getType(signature) != null;
        }
        catch (Exception e) {
            return false;
        }
    }
}
