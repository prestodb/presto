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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.elasticsearch.metadata.EsField;
import com.facebook.presto.elasticsearch.metadata.UnsupportedEsField;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.slice.Slice;

import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class EsTypeManager
{
    private final TypeManager typeManager;

    @Inject
    private EsTypeManager(final TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public Type toPrestoType(EsField esField)
    {
        return toPrestoType(new StringBuilder(), esField);
    }

    private Type toPrestoType(final StringBuilder lastFieldName, EsField esField)
    {
        lastFieldName.append(".").append(esField.getName());
        final Type type;
        switch (esField.getDataType()) {
            case NULL:
            case OBJECT:
                if (esField.hasDocValues() && !esField.getProperties().isEmpty()) {
                    type = RowType.from(esField.getProperties().values()
                            .stream().map(x -> RowType.field(x.getName(), toPrestoType(lastFieldName, x)))
                            .collect(Collectors.toList()));
                    break;
                }
                throw new UnsupportedOperationException("this " + esField.getName() + " esType is OBJECT but not hasDoc have't support!");
            case UNSUPPORTED: {
                String stringType = ((UnsupportedEsField) esField).getOriginalType();
                if ("string".equals(stringType) || "ip".equals(stringType)) {
                    type = VARCHAR;
                    break;
                }
                if ("geo_point".equals(stringType)) {  //Geographic type latitude and longitude value
                    type = mapType(VARCHAR, DOUBLE);
                    break;
                }
                type = VARCHAR;
                break;
            }
            case BOOLEAN:
                type = BOOLEAN;
                break;
            case SHORT:
                type = SMALLINT;
                break;
            case LONG:
                type = BIGINT;
                break;
            case INTEGER:
                type = INTEGER;
                break;
            case FLOAT:
                type = REAL;
                break;
            case SCALED_FLOAT:
            case HALF_FLOAT:
            case DOUBLE:
                type = DOUBLE;
                break;
            case TEXT:
            case KEYWORD:
                type = VARCHAR;
                break;
            case BINARY:
                type = VARBINARY;
                break;
            case DATE:
                type = DATE;
                break;
            default:
                throw new UnsupportedOperationException("column " + esField.getName() + "esType " + esField.getDataType() + " have't support!");
        }
        return type;
    }

    public MapType mapType(Type keyType, Type valueType)
    {
        return (MapType) typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));
    }

    public ArrayType arrayType(Type valueType)
    {
        return (ArrayType) typeManager.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(
                TypeSignatureParameter.of(valueType.getTypeSignature())));
    }

    public static Object getTypeValue(Type type, Object value)
    {
        Object toEncode;
        if (Types.isArrayType(type)) {
            throw new UnsupportedOperationException("Unsupported type " + type);
        }
        else if (Types.isMapType(type)) {
            throw new UnsupportedOperationException("Unsupported type " + type);
        }
        else if (type.equals(VARBINARY)) {
            return ((Slice) value).getBytes();
        }
        else if (type instanceof VarcharType) {
            return ((Slice) value).toStringUtf8();
        }
        else {
            return value;
        }
    }
}
