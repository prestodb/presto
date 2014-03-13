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
package com.facebook.presto.cassandra;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import com.facebook.presto.cassandra.util.CassandraCqlUtils;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.InetAddresses;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.net.InetAddresses.toAddrString;

public enum CassandraType
        implements FullCassandraType
{
    ASCII(VarcharType.VARCHAR, String.class),
    BIGINT(BigintType.BIGINT, Long.class),
    BLOB(VarcharType.VARCHAR, ByteBuffer.class),
    CUSTOM(VarcharType.VARCHAR, ByteBuffer.class),
    BOOLEAN(BooleanType.BOOLEAN, Boolean.class),
    COUNTER(BigintType.BIGINT, Long.class),
    DECIMAL(DoubleType.DOUBLE, BigDecimal.class),
    DOUBLE(DoubleType.DOUBLE, Double.class),
    FLOAT(DoubleType.DOUBLE, Float.class),
    INET(VarcharType.VARCHAR, InetAddress.class),
    INT(BigintType.BIGINT, Integer.class),
    TEXT(VarcharType.VARCHAR, String.class),
    TIMESTAMP(BigintType.BIGINT, Date.class),
    UUID(VarcharType.VARCHAR, java.util.UUID.class),
    TIMEUUID(VarcharType.VARCHAR, java.util.UUID.class),
    VARCHAR(VarcharType.VARCHAR, String.class),
    VARINT(VarcharType.VARCHAR, BigInteger.class),
    LIST(VarcharType.VARCHAR, null),
    MAP(VarcharType.VARCHAR, null),
    SET(VarcharType.VARCHAR, null);

    private final Type nativeType;
    private final Class<?> javaType;

    CassandraType(Type nativeType, Class<?> javaType)
    {
        this.nativeType = checkNotNull(nativeType, "nativeType is null");
        this.javaType = javaType;
    }

    public Type getNativeType()
    {
        return nativeType;
    }

    public int getTypeArgumentSize()
    {
        switch (this) {
            case LIST:
            case SET:
                return 1;
            case MAP:
                return 2;
            default:
                return 0;
        }
    }

    public static CassandraType getSupportedCassandraType(DataType.Name name)
    {
        CassandraType cassandraType = getCassandraType(name);
        checkArgument(cassandraType != null, "Unknown Cassandra type: " + name);
        return cassandraType;
    }

    public static CassandraType getCassandraType(DataType.Name name)
    {
        switch (name) {
            case ASCII:
                return ASCII;
            case BIGINT:
                return BIGINT;
            case BLOB:
                return BLOB;
            case BOOLEAN:
                return BOOLEAN;
            case COUNTER:
                return COUNTER;
            case CUSTOM:
                return CUSTOM;
            case DECIMAL:
                return DECIMAL;
            case DOUBLE:
                return DOUBLE;
            case FLOAT:
                return FLOAT;
            case INET:
                return INET;
            case INT:
                return INT;
            case LIST:
                return LIST;
            case MAP:
                return MAP;
            case SET:
                return SET;
            case TEXT:
                return TEXT;
            case TIMESTAMP:
                return TIMESTAMP;
            case TIMEUUID:
                return TIMEUUID;
            case UUID:
                return UUID;
            case VARCHAR:
                return VARCHAR;
            case VARINT:
                return VARINT;
            default:
                return null;
        }
    }

    public static CassandraType getSupportedCassandraType(String cassandraTypeName)
    {
        CassandraType cassandraType = getCassandraType(cassandraTypeName);
        checkArgument(cassandraType != null, "Unknown Cassandra type: " + cassandraTypeName);
        return cassandraType;
    }

    public static CassandraType getCassandraType(String cassandraTypeName)
    {
        DataType.Name name = DataType.Name.valueOf(cassandraTypeName);
        if (name != null) {
            return getCassandraType(name);
        }
        return null;
    }

    public static Comparable<?> getColumnValue(Row row, int i, FullCassandraType fullCassandraType)
    {
        return getColumnValue(row, i, fullCassandraType.getCassandraType(), fullCassandraType.getTypeArguments());
    }

    public static Comparable<?> getColumnValue(Row row, int i, CassandraType cassandraType,
            List<CassandraType> typeArguments)
    {
        if (row.isNull(i)) {
            return null;
        }
        else {
            switch (cassandraType) {
                case ASCII:
                case TEXT:
                case VARCHAR:
                    return row.getString(i);
                case INT:
                    return (long) row.getInt(i);
                case BIGINT:
                case COUNTER:
                    return row.getLong(i);
                case BOOLEAN:
                    return row.getBool(i);
                case DOUBLE:
                    return row.getDouble(i);
                case FLOAT:
                    return (double) row.getFloat(i);
                case DECIMAL:
                    return row.getDecimal(i).doubleValue();
                case UUID:
                case TIMEUUID:
                    return row.getUUID(i).toString();
                case TIMESTAMP:
                    return row.getDate(i).getTime();
                case INET:
                    return toAddrString(row.getInet(i));
                case VARINT:
                    return row.getVarint(i).toString();
                case BLOB:
                case CUSTOM:
                    return Bytes.toHexString(row.getBytesUnsafe(i));
                case SET:
                    checkTypeArguments(cassandraType, 1, typeArguments);
                    return buildSetValue(row, i, typeArguments.get(0));
                case LIST:
                    checkTypeArguments(cassandraType, 1, typeArguments);
                    return buildListValue(row, i, typeArguments.get(0));
                case MAP:
                    checkTypeArguments(cassandraType, 2, typeArguments);
                    return buildMapValue(row, i, typeArguments.get(0), typeArguments.get(1));
                default:
                    throw new IllegalStateException("Handling of type " + cassandraType
                            + " is not implemented");
            }
        }
    }

    private static String buildSetValue(Row row, int i, CassandraType elemType)
    {
        return buildArrayValue(row.getSet(i, elemType.javaType), elemType);
    }

    private static String buildListValue(Row row, int i, CassandraType elemType)
    {
        return buildArrayValue(row.getList(i, elemType.javaType), elemType);
    }

    private static String buildMapValue(Row row, int i, CassandraType keyType, CassandraType valueType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<?, ?> entry : row.getMap(i, keyType.javaType, valueType.javaType).entrySet()) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(objectToString(entry.getKey(), keyType));
            sb.append(":");
            sb.append(objectToString(entry.getValue(), valueType));
        }
        sb.append("}");
        return sb.toString();
    }

    @VisibleForTesting
    static String buildArrayValue(Collection<?> collection, CassandraType elemType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Object value : collection) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(objectToString(value, elemType));
        }
        sb.append("]");
        return sb.toString();
    }

    private static void checkTypeArguments(CassandraType type, int expectedSize,
            List<CassandraType> typeArguments)
    {
        if (typeArguments == null || typeArguments.size() != expectedSize) {
            throw new IllegalArgumentException("Wrong number of type arguments " + typeArguments
                    + " for " + type);
        }
    }

    public static String getColumnValueForCql(Row row, int i, CassandraType cassandraType)
    {
        if (row.isNull(i)) {
            return null;
        }
        else {
            switch (cassandraType) {
                case ASCII:
                case TEXT:
                case VARCHAR:
                    return CassandraCqlUtils.quoteStringLiteral(row.getString(i));
                case INT:
                    return Integer.toString(row.getInt(i));
                case BIGINT:
                case COUNTER:
                    return Long.toString(row.getLong(i));
                case BOOLEAN:
                    return Boolean.toString(row.getBool(i));
                case DOUBLE:
                    return Double.toString(row.getDouble(i));
                case FLOAT:
                    return Float.toString(row.getFloat(i));
                case DECIMAL:
                    return row.getDecimal(i).toString();
                case UUID:
                case TIMEUUID:
                    return row.getUUID(i).toString();
                case TIMESTAMP:
                    return Long.toString(row.getDate(i).getTime());
                case INET:
                    return CassandraCqlUtils.quoteStringLiteral(toAddrString(row.getInet(i)));
                case VARINT:
                    return row.getVarint(i).toString();
                case BLOB:
                case CUSTOM:
                    return Bytes.toHexString(row.getBytesUnsafe(i));
                default:
                    throw new IllegalStateException("Handling of type " + cassandraType
                            + " is not implemented");
            }
        }
    }

    private static String objectToString(Object object, CassandraType elemType)
    {
        switch (elemType) {
            case ASCII:
            case TEXT:
            case VARCHAR:
            case UUID:
            case TIMEUUID:
            case TIMESTAMP:
            case INET:
            case VARINT:
                return CassandraCqlUtils.quoteStringLiteralForJson(object.toString());

            case BLOB:
            case CUSTOM:
                return CassandraCqlUtils.quoteStringLiteralForJson(Bytes.toHexString((ByteBuffer) object));

            case INT:
            case BIGINT:
            case COUNTER:
            case BOOLEAN:
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
                return object.toString();
            default:
                throw new IllegalStateException("Handling of type " + elemType + " is not implemented");
        }
    }

    @Override
    public CassandraType getCassandraType()
    {
        if (getTypeArgumentSize() == 0) {
            return this;
        }
        else {
            // must not be called for types with type arguments
            throw new IllegalStateException();
        }
    }

    @Override
    public List<CassandraType> getTypeArguments()
    {
        if (getTypeArgumentSize() == 0) {
            return null;
        }
        else {
            // must not be called for types with type arguments
            throw new IllegalStateException();
        }
    }

    public Object getJavaValue(Comparable<?> comparable)
    {
        switch (this) {
            case ASCII:
            case TEXT:
            case VARCHAR:
            case BIGINT:
            case BOOLEAN:
            case DOUBLE:
            case COUNTER:
                return comparable;
            case INET:
                return InetAddresses.forString((String) comparable);
            case INT:
                return ((Long) comparable).intValue();
            case FLOAT:
                // conversion can result in precision lost
                return ((Double) comparable).floatValue();
            case DECIMAL:
                // conversion can result in precision lost
                // Presto uses double for decimal, so to keep the floating point precision, convert it to string.
                // Otherwise partition id doesn't match
                return new BigDecimal(comparable.toString());
            case TIMESTAMP:
                return new Date((Long) comparable);
            case UUID:
            case TIMEUUID:
                return java.util.UUID.fromString((String) comparable);
            case BLOB:
            case CUSTOM:
                return Bytes.fromHexString((String) comparable);
            case VARINT:
                return new BigInteger((String) comparable);
            case SET:
            case LIST:
            case MAP:
            default:
                throw new IllegalStateException("Back conversion not implemented for " + this);
        }
    }

    public static CassandraType toCassandraType(Type type)
    {
        if (type.equals(BooleanType.BOOLEAN)) {
            return BOOLEAN;
        }
        else if (type.equals(BigintType.BIGINT)) {
            return BIGINT;
        }
        else if (type.equals(DoubleType.DOUBLE)) {
            return DOUBLE;
        }
        else if (type.equals(VarcharType.VARCHAR)) {
            return TEXT;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }
}
