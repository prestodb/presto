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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.net.InetAddresses.toAddrString;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;

public enum CassandraType
        implements FullCassandraType
{
    ASCII(VarcharType.VARCHAR, String.class),
    BIGINT(BigintType.BIGINT, Long.class),
    BLOB(VarbinaryType.VARBINARY, ByteBuffer.class),
    CUSTOM(VarbinaryType.VARBINARY, ByteBuffer.class),
    BOOLEAN(BooleanType.BOOLEAN, Boolean.class),
    COUNTER(BigintType.BIGINT, Long.class),
    DECIMAL(DoubleType.DOUBLE, BigDecimal.class),
    DOUBLE(DoubleType.DOUBLE, Double.class),
    FLOAT(DoubleType.DOUBLE, Float.class),
    INET(VarcharType.VARCHAR, InetAddress.class),
    INT(IntegerType.INTEGER, Integer.class),
    TEXT(VarcharType.VARCHAR, String.class),
    TIMESTAMP(TimestampType.TIMESTAMP, Date.class),
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
        this.nativeType = requireNonNull(nativeType, "nativeType is null");
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

    public static NullableValue getColumnValue(Row row, int i, FullCassandraType fullCassandraType)
    {
        return getColumnValue(row, i, fullCassandraType.getCassandraType(), fullCassandraType.getTypeArguments());
    }

    public static NullableValue getColumnValue(Row row, int i, CassandraType cassandraType,
            List<CassandraType> typeArguments)
    {
        Type nativeType = cassandraType.getNativeType();
        if (row.isNull(i)) {
            return NullableValue.asNull(nativeType);
        }
        else {
            switch (cassandraType) {
                case ASCII:
                case TEXT:
                case VARCHAR:
                    return NullableValue.of(nativeType, utf8Slice(row.getString(i)));
                case INT:
                    return NullableValue.of(nativeType, (long) row.getInt(i));
                case BIGINT:
                case COUNTER:
                    return NullableValue.of(nativeType, row.getLong(i));
                case BOOLEAN:
                    return NullableValue.of(nativeType, row.getBool(i));
                case DOUBLE:
                    return NullableValue.of(nativeType, row.getDouble(i));
                case FLOAT:
                    return NullableValue.of(nativeType, (double) row.getFloat(i));
                case DECIMAL:
                    return NullableValue.of(nativeType, row.getDecimal(i).doubleValue());
                case UUID:
                case TIMEUUID:
                    return NullableValue.of(nativeType, utf8Slice(row.getUUID(i).toString()));
                case TIMESTAMP:
                    return NullableValue.of(nativeType, row.getTimestamp(i).getTime());
                case INET:
                    return NullableValue.of(nativeType, utf8Slice(toAddrString(row.getInet(i))));
                case VARINT:
                    return NullableValue.of(nativeType, utf8Slice(row.getVarint(i).toString()));
                case BLOB:
                case CUSTOM:
                    return NullableValue.of(nativeType, wrappedBuffer(row.getBytesUnsafe(i)));
                case SET:
                    checkTypeArguments(cassandraType, 1, typeArguments);
                    return NullableValue.of(nativeType, utf8Slice(buildSetValue(row, i, typeArguments.get(0))));
                case LIST:
                    checkTypeArguments(cassandraType, 1, typeArguments);
                    return NullableValue.of(nativeType, utf8Slice(buildListValue(row, i, typeArguments.get(0))));
                case MAP:
                    checkTypeArguments(cassandraType, 2, typeArguments);
                    return NullableValue.of(nativeType, utf8Slice(buildMapValue(row, i, typeArguments.get(0), typeArguments.get(1))));
                default:
                    throw new IllegalStateException("Handling of type " + cassandraType
                            + " is not implemented");
            }
        }
    }

    public static NullableValue getColumnValueForPartitionKey(Row row, int i, CassandraType cassandraType, List<CassandraType> typeArguments)
    {
        Type nativeType = cassandraType.getNativeType();
        if (row.isNull(i)) {
            return NullableValue.asNull(nativeType);
        }
        switch (cassandraType) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return NullableValue.of(nativeType, utf8Slice(row.getString(i)));
            case UUID:
            case TIMEUUID:
                return NullableValue.of(nativeType, utf8Slice(row.getUUID(i).toString()));
            default:
                return getColumnValue(row, i, cassandraType, typeArguments);
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
                    return Long.toString(row.getTimestamp(i).getTime());
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

    public Object getJavaValue(Object nativeValue)
    {
        switch (this) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return ((Slice) nativeValue).toStringUtf8();
            case BIGINT:
            case BOOLEAN:
            case DOUBLE:
            case COUNTER:
                return nativeValue;
            case INET:
                return InetAddresses.forString(((Slice) nativeValue).toStringUtf8());
            case INT:
                return ((Long) nativeValue).intValue();
            case FLOAT:
                // conversion can result in precision lost
                return ((Double) nativeValue).floatValue();
            case DECIMAL:
                // conversion can result in precision lost
                // Presto uses double for decimal, so to keep the floating point precision, convert it to string.
                // Otherwise partition id doesn't match
                return new BigDecimal(nativeValue.toString());
            case TIMESTAMP:
                return new Date((Long) nativeValue);
            case UUID:
            case TIMEUUID:
                return java.util.UUID.fromString(((Slice) nativeValue).toStringUtf8());
            case BLOB:
            case CUSTOM:
                return ((Slice) nativeValue).toStringUtf8();
            case VARINT:
                return new BigInteger(((Slice) nativeValue).toStringUtf8());
            case SET:
            case LIST:
            case MAP:
            default:
                throw new IllegalStateException("Back conversion not implemented for " + this);
        }
    }

    public Object validatePartitionKey(Object value)
    {
        switch (this) {
            case ASCII:
            case TEXT:
            case VARCHAR:
            case BIGINT:
            case BOOLEAN:
            case DOUBLE:
            case INET:
            case INT:
            case FLOAT:
            case DECIMAL:
            case TIMESTAMP:
            case UUID:
            case TIMEUUID:
                return value;
            case COUNTER:
            case BLOB:
            case CUSTOM:
            case VARINT:
            case SET:
            case LIST:
            case MAP:
            default:
                // todo should we just skip partition pruning instead of throwing an exception?
                throw new PrestoException(NOT_SUPPORTED, "Unsupport partition key type: " + this);
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
        else if (type.equals(IntegerType.INTEGER)) {
            return INT;
        }
        else if (type.equals(DoubleType.DOUBLE)) {
            return DOUBLE;
        }
        else if (type.equals(VarcharType.VARCHAR)) {
            return TEXT;
        }
        else if (type.equals(DateType.DATE)) {
            return TEXT;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }
}
