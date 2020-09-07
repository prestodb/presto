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
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.net.InetAddresses.toAddrString;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.util.Objects.requireNonNull;

public enum CassandraType
{
    ASCII(createUnboundedVarcharType()),
    BIGINT(BigintType.BIGINT),
    BLOB(VarbinaryType.VARBINARY),
    CUSTOM(VarbinaryType.VARBINARY),
    BOOLEAN(BooleanType.BOOLEAN),
    COUNTER(BigintType.BIGINT),
    DECIMAL(DoubleType.DOUBLE),
    DOUBLE(DoubleType.DOUBLE),
    FLOAT(RealType.REAL),
    INET(createVarcharType(Constants.IP_ADDRESS_STRING_MAX_LENGTH)),
    INT(IntegerType.INTEGER),
    SMALLINT(SmallintType.SMALLINT),
    TINYINT(TinyintType.TINYINT),
    TEXT(createUnboundedVarcharType()),
    DATE(DateType.DATE),
    TIMESTAMP(TimestampType.TIMESTAMP),
    UUID(createVarcharType(Constants.UUID_STRING_MAX_LENGTH)),
    TIMEUUID(createVarcharType(Constants.UUID_STRING_MAX_LENGTH)),
    VARCHAR(createUnboundedVarcharType()),
    VARINT(createUnboundedVarcharType()),
    LIST(createUnboundedVarcharType()),
    MAP(createUnboundedVarcharType()),
    SET(createUnboundedVarcharType());

    private static class Constants
    {
        private static final int UUID_STRING_MAX_LENGTH = 36;
        // IPv4: 255.255.255.255 - 15 characters
        // IPv6: FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF - 39 characters
        // IPv4 embedded into IPv6: FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:255.255.255.255 - 45 characters
        private static final int IP_ADDRESS_STRING_MAX_LENGTH = 45;
    }

    private final Type nativeType;

    CassandraType(Type nativeType, Class<?> javaType)
    {
        this.nativeType = requireNonNull(nativeType, "nativeType is null");
    }

    public Type getNativeType()
    {
        return nativeType;
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

    public static NullableValue getColumnValue(Row row, int position, CassandraType cassandraType)
    {
        Type nativeType = cassandraType.getNativeType();
        if (row.isNull(position)) {
            return NullableValue.asNull(nativeType);
        }
        else {
            switch (cassandraType) {
                case ASCII:
                case TEXT:
                case VARCHAR:
                    return NullableValue.of(nativeType, utf8Slice(row.getString(position)));
                case INT:
                    return NullableValue.of(nativeType, (long) row.getInt(position));
                case BIGINT:
                case COUNTER:
                    return NullableValue.of(nativeType, row.getLong(position));
                case BOOLEAN:
                    return NullableValue.of(nativeType, row.getBool(position));
                case DOUBLE:
                    return NullableValue.of(nativeType, row.getDouble(position));
                case FLOAT:
                    return NullableValue.of(nativeType, (long) floatToRawIntBits(row.getFloat(position)));
                case DECIMAL:
                    return NullableValue.of(nativeType, row.getDecimal(position).doubleValue());
                case UUID:
                case TIMEUUID:
                    return NullableValue.of(nativeType, utf8Slice(row.getUUID(position).toString()));
                case TIMESTAMP:
                    return NullableValue.of(nativeType, row.getTimestamp(position).getTime());
                case INET:
                    return NullableValue.of(nativeType, utf8Slice(toAddrString(row.getInet(position))));
                case VARINT:
                    return NullableValue.of(nativeType, utf8Slice(row.getVarint(position).toString()));
                case BLOB:
                case CUSTOM:
                    return NullableValue.of(nativeType, wrappedBuffer(row.getBytesUnsafe(position)));
                case SET:

                case LIST:
                    return NullableValue.of(nativeType, utf8Slice(buildArrayValue(row, position)));
                case MAP:
                    return NullableValue.of(nativeType, utf8Slice(buildMapValue(row, position)));
                default:
                    throw new IllegalStateException("Handling of type " + cassandraType
                            + " is not implemented");
            }
        }
    }

    public static NullableValue getColumnValueForPartitionKey(Row row, int position, CassandraType cassandraType)
    {
        Type nativeType = cassandraType.getNativeType();
        if (row.isNull(position)) {
            return NullableValue.asNull(nativeType);
        }
        switch (cassandraType) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return NullableValue.of(nativeType, utf8Slice(row.getString(position)));
            case UUID:
            case TIMEUUID:
                return NullableValue.of(nativeType, utf8Slice(row.getUUID(position).toString()));
            default:
                return getColumnValue(row, position, cassandraType);
        }
    }

    private static String buildMapValue(Row row, int position)
    {
        DataType type = row.getColumnDefinitions().getType(position);
        checkArgument(type.getTypeArguments().size() == 2, "Expected two type arguments, got: %s", type.getTypeArguments());
        DataType keyType = type.getTypeArguments().get(0);
        DataType valueType = type.getTypeArguments().get(1);
        return buildMapValue((Map<?, ?>) row.getObject(position), keyType, valueType);
    }

    private static String buildMapValue(Map<?, ?> map, DataType keyType, DataType valueType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<?, ?> entry : map.entrySet()) {
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

    private static String buildArrayValue(Row row, int position)
    {
        DataType type = row.getColumnDefinitions().getType(position);
        DataType elementType = getOnlyElement(type.getTypeArguments());
        return buildArrayValue((Collection<?>) row.getObject(position), elementType);
    }

    @VisibleForTesting
    static String buildArrayValue(Collection<?> collection, DataType elementType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Object value : collection) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(objectToString(value, elementType));
        }
        sb.append("]");
        return sb.toString();
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

    private static String objectToString(Object object, DataType dataType)
    {
        CassandraType cassandraType = toCassandraType(dataType.getName())
            .orElseThrow(() -> new IllegalStateException("Unsupported type: " + dataType));

        switch (cassandraType) {
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
            case LIST:
            case SET:
                return buildArrayValue((Collection<?>) object, getOnlyElement(dataType.getTypeArguments()));
            case MAP:
                return buildMapValue((Map<?, ?>) object, dataType.getTypeArguments().get(0), dataType.getTypeArguments().get(1));
            default:
                throw new IllegalStateException("Unsupported type: " + cassandraType);
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
                return intBitsToFloat(((Long) nativeValue).intValue());
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

    public Object validateClusteringKey(Object value)
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
                throw new PrestoException(NOT_SUPPORTED, "Unsupported clustering key type: " + this);
        }
    }

    public static boolean isFullySupported(DataType dataType)
    {
        if (!toCassandraType(dataType.getName()).isPresent()) {
            return false;
        }

        return dataType.getTypeArguments().stream()
            .allMatch(CassandraType::isFullySupported);
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
        else if (type.equals(RealType.REAL)) {
            return FLOAT;
        }
        else if (isVarcharType(type)) {
            return TEXT;
        }
        else if (type.equals(DateType.DATE)) {
            return TEXT;
        }
        else if (type.equals(VarbinaryType.VARBINARY)) {
            return BLOB;
        }
        else if (type.equals(TimestampType.TIMESTAMP)) {
            return TIMESTAMP;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }
}
