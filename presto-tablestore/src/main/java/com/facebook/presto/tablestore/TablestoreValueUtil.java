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
package com.facebook.presto.tablestore;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import io.airlift.slice.Slice;

import javax.validation.constraints.NotNull;

import java.util.Arrays;

import static com.alicloud.openservices.tablestore.model.PrimaryKeyValue.INF_MAX;
import static com.alicloud.openservices.tablestore.model.PrimaryKeyValue.INF_MIN;
import static com.alicloud.openservices.tablestore.model.PrimaryKeyValue.fromLong;
import static java.lang.Long.MAX_VALUE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class TablestoreValueUtil
{
    private static final Logger log = Logger.get(TablestoreValueUtil.class);

    private TablestoreValueUtil()
    {
    }

    public static ColumnValue toColumnValue(Marker marker)
    {
        return convert(marker.getType(), false, marker.getValue());
    }

    @SuppressWarnings("unchecked")
    public static <T> T convert(Type type, boolean toPrimaryKeyNotColumnValue, @NotNull Object value)
    {
        Class<T>[] acceptedTypes = null;
        try {
            if (type instanceof VarcharType || type instanceof CharType) {
                String x = null;
                if (value instanceof String) {
                    x = (String) value;
                }
                else if (value instanceof Slice) {
                    x = ((Slice) value).toStringUtf8();
                }
                else if (value instanceof byte[]) {
                    x = new String((byte[]) value, UTF_8);
                }
                else if (value instanceof Number || value instanceof Boolean || value instanceof Character) {
                    x = value.toString();
                }
                if (x != null) {
                    if (toPrimaryKeyNotColumnValue) {
                        return (T) PrimaryKeyValue.fromString(x);
                    }
                    else {
                        return (T) ColumnValue.fromString(x);
                    }
                }
                acceptedTypes = new Class[] {String.class, Slice.class, byte[].class, Number.class, Boolean.class, Character.class};
            }
            else if (type instanceof BigintType || type instanceof IntegerType
                    || type instanceof SmallintType || type instanceof TinyintType) {
                Long x = null;
                if (value instanceof Integer || value instanceof Long || value instanceof Short || value instanceof Byte) {
                    x = ((Number) value).longValue();
                }
                else if (value instanceof String) {
                    x = Long.parseLong((String) value);
                }
                else if (value instanceof Character) {
                    x = (long) (Character) value;
                }
                if (x != null) {
                    if (toPrimaryKeyNotColumnValue) {
                        return (T) PrimaryKeyValue.fromLong(x);
                    }
                    else {
                        return (T) ColumnValue.fromLong(x);
                    }
                }
                acceptedTypes = new Class[] {String.class, Integer.class, Long.class, Short.class, Byte.class, Character.class};
            }
            else if (type instanceof BooleanType) {
                Boolean x = null;
                if (value instanceof Boolean) {
                    x = (Boolean) value;
                }
                else if (value instanceof String) {
                    x = Boolean.parseBoolean((String) value);
                }
                else if (value instanceof Number) {
                    x = ((Number) value).longValue() != 0;
                }
                if (x != null && !toPrimaryKeyNotColumnValue) {
                    return (T) ColumnValue.fromBoolean(x);
                }
                acceptedTypes = new Class[] {String.class, Number.class, Boolean.class};
            }
            else if (type instanceof DoubleType || type instanceof RealType) {
                Double x = null;
                if (value instanceof Double) {
                    x = (Double) value;
                }
                else if (value instanceof String) {
                    x = Double.parseDouble((String) value);
                }
                else if (value instanceof Number) {
                    x = ((Number) value).doubleValue();
                }
                if (x != null && !toPrimaryKeyNotColumnValue) {
                    return (T) ColumnValue.fromDouble(x);
                }
                acceptedTypes = new Class[] {String.class, Number.class, Double.class};
            }
            else if (type instanceof VarbinaryType) {
                byte[] x = null;
                if (value instanceof byte[]) {
                    x = (byte[]) value;
                }
                else if (value instanceof String) {
                    x = ((String) value).getBytes(UTF_8);
                }
                else if (value instanceof Slice) {
                    x = ((Slice) value).getBytes();
                }
                if (x != null) {
                    if (toPrimaryKeyNotColumnValue) {
                        return (T) PrimaryKeyValue.fromBinary(x);
                    }
                    else {
                        return (T) ColumnValue.fromBinary(x);
                    }
                }
                acceptedTypes = new Class[] {String.class, byte[].class, Slice.class};
            }
        }
        catch (Exception e) {
            String x = String.valueOf(value);
            String messageTemplate = "convert() failed for %s, required type is %s, and type of value is %s, and value=%s";
            String fullMessage = format(messageTemplate, (toPrimaryKeyNotColumnValue ? "PrimaryKeyValue" : "ColumnValue"), type, value.getClass(), x);
            log.error(fullMessage, e);

            String shortMessage = fullMessage;
            if (x.length() > 20) {
                shortMessage = format(messageTemplate, (toPrimaryKeyNotColumnValue ? "PrimaryKeyValue" : "ColumnValue"), type, value.getClass(), x.substring(0, 20) + "...");
            }
            throw new IllegalArgumentException(shortMessage, e);
        }

        String str = "Unsupported conversion for " + (toPrimaryKeyNotColumnValue ? "PrimaryKeyValue" : "ColumnValue") + ", ";

        if (acceptedTypes == null) {
            str += "the required tablestore type isn't supported not:" + type;
        }
        else {
            str += "the required tablestore type is " + type + ", but ";
            if (value == null) {
                str += "the value is NULL";
            }
            else {
                str += "the type of value is " + value.getClass();
            }
            str += " and accepted java types is " + Arrays.toString(acceptedTypes);
        }
        throw new UnsupportedOperationException(str);
    }

    public static PrimaryKeyValue toPrimaryKeyValue(Marker marker)
    {
        return convert(marker.getType(), true, marker.getValue());
    }

    public static PrimaryKeyValue addOneForPrimaryKeyValue(PrimaryKeyValue target)
    {
        if (target == INF_MAX || target == INF_MIN) {
            return target;
        }
        switch (target.getType()) {
            case INTEGER:
                long longValue = target.asLong();
                if (longValue == MAX_VALUE) {
                    return INF_MAX;
                }
                return fromLong(longValue + 1);
            case STRING:
                String stringValue = target.asString();
                return PrimaryKeyValue.fromString(stringValue + "\0");
            case BINARY:
                byte[] binaryValue = target.asBinary();
                byte[] newBinaryValue = new byte[binaryValue.length + 1];
                System.arraycopy(binaryValue, 0, newBinaryValue, 0, binaryValue.length);
                newBinaryValue[newBinaryValue.length - 1] = 0;
                return PrimaryKeyValue.fromBinary(newBinaryValue);
            default:
                throw new IllegalArgumentException("Unknown type: " + target.getType());
        }
    }
}
