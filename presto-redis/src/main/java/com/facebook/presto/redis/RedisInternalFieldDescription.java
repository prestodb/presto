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
package com.facebook.presto.redis;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnMetadata;

import java.util.Map;

import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * Describes an internal (managed by the connector) field which is added to each table row. The definition itself makes the row
 * show up in the tables (the columns are hidden by default, so they must be explicitly selected) but unless the field is hooked in using the
 * forBooleanValue/forLongValue/forBytesValue methods and the resulting FieldValueProvider is then passed into the appropriate row decoder, the fields
 * will be null. Most values are assigned in the {@link RedisRecordSet}.
 */
public enum RedisInternalFieldDescription
{
    /**
     * <tt>_key</tt> - Represents the key as a text column.
     */
    KEY_FIELD("_key", createUnboundedVarcharType(), "Key text"),

    /**
     * <tt>_value</tt> - Represents the value as a text column. Format is UTF-8
     */
    VALUE_FIELD("_value", createUnboundedVarcharType(), "Value text"),

    /**
     * <tt>_value_corrupt</tt> - True if the row converter could not read the value. May be null if the row converter does not set a value (e.g. the dummy row converter does not).
     */
    VALUE_CORRUPT_FIELD("_value_corrupt", BooleanType.BOOLEAN, "Value data is corrupt"),

    /**
     * <tt>_value_length</tt> - length in bytes of the value.
     */
    VALUE_LENGTH_FIELD("_value_length", BigintType.BIGINT, "Total number of value bytes"),

    /**
     * <tt>_key_corrupt</tt> - True if the row converter could not read the key. May be null if the row converter does not set a value (e.g. the dummy row converter does not).
     */
    KEY_CORRUPT_FIELD("_key_corrupt", BooleanType.BOOLEAN, "Key data is corrupt"),

    /**
     * <tt>_key_length</tt> - length in bytes of the key.
     */
    KEY_LENGTH_FIELD("_key_length", BigintType.BIGINT, "Total number of key bytes");

    private static final Map<String, RedisInternalFieldDescription> BY_COLUMN_NAME =
            stream(RedisInternalFieldDescription.values())
                    .collect(toImmutableMap(RedisInternalFieldDescription::getColumnName, identity()));

    public static RedisInternalFieldDescription forColumnName(String columnName)
    {
        RedisInternalFieldDescription description = BY_COLUMN_NAME.get(columnName);
        checkArgument(description != null, "Unknown internal column name %s", columnName);
        return description;
    }

    private final String columnName;
    private final Type type;
    private final String comment;

    RedisInternalFieldDescription(
            String columnName,
            Type type,
            String comment)
    {
        checkArgument(!isNullOrEmpty(columnName), "name is null or is empty");
        this.columnName = columnName;
        this.type = requireNonNull(type, "type is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    public String getColumnName()
    {
        return columnName;
    }

    public Type getType()
    {
        return type;
    }

    RedisColumnHandle getColumnHandle(String connectorId, int index, boolean hidden)
    {
        return new RedisColumnHandle(connectorId,
                index,
                getColumnName(),
                getType(),
                null,
                null,
                null,
                false,
                hidden,
                true);
    }

    ColumnMetadata getColumnMetadata(boolean hidden)
    {
        return new ColumnMetadata(columnName, type, comment, hidden);
    }
}
