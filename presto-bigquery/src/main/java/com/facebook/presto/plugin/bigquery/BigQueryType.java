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
package com.facebook.presto.plugin.bigquery;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.TimeWithTimeZoneType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.google.cloud.bigquery.Field;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.parseInt;
import static java.time.ZoneId.systemDefault;
import static java.util.stream.Collectors.toList;

public enum BigQueryType
{
    BOOLEAN(BooleanType.BOOLEAN),
    BYTES(VarbinaryType.VARBINARY),
    DATE(DateType.DATE),
    DATETIME(TimestampType.TIMESTAMP),
    FLOAT(DoubleType.DOUBLE),
    GEOGRAPHY(VarcharType.VARCHAR),
    INTEGER(BigintType.BIGINT),
    NUMERIC(DecimalType.createDecimalType(BigQueryMetadata.NUMERIC_DATA_TYPE_PRECISION, BigQueryMetadata.NUMERIC_DATA_TYPE_SCALE)),
    RECORD(null),
    STRING(createUnboundedVarcharType()),
    TIME(TimeWithTimeZoneType.TIME_WITH_TIME_ZONE),
    TIMESTAMP(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE);

    private static final int[] NANO_FACTOR = {
            -1, // 0, no need to multiply
            100_000_000, // 1 digit after the dot
            10_000_000, // 2 digits after the dot
            1_000_000, // 3 digits after the dot
            100_000, // 4 digits after the dot
            10_000, // 5 digits after the dot
            1000, // 6 digits after the dot
            100, // 7 digits after the dot
            10, // 8 digits after the dot
            1, // 9 digits after the dot
    };

    private final Type nativeType;

    BigQueryType(Type nativeType)
    {
        this.nativeType = nativeType;
    }

    static RowType.Field toRawTypeField(Map.Entry<String, BigQueryType.Adaptor> entry)
    {
        return toRawTypeField(entry.getKey(), entry.getValue());
    }

    static RowType.Field toRawTypeField(String name, BigQueryType.Adaptor typeAdaptor)
    {
        Type prestoType = typeAdaptor.getPrestoType();
        return RowType.field(name, prestoType);
    }

    static LocalDateTime toLocalDateTime(String datetime)
    {
        int dotPosition = datetime.indexOf('.');
        if (dotPosition == -1) {
            // no sub-second element
            return LocalDateTime.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(datetime));
        }
        LocalDateTime result = LocalDateTime.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(datetime.substring(0, dotPosition)));
        // has sub-second element, so convert to nanosecond
        String nanosStr = datetime.substring(dotPosition + 1);
        int nanoOfSecond = parseInt(nanosStr) * NANO_FACTOR[nanosStr.length()];
        return result.withNano(nanoOfSecond);
    }

    static long toPrestoTimestamp(String datetime)
    {
        return toLocalDateTime(datetime).atZone(systemDefault()).toInstant().toEpochMilli();
    }

    public Type getNativeType(BigQueryType.Adaptor typeAdaptor)
    {
        if (this == RECORD) {
            // create the row
            Map<String, BigQueryType.Adaptor> subTypes = typeAdaptor.getBigQuerySubTypes();
            checkArgument(!subTypes.isEmpty(), "a record or struct must have sub-fields");
            List<RowType.Field> fields = subTypes.entrySet().stream().map(BigQueryType::toRawTypeField).collect(toList());
            return RowType.from(fields);
        }
        else {
            return nativeType;
        }
    }

    interface Adaptor
    {
        BigQueryType getBigQueryType();

        Map<String, BigQueryType.Adaptor> getBigQuerySubTypes();

        Field.Mode getMode();

        default Type getPrestoType()
        {
            Type rawType = getBigQueryType().getNativeType(this);
            return getMode() == Field.Mode.REPEATED ? new ArrayType(rawType) : rawType;
        }
    }
}
