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
package com.facebook.presto.pinot;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.pinot.spi.data.DateTimeFieldSpec.TimeFormat.EPOCH;

public class PinotColumnUtils
{
    private static final String DAYS_SINCE_EPOCH_TIME_FORMAT = "1:DAYS:EPOCH";
    private static final String MILLISECONDS_SINCE_EPOCH_TIME_FORMAT = "1:MILLISECONDS:EPOCH";

    private PinotColumnUtils()
    {
    }

    public static List<PinotColumn> getPinotColumnsForPinotSchema(Schema pinotTableSchema, boolean inferDateType, boolean inferTimestampType)
    {
        return pinotTableSchema.getColumnNames().stream()
                .filter(columnName -> !columnName.startsWith("$")) // Hidden columns starts with "$", ignore them as we can't use them in PQL
                .map(columnName -> new PinotColumn(
                        columnName,
                        getPrestoTypeFromPinotType(pinotTableSchema.getFieldSpecFor(columnName), inferDateType, inferTimestampType),
                        isNullableColumnFromPinotType(pinotTableSchema.getFieldSpecFor(columnName)),
                        getCommentFromPinotType(pinotTableSchema.getFieldSpecFor(columnName))))
                .collect(toImmutableList());
    }

    private static String getCommentFromPinotType(FieldSpec field)
    {
        return field.getFieldType().name();
    }

    private static boolean isNullableColumnFromPinotType(FieldSpec field)
    {
        return false;
    }

    public static Type getPrestoTypeFromPinotType(FieldSpec field, boolean inferDateType, boolean inferTimestampType)
    {
        if (field.isSingleValueField()) {
            switch (field.getFieldType()) {
                case TIME:
                    TimeFieldSpec timeFieldSpec = (TimeFieldSpec) field;
                    TimeGranularitySpec outSpec = timeFieldSpec.getOutgoingGranularitySpec();
                    if (outSpec != null) {
                        if (outSpec.getTimeFormat().equalsIgnoreCase(EPOCH.name())) {
                            if (inferDateType && (DAYS == outSpec.getTimeType()) && (outSpec.getTimeUnitSize() == 1)) {
                                return DateType.DATE;
                            }
                            if (inferTimestampType && (MILLISECONDS == outSpec.getTimeType()) && (outSpec.getTimeUnitSize() == 1)) {
                                return TimestampType.TIMESTAMP;
                            }
                        }
                    }
                    else {
                        TimeGranularitySpec inSpec = timeFieldSpec.getIncomingGranularitySpec();
                        if (inferDateType && (DAYS == inSpec.getTimeType()) && (inSpec.getTimeUnitSize() == 1)) {
                            return DateType.DATE;
                        }
                        if (inferTimestampType && (MILLISECONDS == inSpec.getTimeType()) && (inSpec.getTimeUnitSize() == 1)) {
                            return TimestampType.TIMESTAMP;
                        }
                    }
                    return getPrestoTypeFromPinotType(field.getDataType());
                case DATE_TIME:
                    DateTimeFieldSpec dateTimeFieldSpec = (DateTimeFieldSpec) field;
                    if (inferDateType && dateTimeFieldSpec.getFormat().equalsIgnoreCase(DAYS_SINCE_EPOCH_TIME_FORMAT)) {
                        return DateType.DATE;
                    }
                    if (inferTimestampType && dateTimeFieldSpec.getFormat().equalsIgnoreCase(MILLISECONDS_SINCE_EPOCH_TIME_FORMAT)) {
                        return TimestampType.TIMESTAMP;
                    }
            }
            return getPrestoTypeFromPinotType(field.getDataType());
        }

        return new ArrayType(getPrestoTypeFromPinotType(field.getDataType()));
    }

    private static Type getPrestoTypeFromPinotType(DataType dataType)
    {
        switch (dataType) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case DOUBLE:
            case FLOAT:
                return DoubleType.DOUBLE;
            case INT:
                return IntegerType.INTEGER;
            case LONG:
                return BigintType.BIGINT;
            case STRING:
                return VarcharType.VARCHAR;
            case BYTES:
                return VarbinaryType.VARBINARY;
            default:
                break;
        }
        throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "Not support type conversion for pinot data type: " + dataType);
    }
}
