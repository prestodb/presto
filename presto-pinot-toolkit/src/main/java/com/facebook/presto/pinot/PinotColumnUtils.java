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

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.common.data.Schema;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class PinotColumnUtils
{
    private PinotColumnUtils()
    {
    }

    public static List<PinotColumn> getPinotColumnsForPinotSchema(Schema pinotTableSchema)
    {
        return pinotTableSchema.getColumnNames().stream()
                .filter(columnName -> !columnName.startsWith("$")) // Hidden columns starts with "$", ignore them as we can't use them in PQL
                .map(columnName -> new PinotColumn(columnName, getPrestoTypeFromPinotType(pinotTableSchema.getFieldSpecFor(columnName))))
                .collect(toImmutableList());
    }

    public static Type getPrestoTypeFromPinotType(FieldSpec field)
    {
        if (field.isSingleValueField()) {
            return getPrestoTypeFromPinotType(field.getDataType());
        }
        return VarcharType.VARCHAR;
    }

    public static Type getPrestoTypeFromPinotType(DataType dataType)
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
            default:
                break;
        }
        throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "Not support type conversion for pinot data type: " + dataType);
    }
}
