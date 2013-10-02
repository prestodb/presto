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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.serde.Constants.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.BINARY_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.BOOLEAN_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.DOUBLE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.FLOAT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.INT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.LIST_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.MAP_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.SMALLINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.STRUCT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.TIMESTAMP_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.TINYINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

public enum HiveType
{
    BOOLEAN(ColumnType.BOOLEAN),
    BYTE(ColumnType.LONG),
    SHORT(ColumnType.LONG),
    INT(ColumnType.LONG),
    LONG(ColumnType.LONG),
    FLOAT(ColumnType.DOUBLE),
    DOUBLE(ColumnType.DOUBLE),
    STRING(ColumnType.STRING),
    TIMESTAMP(ColumnType.LONG),
    BINARY(ColumnType.STRING),
    LIST(ColumnType.STRING),
    MAP(ColumnType.STRING),
    STRUCT(ColumnType.STRING);

    private final ColumnType nativeType;

    HiveType(ColumnType nativeType)
    {
        this.nativeType = checkNotNull(nativeType, "nativeType is null");
    }

    public ColumnType getNativeType()
    {
        return nativeType;
    }

    public static HiveType getSupportedHiveType(PrimitiveCategory primitiveCategory)
    {
        HiveType hiveType = getHiveType(primitiveCategory);
        checkArgument(hiveType != null, "Unknown primitive Hive category: " + primitiveCategory);
        return hiveType;
    }

    public static HiveType getHiveType(PrimitiveCategory primitiveCategory)
    {
        switch (primitiveCategory) {
            case BOOLEAN:
                return BOOLEAN;
            case BYTE:
                return BYTE;
            case SHORT:
                return SHORT;
            case INT:
                return INT;
            case LONG:
                return LONG;
            case FLOAT:
                return FLOAT;
            case DOUBLE:
                return DOUBLE;
            case STRING:
                return STRING;
            case TIMESTAMP:
                return TIMESTAMP;
            case BINARY:
                return BINARY;
            default:
                return null;
        }
    }

    public static HiveType getSupportedHiveType(String hiveTypeName)
    {
        HiveType hiveType = getHiveType(hiveTypeName);
        checkArgument(hiveType != null, "Unknown Hive type: " + hiveTypeName);
        return hiveType;
    }

    public static HiveType getHiveType(String hiveTypeName)
    {
        switch (hiveTypeName) {
            case BOOLEAN_TYPE_NAME:
                return BOOLEAN;
            case TINYINT_TYPE_NAME:
                return BYTE;
            case SMALLINT_TYPE_NAME:
                return SHORT;
            case INT_TYPE_NAME:
                return INT;
            case BIGINT_TYPE_NAME:
                return LONG;
            case FLOAT_TYPE_NAME:
                return FLOAT;
            case DOUBLE_TYPE_NAME:
                return DOUBLE;
            case STRING_TYPE_NAME:
                return STRING;
            case TIMESTAMP_TYPE_NAME:
                return TIMESTAMP;
            case BINARY_TYPE_NAME:
                return BINARY;
            case LIST_TYPE_NAME:
                return LIST;
            case MAP_TYPE_NAME:
                return MAP;
            case STRUCT_TYPE_NAME:
                return STRUCT;
            default:
                return null;
        }
    }

    public static HiveType getSupportedHiveType(ObjectInspector fieldInspector)
    {
        HiveType hiveType = getHiveType(fieldInspector);
        checkArgument(hiveType != null, "Unknown Hive category: " + fieldInspector.getCategory());
        return hiveType;
    }

    public static HiveType getHiveType(ObjectInspector fieldInspector)
    {
        switch (fieldInspector.getCategory()) {
            case PRIMITIVE:
                return getHiveType(((PrimitiveObjectInspector) fieldInspector).getPrimitiveCategory());
            case MAP:
                return MAP;
            case LIST:
                return LIST;
            case STRUCT:
                return STRUCT;
            default:
                return null;
        }
    }
}
