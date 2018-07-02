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
package com.facebook.presto.jdbc;

import com.facebook.presto.spi.type.TypeSignature;

import java.sql.Types;

abstract class AbstractInfo
{
    protected static final int VARCHAR_MAX = 1024 * 1024 * 1024;
    protected static final int VARBINARY_MAX = 1024 * 1024 * 1024;
    protected static final int TIME_ZONE_MAX = 40; // current longest time zone is 32
    protected static final int TIME_MAX = "HH:mm:ss.SSS".length();
    protected static final int TIME_WITH_TIME_ZONE_MAX = TIME_MAX + TIME_ZONE_MAX;
    protected static final int TIMESTAMP_MAX = "yyyy-MM-dd HH:mm:ss.SSS".length();
    protected static final int TIMESTAMP_WITH_TIME_ZONE_MAX = TIMESTAMP_MAX + TIME_ZONE_MAX;
    protected static final int DATE_MAX = "yyyy-MM-dd".length();

    public enum Nullable
    {
        NO_NULLS, NULLABLE, UNKNOWN
    }

    protected static int getType(TypeSignature type)
    {
        if (type.getBase().equals("array")) {
            return Types.ARRAY;
        }
        switch (type.getBase()) {
            case "boolean":
                return Types.BOOLEAN;
            case "bigint":
                return Types.BIGINT;
            case "integer":
                return Types.INTEGER;
            case "smallint":
                return Types.SMALLINT;
            case "tinyint":
                return Types.TINYINT;
            case "real":
                return Types.REAL;
            case "double":
                return Types.DOUBLE;
            case "varchar":
                return Types.VARCHAR;
            case "char":
                return Types.CHAR;
            case "varbinary":
                return Types.VARBINARY;
            case "time":
                return Types.TIME;
            case "time with time zone":
                return Types.TIME;
            case "timestamp":
                return Types.TIMESTAMP;
            case "timestamp with time zone":
                return Types.TIMESTAMP;
            case "date":
                return Types.DATE;
            case "decimal":
                return Types.DECIMAL;
            case "unknown":
                return Types.NULL;
            default:
                return Types.JAVA_OBJECT;
        }
    }
}
