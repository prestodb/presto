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
package com.facebook.presto.google.sheets;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;

import javax.inject.Inject;

public class SheetsHelper
{
    @Inject
    private SheetsHelper()
    {
    }

    public static Type getType(String type)
    {
        switch (type) {
            case "bigint":
                return BigintType.BIGINT;
            case "integer":
                return IntegerType.INTEGER;
            case "smallint":
                return SmallintType.SMALLINT;
            case "tinyint":
                return TinyintType.TINYINT;
            case "real":
                return RealType.REAL;
            case "double":
                return DoubleType.DOUBLE;
            case "varbinary":
                return VarbinaryType.VARBINARY;
            case "time":
                return TimeType.TIME;
            case "timestamp":
                return TimestampType.TIMESTAMP;
            case "date":
                return DateType.DATE;
            default:
                return VarcharType.VARCHAR;
        }
    }
}
