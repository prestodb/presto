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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;

import java.sql.JDBCType;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;

public class ColumnIndexStatsUtils
{
    private ColumnIndexStatsUtils() {}

    public static JDBCType jdbcType(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return JDBCType.BOOLEAN;
        }
        if (type.equals(BIGINT) || type.equals(TIMESTAMP)) {
            return JDBCType.BIGINT;
        }
        if (type.equals(INTEGER)) {
            return JDBCType.INTEGER;
        }
        if (type.equals(DOUBLE)) {
            return JDBCType.DOUBLE;
        }
        if (type.equals(DATE)) {
            return JDBCType.INTEGER;
        }
        if (type instanceof VarcharType) {
            return JDBCType.VARBINARY;
        }
        return null;
    }
}
