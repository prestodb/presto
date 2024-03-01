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
package com.facebook.presto.druid.metadata;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;

import static java.util.Objects.requireNonNull;

public enum DruidColumnType
{
    BIGINT(BigintType.BIGINT, "long"),
    DOUBLE(DoubleType.DOUBLE, "double"),
    FLOAT(RealType.REAL, "float"),
    OTHER(VarcharType.VARCHAR, "other"),
    TIMESTAMP(TimestampType.TIMESTAMP, "timestamp"),
    VARCHAR(VarcharType.VARCHAR, "string");

    private final String ingestType;
    private final Type prestoType;

    DruidColumnType(Type prestoType, String ingestType)
    {
        this.prestoType = requireNonNull(prestoType, "ingestType is null");
        this.ingestType = requireNonNull(ingestType, "ingestType is null");
    }

    public String getIngestType()
    {
        return ingestType;
    }

    public Type getPrestoType()
    {
        return prestoType;
    }

    public static DruidColumnType fromPrestoType(Type type)
    {
        if (type instanceof BigintType) {
            return BIGINT;
        }
        if (type instanceof DoubleType) {
            return DOUBLE;
        }
        if (type instanceof RealType) {
            return FLOAT;
        }
        if (type instanceof TimestampType) {
            return TIMESTAMP;
        }
        if (type instanceof VarcharType) {
            return VARCHAR;
        }
        return OTHER;
    }
}
