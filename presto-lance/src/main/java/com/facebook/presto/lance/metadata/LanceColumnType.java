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
package com.facebook.presto.lance.metadata;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import static java.util.Objects.requireNonNull;

public enum LanceColumnType
{
    BIGINT(BigintType.BIGINT, new ArrowType.Int(64, true)),
    INTEGER(IntegerType.INTEGER, new ArrowType.Int(32, true)),
    DOUBLE(DoubleType.DOUBLE, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
    FLOAT(RealType.REAL, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
    VARCHAR(VarcharType.VARCHAR, new ArrowType.Utf8()),
    TIMESTAMP(TimestampType.TIMESTAMP, new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
    BOOLEAN(BooleanType.BOOLEAN, new ArrowType.Bool()),
    OTHER(VarcharType.VARCHAR, new ArrowType.Utf8());

    private final ArrowType arrowType;
    private final Type prestoType;

    LanceColumnType(Type prestoType, ArrowType arrowType)
    {
        this.prestoType = requireNonNull(prestoType, "prestoType is null");
        this.arrowType = requireNonNull(arrowType, "arrowType is null");
    }

    public ArrowType getArrowType()
    {
        return arrowType;
    }

    public Type getPrestoType()
    {
        return prestoType;
    }

    public static LanceColumnType fromPrestoType(Type type)
    {
        if (type instanceof BigintType) {
            return BIGINT;
        }
        if (type instanceof IntegerType) {
            return INTEGER;
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

    public static LanceColumnType fromArrowType(ArrowType type)
    {
        if (type instanceof ArrowType.Bool) {
            return BOOLEAN;
        }
        else if (type instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) type;
            if (intType.getBitWidth() == 32) {
                return INTEGER;
            }
            else if (intType.getBitWidth() == 64) {
                return BIGINT;
            }
        }
        else if (type instanceof ArrowType.FloatingPoint) {
            return DOUBLE;
        }
        else if (type instanceof ArrowType.Utf8) {
            return VARCHAR;
        }
        return OTHER;
    }
}
