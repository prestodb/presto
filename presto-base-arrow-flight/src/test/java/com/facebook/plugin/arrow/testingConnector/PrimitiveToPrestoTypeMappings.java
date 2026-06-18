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
package com.facebook.plugin.arrow.testingConnector;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

public final class PrimitiveToPrestoTypeMappings
{
    private PrimitiveToPrestoTypeMappings()
    {
        throw new UnsupportedOperationException();
    }

    public static Type fromPrimitiveToPrestoType(String dataType)
    {
        switch (dataType) {
            case "INTEGER":
                return IntegerType.INTEGER;
            case "VARCHAR":
                return createUnboundedVarcharType();
            case "DOUBLE":
                return DoubleType.DOUBLE;
            case "SMALLINT":
                return SmallintType.SMALLINT;
            case "BOOLEAN":
                return BooleanType.BOOLEAN;
            case "TIMESTAMP":
                return TimestampType.TIMESTAMP;
            case "TIME":
                return TimeType.TIME;
            case "REAL":
                return RealType.REAL;
            case "DATE":
                return DateType.DATE;
            case "BIGINT":
                return BigintType.BIGINT;
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported datatype '" + dataType + "' in the selected table.");
    }
}
