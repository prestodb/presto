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
package com.facebook.presto.common.type;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.DEFAULT_PRECISION;
import static com.facebook.presto.common.type.DecimalType.DEFAULT_SCALE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.DECIMAL;
import static com.facebook.presto.common.type.TestingIdType.ID;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;

public class TestingTypeManager
        implements TypeManager
{
    @Override
    public Type getType(TypeSignature signature)
    {
        for (Type type : getTypes()) {
            if (signature.getBase().equals(type.getTypeSignature().getBase())) {
                return type;
            }
        }
        if (signature.getBase().equals(DECIMAL)) {
            int precision;
            int scale;
            try {
                precision = signature.getParameters().get(0).getLongLiteral().intValue();
                scale = signature.getParameters().get(1).getLongLiteral().intValue();
            }
            catch (Exception e) {
                precision = DEFAULT_PRECISION;
                scale = DEFAULT_SCALE;
            }
            return createDecimalType(precision, scale);
        }

        return null;
    }

    @Override
    public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
    {
        return getType(new TypeSignature(baseTypeName, typeParameters));
    }

    @Override
    public boolean canCoerce(Type actualType, Type expectedType)
    {
        throw new UnsupportedOperationException();
    }

    private List<Type> getTypes()
    {
        return ImmutableList.of(BOOLEAN, INTEGER, BIGINT, DOUBLE, VARCHAR, VARBINARY, TIMESTAMP, DATE, ID, HYPER_LOG_LOG, TIME);
    }
}
