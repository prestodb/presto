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
package com.facebook.presto.spi.type;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.TestingIdType.ID;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.stream.Collectors.toList;

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
        return null;
    }

    @Override
    public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
    {
        return getType(new TypeSignature(baseTypeName, typeParameters));
    }

    @Override
    public Type getParameterizedType(String baseTypeName, List<TypeSignature> typeParameters, List<String> literalParameters)
    {
        if (literalParameters.isEmpty()) {
            return getParameterizedType(
                    baseTypeName,
                    typeParameters.stream().map(TypeSignatureParameter::of).collect(toList()));
        }
        return getType(new TypeSignature(baseTypeName, typeParameters, literalParameters));
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.<Type>of(BOOLEAN, BIGINT, DOUBLE, VARCHAR, VARBINARY, TIMESTAMP, DATE, ID, HYPER_LOG_LOG);
    }

    @Override
    public Optional<Type> getCommonSuperType(List<? extends Type> types)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
    {
        throw new UnsupportedOperationException();
    }
}
