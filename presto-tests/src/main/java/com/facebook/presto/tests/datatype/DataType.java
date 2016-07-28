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
package com.facebook.presto.tests.datatype;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;

import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Optional.empty;
import static java.util.Optional.of;

public class DataType<T>
{
    private String insertType;
    private Type prestoResultType;
    private Function<T, String> toLiteral;

    public static DataType<String> varcharDataType(int size)
    {
        return varcharDataType(size, "");
    }

    public static DataType<String> varcharDataType(int size, String properties)
    {
        return varcharDataType(of(size), properties);
    }

    public static DataType<String> varcharDataType()
    {
        return varcharDataType(empty(), "");
    }

    private static DataType<String> varcharDataType(Optional<Integer> length, String properties)
    {
        String prefix = length.map(size -> "varchar(" + size + ")").orElse("varchar");
        String suffix = properties.isEmpty() ? "" : " " + properties;
        VarcharType varcharType = length.map(VarcharType::createVarcharType).orElse(createUnboundedVarcharType());
        return stringDataType(prefix + suffix, varcharType);
    }

    public static DataType<String> stringDataType(String insertType, Type prestoResultType)
    {
        return dataType(insertType, prestoResultType, DataType::quote);
    }

    private static String quote(String value)
    {
        return "'" + value + "'";
    }

    public static <T> DataType<T> dataType(String insertType, Type prestoResultType, Function<T, String> toLiteral)
    {
        return new DataType<>(insertType, prestoResultType, toLiteral);
    }

    private DataType(String insertType, Type prestoResultType, Function<T, String> toLiteral)
    {
        this.insertType = insertType;
        this.prestoResultType = prestoResultType;
        this.toLiteral = toLiteral;
    }

    public String toLiteral(T inputValue)
    {
        return toLiteral.apply(inputValue);
    }

    public String getInsertType()
    {
        return insertType;
    }

    public Type getPrestoResultType()
    {
        return prestoResultType;
    }
}
