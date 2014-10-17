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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.server.SliceSerializer;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Throwables;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.util.Arrays;

public final class TestingRowConstructor
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get().registerModule(new SimpleModule().addSerializer(Slice.class, new SliceSerializer()));

    private TestingRowConstructor() {}

    @ScalarFunction("test_row")
    @SqlType("row<bigint,bigint>('col0','col1')")
    public static Slice testRowBigintBigint(@Nullable @SqlType(StandardTypes.BIGINT) Long arg1, @Nullable @SqlType(StandardTypes.BIGINT) Long arg2)
    {
        return toStackRepresentation(arg1, arg2);
    }

    @ScalarFunction("test_row")
    @SqlType("row<bigint,double>('col0','col1')")
    public static Slice testRowBigintBigint(@Nullable @SqlType(StandardTypes.BIGINT) Long arg1, @Nullable @SqlType(StandardTypes.DOUBLE) Double arg2)
    {
        return toStackRepresentation(arg1, arg2);
    }

    @ScalarFunction("test_row")
    @SqlType("row<double,double>('col0','col1')")
    public static Slice testRowBigintBigint(@Nullable @SqlType(StandardTypes.DOUBLE) Double arg1, @Nullable @SqlType(StandardTypes.DOUBLE) Double arg2)
    {
        return toStackRepresentation(arg1, arg2);
    }

    @ScalarFunction("test_row")
    @SqlType("row<double,varchar>('col0','col1')")
    public static Slice testRowBigintBigint(@Nullable @SqlType(StandardTypes.DOUBLE) Double arg1, @Nullable @SqlType(StandardTypes.VARCHAR) Slice arg2)
    {
        return toStackRepresentation(arg1, arg2);
    }

    @ScalarFunction("test_row")
    @SqlType("row<boolean,boolean>('col0','col1')")
    public static Slice testRowBigintBigint(@Nullable @SqlType(StandardTypes.BOOLEAN) Boolean arg1, @Nullable @SqlType(StandardTypes.BOOLEAN) Boolean arg2)
    {
        return toStackRepresentation(arg1, arg2);
    }

    @ScalarFunction("test_row")
    @SqlType("row<timestamp>('col0')")
    public static Slice testRowBigintBigint(@Nullable @SqlType(StandardTypes.TIMESTAMP) Long arg1)
    {
        return toStackRepresentation(arg1);
    }

    private static Slice toStackRepresentation(Object... values)
    {
        try {
            return Slices.utf8Slice(OBJECT_MAPPER.writeValueAsString(Arrays.asList(values)));
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }
}
