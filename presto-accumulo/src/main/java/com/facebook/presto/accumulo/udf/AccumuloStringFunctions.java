/*
 * Copyright 2016 Bloomberg L.P.
 *
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
package com.facebook.presto.accumulo.udf;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.UUID;

/**
 * Class containing String-based SQL functions for Accumulo connector
 */
public class AccumuloStringFunctions
{
    private AccumuloStringFunctions()
    {}

    @Description("Returns a randomly generated UUID")
    @ScalarFunction(value = "uuid", deterministic = false)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice UUID()
    {
        return Slices.utf8Slice(UUID.randomUUID().toString());
    }
}
