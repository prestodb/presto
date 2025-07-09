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
package com.facebook.presto.plugin.jdbc.mapping;

import com.facebook.presto.plugin.jdbc.mapping.functions.BooleanWriteFunction;
import com.facebook.presto.plugin.jdbc.mapping.functions.DoubleWriteFunction;
import com.facebook.presto.plugin.jdbc.mapping.functions.LongWriteFunction;
import com.facebook.presto.plugin.jdbc.mapping.functions.ObjectWriteFunction;
import com.facebook.presto.plugin.jdbc.mapping.functions.SliceWriteFunction;

import static java.util.Objects.requireNonNull;

/*
 * JDBC based connectors can control to define how data should be written back to data source by WriteFunctions.
 */
public final class WriteMapping
{
    private final WriteFunction writeFunction;

    private WriteMapping(WriteFunction writeFunction)
    {
        this.writeFunction = requireNonNull(writeFunction, "writeFunction is null");
    }

    public static WriteMapping createBooleanWriteMapping(BooleanWriteFunction writeFunction)
    {
        return new WriteMapping(writeFunction);
    }

    public static WriteMapping createLongWriteMapping(LongWriteFunction writeFunction)
    {
        return new WriteMapping(writeFunction);
    }

    public static WriteMapping createDoubleWriteMapping(DoubleWriteFunction writeFunction)
    {
        return new WriteMapping(writeFunction);
    }

    public static WriteMapping createSliceWriteMapping(SliceWriteFunction writeFunction)
    {
        return new WriteMapping(writeFunction);
    }

    public static <T> WriteMapping createObjectWriteMapping(Class<T> javaType, ObjectWriteFunction.ObjectWriteFunctionImplementation<T> writeFunctionImplementation)
    {
        return createObjectWriteMapping(ObjectWriteFunction.of(javaType, writeFunctionImplementation));
    }

    public static WriteMapping createObjectWriteMapping(ObjectWriteFunction writeFunction)
    {
        return new WriteMapping(writeFunction);
    }

    public WriteFunction getWriteFunction()
    {
        return writeFunction;
    }
}
