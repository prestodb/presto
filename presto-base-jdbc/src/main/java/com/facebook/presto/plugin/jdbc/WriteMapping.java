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
package com.facebook.presto.plugin.jdbc;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class WriteMapping
{
    private final String dataType;
    private final WriteFunction writeFunction;

    private WriteMapping(String dataType, WriteFunction writeFunction)
    {
        this.dataType = requireNonNull(dataType, "data type is null");
        this.writeFunction = requireNonNull(writeFunction, "writeFunction is null");
    }

    public static WriteMapping booleanMapping(String dataType, BooleanWriteFunction writeFunction)
    {
        return new WriteMapping(dataType, writeFunction);
    }

    public static WriteMapping longMapping(String dataType, LongWriteFunction writeFunction)
    {
        return new WriteMapping(dataType, writeFunction);
    }

    public static WriteMapping doubleMapping(String dataType, DoubleWriteFunction writeFunction)
    {
        return new WriteMapping(dataType, writeFunction);
    }

    public static WriteMapping sliceMapping(String dataType, SliceWriteFunction writeFunction)
    {
        return new WriteMapping(dataType, writeFunction);
    }

    public static <T> WriteMapping objectMapping(String dataType, Class<T> javaType, ObjectWriteFunction.ObjectWriteFunctionImplementation<T> writeFunctionImplementation)
    {
        return objectMapping(dataType, ObjectWriteFunction.of(javaType, writeFunctionImplementation));
    }

    public static WriteMapping objectMapping(String dataType, ObjectWriteFunction writeFunction)
    {
        return new WriteMapping(dataType, writeFunction);
    }

    public String getDataType()
    {
        return dataType;
    }

    public WriteFunction getWriteFunction()
    {
        return writeFunction;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("dataType", dataType)
                .toString();
    }
}
