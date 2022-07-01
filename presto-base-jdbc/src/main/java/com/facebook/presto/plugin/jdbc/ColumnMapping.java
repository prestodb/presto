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

import com.facebook.presto.common.type.Type;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ColumnMapping
{
    private final Type type;
    private final ReadFunction readFunction;
    private final WriteFunction writeFunction;

    private ColumnMapping(Type type, ReadFunction readFunction, WriteFunction writeFunction)
    {
        this.type = requireNonNull(type, "type is null");
        this.readFunction = requireNonNull(readFunction, "readFunction is null");
        this.writeFunction = requireNonNull(writeFunction, "writeFunction is null");
        checkArgument(
                type.getJavaType() == readFunction.getJavaType(),
                "Presto type %s is not compatible with read function %s returning %s",
                type,
                readFunction,
                readFunction.getJavaType());
        checkArgument(
                type.getJavaType() == writeFunction.getJavaType(),
                "Presto type %s is not compatible with write function %s accepting %s",
                type,
                writeFunction,
                writeFunction.getJavaType());
    }

    public static ColumnMapping booleanMapping(Type prestoType, BooleanReadFunction readFunction, BooleanWriteFunction writeFunction)
    {
        return new ColumnMapping(prestoType, readFunction, writeFunction);
    }

    public static ColumnMapping longMapping(Type prestoType, LongReadFunction readFunction, LongWriteFunction writeFunction)
    {
        return new ColumnMapping(prestoType, readFunction, writeFunction);
    }

    public static ColumnMapping doubleMapping(Type prestoType, DoubleReadFunction readFunction, DoubleWriteFunction writeFunction)
    {
        return new ColumnMapping(prestoType, readFunction, writeFunction);
    }

    public static ColumnMapping sliceMapping(Type prestoType, SliceReadFunction readFunction, SliceWriteFunction writeFunction)
    {
        return new ColumnMapping(prestoType, readFunction, writeFunction);
    }

    public static ColumnMapping objectMapping(Type prestoType, ObjectReadFunction readFunction, ObjectWriteFunction writeFunction)
    {
        return new ColumnMapping(prestoType, readFunction, writeFunction);
    }

    public Type getType()
    {
        return type;
    }

    public ReadFunction getReadFunction()
    {
        return readFunction;
    }

    public WriteFunction getWriteFunction()
    {
        return writeFunction;
    }
}
