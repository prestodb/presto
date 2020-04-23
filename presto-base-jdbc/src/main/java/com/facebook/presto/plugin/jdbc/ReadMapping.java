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

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class ReadMapping
{
    public static ReadMapping booleanReadMapping(Type prestoType, BooleanReadFunction readFunction)
    {
        return new ReadMapping(prestoType, readFunction);
    }

    public static ReadMapping longReadMapping(Type prestoType, LongReadFunction readFunction)
    {
        return new ReadMapping(prestoType, readFunction);
    }

    public static ReadMapping doubleReadMapping(Type prestoType, DoubleReadFunction readFunction)
    {
        return new ReadMapping(prestoType, readFunction);
    }

    public static ReadMapping sliceReadMapping(Type prestoType, SliceReadFunction readFunction)
    {
        return new ReadMapping(prestoType, readFunction);
    }

    private final Type type;
    private final ReadFunction readFunction;

    private ReadMapping(Type type, ReadFunction readFunction)
    {
        this.type = requireNonNull(type, "type is null");
        this.readFunction = requireNonNull(readFunction, "readFunction is null");
        checkArgument(
                type.getJavaType() == readFunction.getJavaType(),
                "Presto type %s is not compatible with read function %s returning %s",
                type,
                readFunction,
                readFunction.getJavaType());
    }

    public Type getType()
    {
        return type;
    }

    public ReadFunction getReadFunction()
    {
        return readFunction;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .toString();
    }
}
