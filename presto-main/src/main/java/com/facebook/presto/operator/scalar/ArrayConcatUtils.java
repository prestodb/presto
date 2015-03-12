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

import com.facebook.presto.spi.block.StructBuilder;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

public final class ArrayConcatUtils
{
    private ArrayConcatUtils() {}

    public static Slice concat(Type elementType, Slice left, Slice right)
    {
        return StructBuilder.arrayBuilder(elementType)
                        .addAll(StructBuilder.readBlock(left.getInput()))
                        .addAll(StructBuilder.readBlock(right.getInput()))
                        .build();
    }

    private static Slice concatElement(Type elementType, Slice in, Object value, boolean append)
    {
        StructBuilder builder = StructBuilder.arrayBuilder(elementType);

        if (!append) {
            builder.add(value);
        }

        builder.addAll(StructBuilder.readBlock(in.getInput()));

        if (append) {
            builder.add(value);
        }

        return builder.build();
    }

    public static Slice appendElement(Type elementType, Slice in, Object value)
    {
        return concatElement(elementType, in, value, true);
    }

    public static Slice appendElement(Type elementType, Slice in, long value)
    {
        return appendElement(elementType, in, Long.valueOf(value));
    }

    public static Slice appendElement(Type elementType, Slice in, boolean value)
    {
        return appendElement(elementType, in, Boolean.valueOf(value));
    }

    public static Slice appendElement(Type elementType, Slice in, double value)
    {
        return appendElement(elementType, in, Double.valueOf(value));
    }

    public static Slice appendElement(Type elementType, Slice in, Slice value)
    {
        return concatElement(elementType, in, value, true);
    }

    public static Slice prependElement(Type elementType, Slice value, Slice in)
    {
        return concatElement(elementType, in, value, false);
    }

    public static Slice prependElement(Type elementType, Object value, Slice in)
    {
        return concatElement(elementType, in, value, false);
    }

    public static Slice prependElement(Type elementType, long value, Slice in)
    {
        return prependElement(elementType, Long.valueOf(value), in);
    }

    public static Slice prependElement(Type elementType, boolean value, Slice in)
    {
        return prependElement(elementType, Boolean.valueOf(value), in);
    }

    public static Slice prependElement(Type elementType, double value, Slice in)
    {
        return prependElement(elementType, Double.valueOf(value), in);
    }
}
