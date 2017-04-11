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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static java.util.Objects.requireNonNull;

public final class Varchars
{
    private Varchars() {}

    public static boolean isVarcharType(Type type)
    {
        return type instanceof VarcharType;
    }

    public static Slice truncateToLength(Slice slice, Type type)
    {
        requireNonNull(type, "type is null");
        if (!isVarcharType(type)) {
            throw new IllegalArgumentException("type must be the instance of VarcharType");
        }
        return truncateToLength(slice, VarcharType.class.cast(type));
    }

    public static Slice truncateToLength(Slice slice, VarcharType varcharType)
    {
        requireNonNull(varcharType, "varcharType is null");
        return truncateToLength(slice, varcharType.getLength());
    }

    public static Slice truncateToLength(Slice slice, int maxLength)
    {
        requireNonNull(slice, "slice is null");
        if (maxLength < 0) {
            throw new IllegalArgumentException("Max length must be greater or equal than zero");
        }
        if (maxLength == 0) {
            return Slices.EMPTY_SLICE;
        }
        // min codepoint size is 1 byte.
        // if size in bytes is less than the max length
        // we don't need to decode codepoints
        int sizeInBytes = slice.length();
        if (sizeInBytes <= maxLength) {
            return slice;
        }
        int indexEnd = offsetOfCodePoint(slice, maxLength);
        if (indexEnd < 0) {
            return slice;
        }
        return slice.slice(0, indexEnd);
    }
}
