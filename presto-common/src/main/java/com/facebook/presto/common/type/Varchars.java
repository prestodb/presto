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
package com.facebook.presto.common.type;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static java.lang.Math.min;
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

        return slice.slice(0, byteCount(slice, 0, slice.length(), maxLength));
    }

    /**
     * Get the byte count of a given {@param slice} with in range {@param offset} to {@param offset} + {@param length}
     * for at most {@param codePointCount} many code points
     */
    public static int byteCount(Slice slice, int offset, int length, int codePointCount)
    {
        requireNonNull(slice, "slice is null");
        if (length < 0) {
            throw new IllegalArgumentException("length must be greater than or equal to zero");
        }
        if (offset < 0 || offset + length > slice.length()) {
            throw new IllegalArgumentException("invalid offset/length");
        }
        if (codePointCount < 0) {
            throw new IllegalArgumentException("codePointsCount must be greater than or equal to zero");
        }
        if (codePointCount == 0) {
            return 0;
        }

        // min codepoint size is 1 byte.
        // if size in bytes is less than the max length
        // we don't need to decode codepoints
        if (codePointCount > length) {
            return length;
        }

        // get the end index with respect to code point count
        int endIndex = offsetOfCodePoint(slice, offset, codePointCount);
        if (endIndex < 0) {
            // end index runs over the slice's length
            return length;
        }
        if (offset > endIndex) {
            throw new AssertionError("offset cannot be smaller than or equal to endIndex");
        }

        // end index could run over length because of large code points (e.g., 4-byte code points)
        // or within length because of small code points (e.g., 1-byte code points)
        return min(endIndex - offset, length);
    }
}
