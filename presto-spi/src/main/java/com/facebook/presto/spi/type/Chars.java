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

import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static java.util.Objects.requireNonNull;

public final class Chars
{
    private Chars() {}

    public static boolean isCharType(Type type)
    {
        return type instanceof CharType;
    }

    public static Slice trimSpacesAndTruncateToLength(Slice slice, Type type)
    {
        requireNonNull(type, "type is null");
        if (!isCharType(type)) {
            throw new IllegalArgumentException("type must be the instance of CharType");
        }
        return trimSpacesAndTruncateToLength(slice, CharType.class.cast(type));
    }

    public static Slice trimSpacesAndTruncateToLength(Slice slice, CharType charType)
    {
        requireNonNull(charType, "charType is null");
        return trimSpacesAndTruncateToLength(slice, charType.getLength());
    }

    public static Slice trimSpacesAndTruncateToLength(Slice slice, int maxLength)
    {
        requireNonNull(slice, "slice is null");
        if (maxLength < 0) {
            throw new IllegalArgumentException("Max length must be greater or equal than zero");
        }
        return truncateToLength(trimSpaces(slice), maxLength);
    }

    public static Slice trimSpaces(Slice slice)
    {
        requireNonNull(slice, "slice is null");
        return slice.slice(0, sliceLengthWithoutTrailingSpaces(slice));
    }

    private static int sliceLengthWithoutTrailingSpaces(Slice slice)
    {
        for (int i = slice.length(); i > 0; --i) {
            if (slice.getByte(i - 1) != ' ') {
                return i;
            }
        }
        return 0;
    }
}
