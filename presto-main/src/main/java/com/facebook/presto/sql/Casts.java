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
package com.facebook.presto.sql;

import com.facebook.presto.operator.scalar.MathFunctions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.google.common.base.Charsets.US_ASCII;
import static com.google.common.base.Charsets.UTF_8;

public class Casts
{
    private static final Slice TRUE = Slices.copiedBuffer("true", US_ASCII);
    private static final Slice FALSE = Slices.copiedBuffer("false", US_ASCII);

    public static Object toBoolean(Object value)
    {
        try {
            if (value instanceof Boolean) {
                return value;
            }
            else if (value instanceof Long) {
                return ((Long) value).longValue() != 0;
            }
            else if (value instanceof Double) {
                return ((Double) value).doubleValue() != 0;
            }
            else if (value instanceof Slice) {
                String stringValue = ((Slice) value).toString(UTF_8).toLowerCase();
                if (stringValue.equals("t") || stringValue.equals("true") || stringValue.equals("1")) {
                    return true;
                }
                if (stringValue.equals("f") || stringValue.equals("false") || stringValue.equals("0")) {
                    return false;
                }
            }
        }
        catch (RuntimeException ignored) {
        }

        if (value instanceof Slice) {
            value = "'" + ((Slice) value).toString(UTF_8) + "'";
        }
        throw new IllegalArgumentException(String.format("Cannot cast %s to BOOLEAN", value));
    }

    public static Object toLong(Object value)
    {
        try {
            if (value instanceof Long) {
                return value;
            }
            else if (value instanceof Boolean) {
                return (Boolean) value ? 1L : 0L;
            }
            else if (value instanceof Slice) {
                String stringValue = ((Slice) value).toString(UTF_8);
                return Long.parseLong(stringValue);
            }
            else if (value instanceof Double) {
                return (long) MathFunctions.round(((Double) value).doubleValue());
            }
        }
        catch (RuntimeException ignored) {
        }

        if (value instanceof Slice) {
            value = "'" + ((Slice) value).toString(UTF_8) + "'";
        }
        throw new IllegalArgumentException(String.format("Cannot cast %s to BIGINT", value));
    }

    public static Object toSlice(Object value)
    {
        try {
            if (value instanceof Slice) {
                return value;
            }
            else if (value instanceof Long || value instanceof Double) {
                return Slices.copiedBuffer(value.toString(), US_ASCII);
            }
            else if (value instanceof Boolean) {
                return (Boolean) value ? TRUE : FALSE;
            }
        }
        catch (RuntimeException ignored) {
        }

        if (value instanceof Slice) {
            value = "'" + ((Slice) value).toString(UTF_8) + "'";
        }
        throw new IllegalArgumentException(String.format("Cannot cast %s to VARCHAR", value));
    }

    public static Object toDouble(Object value)
    {
        try {
            if (value instanceof Double) {
                return value;
            }
            else if (value instanceof Boolean) {
                return ((Boolean) value) ? 1.0 : 0.0;
            }
            else if (value instanceof Long) {
                return ((Long) value).doubleValue();
            }
            else if (value instanceof Slice) {
                String stringValue = ((Slice) value).toString(UTF_8);
                return Double.parseDouble(stringValue);
            }
        }
        catch (RuntimeException ignored) {
        }

        if (value instanceof Slice) {
            value = "'" + ((Slice) value).toString(UTF_8) + "'";
        }
        throw new IllegalArgumentException(String.format("Cannot cast %s to DOUBLE", value));
    }
}
