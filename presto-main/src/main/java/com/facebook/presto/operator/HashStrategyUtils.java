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
package com.facebook.presto.operator;

import com.facebook.presto.tuple.TupleInfo.Type;
import io.airlift.slice.Slice;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;

public final class HashStrategyUtils
{
    private HashStrategyUtils()
    {
    }

    public static int addToHashCode(int result, int hashCode)
    {
        result = 31 * result + hashCode;
        return result;
    }

    public static boolean valueEquals(Type type, Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        // check if null flags are the same
        boolean leftIsNull = leftSlice.getByte(leftOffset) != 0;
        boolean rightIsNull = rightSlice.getByte(rightOffset) != 0;
        if (leftIsNull != rightIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (leftIsNull) {
            return true;
        }

        return type.getTypeInfo().equals(leftSlice, leftOffset + SIZE_OF_BYTE, rightSlice, rightOffset + SIZE_OF_BYTE);
    }

    public static int valueHashCode(Type type, Slice slice, int offset)
    {
        boolean isNull = slice.getByte(offset) != 0;
        if (isNull) {
            return 0;
        }

        return type.getTypeInfo().hashCode(slice, offset + SIZE_OF_BYTE);
    }
}
