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

    public static int valueHashCode(Type type, Slice slice, int offset)
    {
        boolean isNull = slice.getByte(offset) != 0;
        if (isNull) {
            return 0;
        }

        return type.getTypeInfo().hashCode(slice, offset + SIZE_OF_BYTE);
    }
}
