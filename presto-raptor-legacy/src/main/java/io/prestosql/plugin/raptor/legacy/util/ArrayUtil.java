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
package io.prestosql.plugin.raptor.legacy.util;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

public final class ArrayUtil
{
    private ArrayUtil() {}

    /**
     * Unpack an array of big endian ints.
     */
    public static List<Integer> intArrayFromBytes(byte[] bytes)
    {
        ImmutableList.Builder<Integer> list = ImmutableList.builder();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
            list.add(buffer.getInt());
        }
        return list.build();
    }

    /**
     * Pack an array of ints as big endian.
     */
    public static byte[] intArrayToBytes(Collection<Integer> values)
    {
        ByteBuffer buffer = ByteBuffer.allocate(values.size() * Integer.BYTES);
        for (int value : values) {
            buffer.putInt(value);
        }
        return buffer.array();
    }
}
