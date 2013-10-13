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
package com.facebook.presto.serde;

import com.facebook.presto.type.Type;
import com.facebook.presto.type.Types;
import com.google.common.base.Charsets;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkNotNull;

public final class TypeSerde
{
    private TypeSerde()
    {
    }

    public static void writeInfo(SliceOutput sliceOutput, Type type)
    {
        checkNotNull(type, "type is null");
        checkNotNull(sliceOutput, "sliceOutput is null");

        writeLengthPrefixedString(sliceOutput, type.getName());
    }

    public static Type readType(SliceInput sliceInput)
    {
        checkNotNull(sliceInput, "sliceInput is null");

        String name = readLengthPrefixedString(sliceInput);
        return Types.fromName(name);
    }

    private static String readLengthPrefixedString(SliceInput input)
    {
        int length = input.readInt();
        byte[] bytes = new byte[length];
        input.readBytes(bytes);
        return new String(bytes, Charsets.UTF_8);
    }

    private static void writeLengthPrefixedString(SliceOutput output, String string)
    {
        byte[] bytes = string.getBytes(Charsets.UTF_8);
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }
}
