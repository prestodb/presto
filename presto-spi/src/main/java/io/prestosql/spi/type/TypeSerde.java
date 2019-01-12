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
package io.prestosql.spi.type;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class TypeSerde
{
    private TypeSerde()
    {
    }

    public static void writeType(SliceOutput sliceOutput, Type type)
    {
        requireNonNull(sliceOutput, "sliceOutput is null");
        requireNonNull(type, "type is null");
        writeLengthPrefixedString(sliceOutput, type.getTypeSignature().toString());
    }

    public static Type readType(TypeManager typeManager, SliceInput sliceInput)
    {
        requireNonNull(sliceInput, "sliceInput is null");

        String name = readLengthPrefixedString(sliceInput);
        Type type = typeManager.getType(parseTypeSignature(name));
        if (type == null) {
            throw new IllegalArgumentException("Unknown type " + name);
        }
        return type;
    }

    private static String readLengthPrefixedString(SliceInput input)
    {
        int length = input.readInt();
        byte[] bytes = new byte[length];
        input.readBytes(bytes);
        return new String(bytes, UTF_8);
    }

    private static void writeLengthPrefixedString(SliceOutput output, String string)
    {
        byte[] bytes = string.getBytes(UTF_8);
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }
}
