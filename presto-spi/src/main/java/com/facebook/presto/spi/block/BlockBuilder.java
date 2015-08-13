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
package com.facebook.presto.spi.block;

import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public interface BlockBuilder
        extends Block
{
    /**
     * Write a byte to the current entry;
     */
    BlockBuilder writeByte(int value);

    /**
     * Write a short to the current entry;
     */
    BlockBuilder writeShort(int value);

    /**
     * Write a int to the current entry;
     */
    BlockBuilder writeInt(int value);

    /**
     * Write a long to the current entry;
     */
    BlockBuilder writeLong(long value);

    /**
     * Write a float to the current entry;
     */
    BlockBuilder writeFloat(float v);

    /**
     * Write a double to the current entry;
     */
    BlockBuilder writeDouble(double value);

    /**
     * Write a byte sequences to the current entry;
     */
    BlockBuilder writeBytes(Slice source, int sourceIndex, int length);

    /**
     * Write an object to the current entry;
     */
    default BlockBuilder writeObject(Object value)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Write a native value object to the current entry;
     */
    default BlockBuilder write(Type type, Object value)
    {
        if (value == null) {
            appendNull();
        }
        else if (type.getJavaType() == boolean.class) {
            type.writeBoolean(this, (Boolean) value);
        }
        else if (type.getJavaType() == double.class) {
            type.writeDouble(this, ((Number) value).doubleValue());
        }
        else if (type.getJavaType() == long.class) {
            type.writeLong(this, ((Number) value).longValue());
        }
        else if (type.getJavaType() == Slice.class) {
            Slice slice;
            if (value instanceof byte[]) {
                slice = Slices.wrappedBuffer((byte[]) value);
            }
            else if (value instanceof String) {
                slice = Slices.utf8Slice((String) value);
            }
            else {
                slice = (Slice) value;
            }
            type.writeSlice(this, slice, 0, slice.length());
        }
        else {
            type.writeObject(this, value);
        }
        return this;
    }

    /**
     * Return a writer to the current entry. The caller can operate on the returned caller to incrementally build the object. This is generally more efficient than
     * building the object elsewhere and call writeObject afterwards because a large chunk of memory could potentially be unnecessarily copied in this process.
     */
    default BlockBuilder beginBlockEntry()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Write a byte to the current entry;
     */
    BlockBuilder closeEntry();

    /**
     * Appends a null value to the block.
     */
    BlockBuilder appendNull();

    /**
     * Builds the block. This method can be called multiple times.
     */
    Block build();
}
