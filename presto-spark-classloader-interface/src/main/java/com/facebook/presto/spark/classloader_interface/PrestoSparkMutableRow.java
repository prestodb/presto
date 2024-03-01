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
package com.facebook.presto.spark.classloader_interface;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Externalizable;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

public class PrestoSparkMutableRow
        implements Externalizable, KryoSerializable, PrestoSparkTaskOutput
{
    private ByteBuffer buffer;

    // Can be backed either by the "buffer" or by the "array"
    // Row is backed by the "array" only when deserialized
    // with the PrestoSparkShuffleSerializerInstance#deserialize(byte[], int, int, ClassTag<T>)
    private byte[] array;
    // offset and length are meaningful only when the row is backed by the "array"
    private int offset;
    private int length;

    private int positionCount;

    public ByteBuffer getBuffer()
    {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer)
    {
        this.buffer = buffer;
    }

    public byte[] getArray()
    {
        return array;
    }

    public PrestoSparkMutableRow setArray(byte[] array)
    {
        this.array = array;
        return this;
    }

    public int getOffset()
    {
        return offset;
    }

    public PrestoSparkMutableRow setOffset(int offset)
    {
        this.offset = offset;
        return this;
    }

    public int getLength()
    {
        return length;
    }

    public PrestoSparkMutableRow setLength(int length)
    {
        this.length = length;
        return this;
    }

    public void setPositionCount(int positionCount)
    {
        this.positionCount = positionCount;
    }

    @Override
    public void write(Kryo kryo, Output output)
    {
        throw serializationNotSupportedException();
    }

    @Override
    public void read(Kryo kryo, Input input)
    {
        throw serializationNotSupportedException();
    }

    @Override
    public void writeExternal(ObjectOutput output)
    {
        throw serializationNotSupportedException();
    }

    @Override
    public void readExternal(ObjectInput input)
    {
        throw serializationNotSupportedException();
    }

    private static RuntimeException serializationNotSupportedException()
    {
        // PrestoSparkMutableRow is expected to be serialized only during shuffle.
        // During shuffle rows are always serialized with PrestoSparkShuffleSerializer.
        return new UnsupportedOperationException("PrestoSparkMutableRow is not expected to be serialized with Kryo or standard Java serialization");
    }

    @Override
    public long getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSize()
    {
        if (buffer == null) {
            throw new IllegalStateException("buffer is expected to be not null");
        }
        return buffer.remaining();
    }
}
