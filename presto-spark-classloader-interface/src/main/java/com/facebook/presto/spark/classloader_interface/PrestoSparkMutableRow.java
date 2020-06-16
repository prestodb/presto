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

    public ByteBuffer getBuffer()
    {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer)
    {
        this.buffer = buffer;
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
        return new UnsupportedOperationException("PrestoSparkUnsafeRow is not expected to be serialized with Kryo or standard Java serialization");
    }
}
