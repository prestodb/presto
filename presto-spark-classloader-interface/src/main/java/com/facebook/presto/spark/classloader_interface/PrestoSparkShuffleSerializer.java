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

import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import scala.Tuple2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;
import scala.reflect.ClassTag;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

public class PrestoSparkShuffleSerializer
        extends Serializer
        implements Serializable
{
    @Override
    public SerializerInstance newInstance()
    {
        return new PrestoSparkShuffleSerializerInstance();
    }

    @Override
    public boolean supportsRelocationOfSerializedObjects()
    {
        return true;
    }

    public static class PrestoSparkShuffleSerializerInstance
            extends SerializerInstance
    {
        private final PrestoSparkMutableRow row = new PrestoSparkMutableRow();
        private final Tuple2<MutablePartitionId, PrestoSparkMutableRow> tuple = new Tuple2<>(null, row);

        @Override
        public SerializationStream serializeStream(OutputStream outputStream)
        {
            return new PrestoSparkShuffleSerializationStream(outputStream);
        }

        @Override
        public DeserializationStream deserializeStream(InputStream inputStream)
        {
            return new PrestoSparkShuffleDeserializationStream(inputStream);
        }

        @Override
        public <T> ByteBuffer serialize(T input, ClassTag<T> classTag)
        {
            Tuple2<MutablePartitionId, PrestoSparkMutableRow> tuple = (Tuple2<MutablePartitionId, PrestoSparkMutableRow>) input;
            PrestoSparkMutableRow row = tuple._2;
            return row.getBuffer();
        }

        @Override
        public <T> T deserialize(ByteBuffer buffer, ClassTag<T> classTag)
        {
            throw new UnsupportedOperationException("this method is never used by shuffle");
        }

        public <T> T deserialize(byte[] array, int offset, int length, ClassTag<T> classTag)
        {
            row.setArray(array);
            row.setOffset(offset);
            row.setLength(length);
            return (T) tuple;
        }

        @Override
        public <T> T deserialize(ByteBuffer bytes, ClassLoader loader, ClassTag<T> classTag)
        {
            throw new UnsupportedOperationException("this method is never used by shuffle");
        }
    }

    public static class PrestoSparkShuffleSerializationStream
            extends SerializationStream
    {
        private final DataOutputStream outputStream;

        public PrestoSparkShuffleSerializationStream(OutputStream outputStream)
        {
            this.outputStream = new DataOutputStream(requireNonNull(outputStream, "outputStream is null"));
        }

        @Override
        public <T> SerializationStream writeKey(T key, ClassTag<T> classTag)
        {
            // key is only needed to select partition
            return this;
        }

        @Override
        public <T> SerializationStream writeValue(T value, ClassTag<T> classTag)
        {
            PrestoSparkMutableRow row = (PrestoSparkMutableRow) value;
            ByteBuffer buffer = row.getBuffer();
            int length = buffer.remaining();
            try {
                outputStream.writeInt(length);
                outputStream.write(buffer.array(), buffer.arrayOffset() + buffer.position(), length);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return this;
        }

        @Override
        public void flush()
        {
            try {
                outputStream.flush();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close()
        {
            try {
                outputStream.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public <T> SerializationStream writeObject(T tuple, ClassTag<T> classTag)
        {
            throw new UnsupportedOperationException("this method is never used by shuffle");
        }

        @Override
        public <T> SerializationStream writeAll(Iterator<T> iterator, ClassTag<T> classTag)
        {
            throw new UnsupportedOperationException("this method is never used by shuffle");
        }
    }

    public static class PrestoSparkShuffleDeserializationStream
            extends DeserializationStream
    {
        private final DataInputStream inputStream;

        private final MutablePartitionId mutablePartitionId = new MutablePartitionId();
        private final PrestoSparkMutableRow row = new PrestoSparkMutableRow();
        private final Tuple2<Object, Object> tuple = new Tuple2<>(mutablePartitionId, row);

        private byte[] bytes;
        private ByteBuffer buffer;

        public PrestoSparkShuffleDeserializationStream(InputStream inputStream)
        {
            this.inputStream = new DataInputStream(requireNonNull(inputStream, "inputStream is null"));
        }

        @Override
        public Iterator<Tuple2<Object, Object>> asKeyValueIterator()
        {
            return new AbstractIterator<Tuple2<Object, Object>>()
            {
                private Tuple2<Object, Object> next;

                @Override
                public boolean hasNext()
                {
                    if (next == null) {
                        next = tryComputeNext();
                    }
                    return next != null;
                }

                @Override
                public Tuple2<Object, Object> next()
                {
                    if (next == null) {
                        next = tryComputeNext();
                    }
                    if (next == null) {
                        throw new NoSuchElementException();
                    }
                    Tuple2<Object, Object> result = next;
                    next = null;
                    return result;
                }

                private Tuple2<Object, Object> tryComputeNext()
                {
                    int length;
                    try {
                        length = inputStream.readInt();
                    }
                    catch (EOFException e) {
                        return null;
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }

                    // ensure capacity
                    if (bytes == null || bytes.length < length) {
                        bytes = new byte[length];
                        buffer = ByteBuffer.wrap(bytes);
                    }

                    try {
                        inputStream.readFully(bytes, 0, length);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }

                    buffer.position(0);
                    buffer.limit(length);
                    row.setBuffer(buffer);
                    return tuple;
                }
            };
        }

        @Override
        public <T> T readKey(ClassTag<T> classTag)
        {
            throw new UnsupportedOperationException("this method is never used by shuffle");
        }

        @Override
        public <T> T readValue(ClassTag<T> classTag)
        {
            throw new UnsupportedOperationException("this method is never used by shuffle");
        }

        @Override
        public Iterator<Object> asIterator()
        {
            throw new UnsupportedOperationException("this method is never used by shuffle");
        }

        @Override
        public <T> T readObject(ClassTag<T> classTag)
        {
            throw new UnsupportedOperationException("this method is never used by shuffle");
        }

        @Override
        public void close()
        {
            try {
                inputStream.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
