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
package com.facebook.presto.plugin.turbonium.storage;

import org.openjdk.jol.info.ClassLayout;

import java.util.BitSet;

import static com.facebook.presto.plugin.turbonium.storage.Util.sizeOfBitSet;
import static io.airlift.slice.SizeOf.sizeOf;

public interface Values
{
    default long getLong(int position)
    {
        throw new UnsupportedOperationException();
    }

    default int getInt(int position)
    {
        throw new UnsupportedOperationException();
    }

    default short getShort(int position)
    {
        throw new UnsupportedOperationException();
    }

    default byte getByte(int position)
    {
        throw new UnsupportedOperationException();
    }

    default boolean getBoolean(int position)
    {
        throw new UnsupportedOperationException();
    }

    default long getSizeBytes()
    {
        throw new UnsupportedOperationException();
    }

    class IntValues
            implements Values
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(IntValues.class).instanceSize();
        private final int[] values;

        public IntValues(int[] values)
        {
            this.values = values;
        }

        @Override
        public long getLong(int position)
        {
            return 0xffff_ffffL & values[position];
        }

        @Override
        public long getSizeBytes()
        {
            return INSTANCE_SIZE + sizeOf(values);
        }
    }

    class ShortValues
            implements Values
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(ShortValues.class).instanceSize();
        private final short[] values;

        public ShortValues(short[] values)
        {
            this.values = values;
        }

        public long getLong(int position)
        {
            return getInt(position);
        }

        public int getInt(int position)
        {
            return 0xffff & values[position];
        }

        @Override
        public long getSizeBytes()
        {
            return INSTANCE_SIZE + sizeOf(values);
        }
    }

    class ByteValues
            implements Values
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(ShortValues.class).instanceSize();
        private final byte[] values;

        public ByteValues(byte[] values)
        {
            this.values = values;
        }

        public long getLong(int position)
        {
            return getShort(position);
        }

        public int getInt(int position)
        {
            return getShort(position);
        }

        public short getShort(int position)
        {
            return (short) (0xff & values[position]);
        }
        @Override
        public long getSizeBytes()
        {
            return INSTANCE_SIZE + sizeOf(values);
        }
    }

    class BooleanValues
            implements Values
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(ShortValues.class).instanceSize();
        private final BitSet values;

        public BooleanValues(BitSet values)
        {
            this.values = values;
        }

        public long getLong(int position)
        {
            return getByte(position);
        }

        public int getInt(int position)
        {
            return getByte(position);
        }

        public short getShort(int position)
        {
            return getByte(position);
        }

        public byte getByte(int position)
        {
            return (byte) (values.get(position) ? 1 : 0);
        }

        public boolean getBoolean(int position)
        {
            return values.get(position);
        }
        @Override
        public long getSizeBytes()
        {
            return INSTANCE_SIZE + sizeOfBitSet(values);
        }
    }
}
