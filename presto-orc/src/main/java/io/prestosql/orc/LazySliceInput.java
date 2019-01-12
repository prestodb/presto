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
package io.prestosql.orc;

import io.airlift.slice.FixedLengthSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

final class LazySliceInput
        extends FixedLengthSliceInput
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LazySliceInput.class).instanceSize();

    private final int globalLength;
    private final Supplier<FixedLengthSliceInput> loader;
    private int initialPosition;
    private FixedLengthSliceInput delegate;

    public LazySliceInput(long globalLength, Supplier<FixedLengthSliceInput> loader)
    {
        checkArgument(globalLength > 0, "globalLength must be at least 1");
        checkArgument(globalLength <= Integer.MAX_VALUE, "globalLength must be less than 2GB");
        this.globalLength = toIntExact(globalLength);
        this.loader = requireNonNull(loader, "loader is null");
    }

    private SliceInput getDelegate()
    {
        if (delegate == null) {
            delegate = requireNonNull(loader.get(), "loader returned a null stream");
            verify(delegate.length() == globalLength, "loader returned stream of length %s, but length %s was expected", delegate.length(), globalLength);
            delegate.setPosition(initialPosition);
        }
        return delegate;
    }

    @Override
    public long length()
    {
        return globalLength;
    }

    @Override
    public long position()
    {
        if (delegate == null) {
            return initialPosition;
        }
        return delegate.position();
    }

    @Override
    public void setPosition(long position)
    {
        if (delegate == null) {
            if (position < 0 || position > globalLength) {
                throw new IndexOutOfBoundsException("Invalid position " + position + " for slice with length " + globalLength);
            }
            initialPosition = toIntExact(position);
            return;
        }
        delegate.setPosition(position);
    }

    @Override
    public boolean isReadable()
    {
        if (delegate == null) {
            return true;
        }
        return delegate.isReadable();
    }

    @Override
    public int available()
    {
        if (delegate == null) {
            return globalLength;
        }
        return delegate.available();
    }

    @Override
    public int read()
    {
        return getDelegate().read();
    }

    @Override
    public boolean readBoolean()
    {
        return getDelegate().readBoolean();
    }

    @Override
    public byte readByte()
    {
        return getDelegate().readByte();
    }

    @Override
    public int readUnsignedByte()
    {
        return getDelegate().readUnsignedByte();
    }

    @Override
    public short readShort()
    {
        return getDelegate().readShort();
    }

    @Override
    public int readUnsignedShort()
    {
        return getDelegate().readUnsignedShort();
    }

    @Override
    public int readInt()
    {
        return getDelegate().readInt();
    }

    @Override
    public long readLong()
    {
        return getDelegate().readLong();
    }

    @Override
    public float readFloat()
    {
        return getDelegate().readFloat();
    }

    @Override
    public double readDouble()
    {
        return getDelegate().readDouble();
    }

    @Override
    public Slice readSlice(int length)
    {
        return getDelegate().readSlice(length);
    }

    @Override
    public int read(byte[] destination, int destinationIndex, int length)
    {
        return getDelegate().read(destination, destinationIndex, length);
    }

    @Override
    public void readBytes(byte[] destination, int destinationIndex, int length)
    {
        getDelegate().readBytes(destination, destinationIndex, length);
    }

    @Override
    public void readBytes(Slice destination, int destinationIndex, int length)
    {
        getDelegate().readBytes(destination, destinationIndex, length);
    }

    @Override
    public void readBytes(OutputStream out, int length)
            throws IOException
    {
        getDelegate().readBytes(out, length);
    }

    @Override
    public long skip(long length)
    {
        return getDelegate().skip(length);
    }

    @Override
    public int skipBytes(int length)
    {
        return getDelegate().skipBytes(length);
    }

    @Override
    public long getRetainedSize()
    {
        return INSTANCE_SIZE + getDelegate().getRetainedSize();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("globalLength", globalLength)
                .add("loaded", delegate != null)
                .toString();
    }
}
