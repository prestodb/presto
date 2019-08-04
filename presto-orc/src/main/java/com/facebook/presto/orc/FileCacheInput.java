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
package com.facebook.presto.orc;

import io.airlift.slice.FixedLengthSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

final class FileCacheInput
        extends FixedLengthSliceInput
{
    public static final int MAX_BUFFER_SIZE = (8 << 20) - 128;
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FileCacheInput.class).instanceSize();

    private final StreamId streamId;
    private final ReadTracker tracker;
    // Position in file of first byte covered by this.
    private final long loadOffset;
    // Total number of vytes covered by this.
    private final int loadLength;
    // Offset of logical position 0 from loadOffset. Non-0 if this stream represents a subrange in a larger buffer, so that the larger buffer is to be wholly loaded when the first subrange is accessed.
    private final int viewOffset;
    // Number of bytes visible, starting at viewOffset.
    private final int viewLength;
    private FileCache.Entry entry;
    private byte[] buffer;
    // Offset of 1st byte in buffer from loadOffset.
    private int bufferOffset;
    // Number of valid bytes in buffer.
    private int bufferBytes;
    // Position relative to loadOffset.
    private int position;
    private FixedLengthSliceInput delegate;
    private OrcDataSource dataSource;
    private int readCount;
    private int prefetchWatermark = MAX_BUFFER_SIZE;
    private FileCache.Listener listener;

    public FileCacheInput(OrcDataSource dataSource, StreamId streamId, ReadTracker tracker, long loadOffset, long loadLength, int viewOffset, int viewLength)
    {
        checkArgument(viewLength > 0, "viewLength must be at least 1");
        checkArgument(viewLength <= Integer.MAX_VALUE, "viewLength must be less than 2GB");
        checkArgument(viewLength + viewOffset <= loadLength, "viewOffset + viewLength must fit in loadLength bytes");
        checkArgument(viewOffset > 0 ? loadLength <= MAX_BUFFER_SIZE : true, "An offset view applies only to small disk ranges");
        this.tracker = requireNonNull(tracker);
        this.streamId = requireNonNull(streamId);
        this.loadOffset = loadOffset;
        this.loadLength = toIntExact(loadLength);
        this.viewOffset = viewOffset;
        this.viewLength = viewLength;
        this.position = viewOffset;
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        if (streamId.getLabel() != null) {
            listener = FileCache.getListener(streamId.getLabel());
        }
    }

    // Ensures that delegate has at least the next length
    // characters. When the range spans 2 buffers, returns a delegate
    // over a copy of the last part of the previous and the first bytes of
    // the new buffer. Length must be less than the max buffer size.
    private FixedLengthSliceInput getDelegate(int length)
    {
        readCount++;
        if (buffer == null) {
            loadPosition(position);
        }
        int available = available();
        if (available >= length) {
            if (position - bufferOffset > prefetchWatermark && readCount > 3) {
                FileCache.asyncPrefetch(dataSource, loadOffset + bufferOffset + MAX_BUFFER_SIZE, Math.min(loadLength - (bufferOffset + MAX_BUFFER_SIZE), MAX_BUFFER_SIZE), listener, 100);
                prefetchWatermark = MAX_BUFFER_SIZE;
            }
            position += length;
            return delegate;
        }
        checkArgument(length < MAX_BUFFER_SIZE);
        verify(viewOffset == 0, "A multi-buffer FileCacheInput is supposed to have a 0 viewOffset");
        byte[] temp = new byte[length];
        System.arraycopy(buffer, position - bufferOffset, temp, 0, available);
        entry.close();
        entry = null;
        bufferOffset += MAX_BUFFER_SIZE;
        bufferBytes = Math.min(MAX_BUFFER_SIZE, loadLength - bufferOffset);
        try {
            entry = FileCache.get(dataSource, loadOffset + bufferOffset, bufferBytes, listener, 100);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        buffer = entry.getBuffer();
        delegate = Slices.wrappedBuffer(buffer, 0, bufferBytes).getInput();
        System.arraycopy(buffer, 0, temp, available, length - available);
        delegate.skip(length - available);
        position += length;
        return Slices.wrappedBuffer(temp, 0, length).getInput();
    }

    private void loadPosition(int position)
    {
        if (entry != null) {
            entry.close();
            entry = null;
        }
        int loadStart = quantize(position);
        tracker.recordRead(streamId);
        bufferBytes = Math.min(MAX_BUFFER_SIZE, loadLength - loadStart);
        try {
            entry = FileCache.get(dataSource, loadOffset + loadStart, bufferBytes, listener, 100);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        bufferOffset = loadStart;
        if (bufferBytes == MAX_BUFFER_SIZE) {
            prefetchWatermark = MAX_BUFFER_SIZE / 4 * 3;
        }
        buffer = entry.getBuffer();
        delegate = Slices.wrappedBuffer(buffer, 0, bufferBytes).getInput();
        delegate.setPosition(position - bufferOffset);
    }

    private static int quantize(int position)
    {
        // Round down to previous multiple of MAX_BUFFER_SIZE.
        return (position / MAX_BUFFER_SIZE) * MAX_BUFFER_SIZE;
    }

    @Override
    public long length()
    {
        return viewLength;
    }

    @Override
    public long position()
    {
        return position - viewOffset;
    }

    @Override
    public void setPosition(long position)
    {
        position += viewOffset;
        checkArgument(position >= 0 && position < loadLength);
        this.position = toIntExact(position);
        if (buffer != null) {
            if (position >= bufferOffset && position - bufferOffset < bufferBytes) {
                delegate.setPosition(position - bufferOffset);
            }
            else {
                loadPosition(toIntExact(position));
            }
        }
    }

    @Override
    public boolean isReadable()
    {
        return position < viewLength;
    }

    @Override
    public int available()
    {
        if (delegate == null) {
            return 0;
        }
        if (viewOffset > 0 || viewLength != loadLength) {
            return viewLength - (position - viewOffset);
        }
        return bufferBytes - (position - bufferOffset);
    }

    @Override
    public int read()
    {
        return getDelegate(1).read();
    }

    @Override
    public boolean readBoolean()
    {
        return getDelegate(1).readBoolean();
    }

    @Override
    public byte readByte()
    {
        return getDelegate(1).readByte();
    }

    @Override
    public int readUnsignedByte()
    {
        return getDelegate(1).readUnsignedByte();
    }

    @Override
    public short readShort()
    {
        return getDelegate(2).readShort();
    }

    @Override
    public int readUnsignedShort()
    {
        return getDelegate(2).readUnsignedShort();
    }

    @Override
    public int readInt()
    {
        return getDelegate(4).readInt();
    }

    @Override
    public long readLong()
    {
        return getDelegate(8).readLong();
    }

    @Override
    public float readFloat()
    {
        return getDelegate(4).readFloat();
    }

    @Override
    public double readDouble()
    {
        return getDelegate(8).readDouble();
    }

    @Override
    public Slice readSlice(int length)
    {
        return getDelegate(length).readSlice(length);
    }

    @Override
    public int read(byte[] destination, int destinationIndex, int length)
    {
        return getDelegate(8).read(destination, destinationIndex, length);
    }

    @Override
    public void readBytes(byte[] destination, int destinationIndex, int length)
    {
        getDelegate(length).readBytes(destination, destinationIndex, length);
    }

    @Override
    public void readBytes(Slice destination, int destinationIndex, int length)
    {
        getDelegate(length).readBytes(destination, destinationIndex, length);
    }

    @Override
    public void readBytes(OutputStream out, int length)
            throws IOException
    {
        getDelegate(length).readBytes(out, length);
    }

    @Override
    public long skip(long length)
    {
        if (length == 0) {
            return 0;
        }
        if (length <= available()) {
            position += length;
            return delegate.skip(length);
        }
        if (entry != null) {
            entry.close();
            entry = null;
            buffer = null;
            delegate = null;
        }
        position += length;
        return length;
    }

    @Override
    public int skipBytes(int length)
    {
        return (int) skip(length);
    }

    @Override
    public void close()
    {
        if (entry != null) {
            entry.close();
            entry = null;
        }
        delegate = null;
    }

    @Override
    public long getRetainedSize()
    {
        return INSTANCE_SIZE + getDelegate(0).getRetainedSize();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("length", viewLength)
                .add("loaded", bufferBytes)
            .toString();
    }
}
