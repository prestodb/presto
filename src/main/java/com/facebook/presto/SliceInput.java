package com.facebook.presto;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.charset.Charset;

public final class SliceInput extends InputStream implements DataInput
{
    private final Slice slice;
    private int position;

    public SliceInput(Slice slice)
    {
        this.slice = slice;
    }

    /**
     * Returns the {@code position} of this buffer.
     */
    public int position()
    {
        return position;
    }

    /**
     * Sets the {@code position} of this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code position} is
     * less than {@code 0} or
     * greater than {@code this.writerIndex}
     */
    public void setPosition(int position)
    {
        if (position < 0 || position > slice.length()) {
            throw new IndexOutOfBoundsException();
        }
        this.position = position;
    }

    /**
     * Returns {@code true}
     * if and only if {@code available()} is greater
     * than {@code 0}.
     */
    public boolean isReadable()
    {
        return available() > 0;
    }

    /**
     * Returns the number of readable bytes which is equal to
     * {@code (this.slice.length() - this.position)}.
     */
    public int available()
    {
        return slice.length() - position;
    }

    @Override
    public boolean readBoolean()
            throws IOException
    {
        return readByte() != 0;
    }

    @Override
    public int read()
    {
        return readByte();
    }

    /**
     * Gets a byte at the current {@code position} and increases
     * the {@code position} by {@code 1} in this buffer.
     *
     * @throws IndexOutOfBoundsException if {@code this.available()} is less than {@code 1}
     */
    public byte readByte()
    {
        if (position == slice.length()) {
            throw new IndexOutOfBoundsException();
        }
        return slice.getByte(position++);
    }

    /**
     * Gets an unsigned byte at the current {@code position} and increases
     * the {@code position} by {@code 1} in this buffer.
     *
     * @throws IndexOutOfBoundsException if {@code this.available()} is less than {@code 1}
     */
    public int readUnsignedByte()
    {
        return (short) (readByte() & 0xFF);
    }

    /**
     * Gets a 16-bit short integer at the current {@code position}
     * and increases the {@code position} by {@code 2} in this buffer.
     *
     * @throws IndexOutOfBoundsException if {@code this.available()} is less than {@code 2}
     */
    public short readShort()
    {
        short v = slice.getShort(position);
        position += 2;
        return v;
    }

    @Override
    public int readUnsignedShort()
            throws IOException
    {
        return readShort() & 0xff;
    }

    /**
     * Gets a 32-bit integer at the current {@code position}
     * and increases the {@code position} by {@code 4} in this buffer.
     *
     * @throws IndexOutOfBoundsException if {@code this.available()} is less than {@code 4}
     */
    public int readInt()
    {
        int v = slice.getInt(position);
        position += 4;
        return v;
    }

    /**
     * Gets an unsigned 32-bit integer at the current {@code position}
     * and increases the {@code position} by {@code 4} in this buffer.
     *
     * @throws IndexOutOfBoundsException if {@code this.available()} is less than {@code 4}
     */
    public long readUnsignedInt()
    {
        return readInt() & 0xFFFFFFFFL;
    }

    /**
     * Gets a 64-bit integer at the current {@code position}
     * and increases the {@code position} by {@code 8} in this buffer.
     *
     * @throws IndexOutOfBoundsException if {@code this.available()} is less than {@code 8}
     */
    public long readLong()
    {
        long v = slice.getLong(position);
        position += 8;
        return v;
    }

    public byte[] readByteArray(int length)
    {
        byte[] value = slice.copyBytes(position, length);
        position += length;
        return value;
    }

    /**
     * Transfers this buffer's data to a newly created buffer starting at
     * the current {@code position} and increases the {@code position}
     * by the number of the transferred bytes (= {@code length}).
     * The returned buffer's {@code position} and {@code writerIndex} are
     * {@code 0} and {@code length} respectively.
     *
     * @param length the number of bytes to transfer
     * @return the newly created buffer which contains the transferred bytes
     * @throws IndexOutOfBoundsException if {@code length} is greater than {@code this.available()}
     */
    public Slice readBytes(int length)
    {
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        Slice value = slice.slice(position, length);
        position += length;
        return value;
    }

    /**
     * Returns a new slice of this buffer's sub-region starting at the current
     * {@code position} and increases the {@code position} by the size
     * of the new slice (= {@code length}).
     *
     * @param length the size of the new slice
     * @return the newly created slice
     * @throws IndexOutOfBoundsException if {@code length} is greater than {@code this.available()}
     */
    public Slice readSlice(int length)
    {
        Slice newSlice = slice.slice(position, length);
        position += length;
        return newSlice;
    }

    @Override
    public void readFully(byte[] destination)
    {
        readBytes(destination);
    }

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the current {@code position} and increases the {@code position}
     * by the number of the transferred bytes (= {@code dst.length}).
     *
     * @throws IndexOutOfBoundsException if {@code dst.length} is greater than {@code this.available()}
     */
    public void readBytes(byte[] destination)
    {
        readBytes(destination, 0, destination.length);
    }

    @Override
    public void readFully(byte[] destination, int offset, int length)
    {
        readBytes(destination, offset, length);
    }

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the current {@code position} and increases the {@code position}
     * by the number of the transferred bytes (= {@code length}).
     *
     * @param destinationIndex the first index of the destination
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code destinationIndex} is less than {@code 0},
     * if {@code length} is greater than {@code this.available()}, or
     * if {@code destinationIndex + length} is greater than {@code destination.length}
     */
    public void readBytes(byte[] destination, int destinationIndex, int length)
    {
        slice.getBytes(position, destination, destinationIndex, length);
        position += length;
    }

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the current {@code position} until the destination becomes
     * non-writable, and increases the {@code position} by the number of the
     * transferred bytes.  This method is basically same with
     * {@link #readBytes(Slice, int, int)}, except that this method
     * increases the {@code writerIndex} of the destination by the number of
     * the transferred bytes while {@link #readBytes(Slice, int, int)}
     * does not.
     *
     * @throws IndexOutOfBoundsException if {@code destination.writableBytes} is greater than
     * {@code this.available()}
     */
    public void readBytes(Slice destination)
    {
        readBytes(destination, destination.length());
    }

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the current {@code position} and increases the {@code position}
     * by the number of the transferred bytes (= {@code length}).  This method
     * is basically same with {@link #readBytes(Slice, int, int)},
     * except that this method increases the {@code writerIndex} of the
     * destination by the number of the transferred bytes (= {@code length})
     * while {@link #readBytes(Slice, int, int)} does not.
     *
     * @throws IndexOutOfBoundsException if {@code length} is greater than {@code this.available()} or
     * if {@code length} is greater than {@code destination.writableBytes}
     */
    public void readBytes(Slice destination, int length)
    {
        if (length > destination.length()) {
            throw new IndexOutOfBoundsException();
        }
        readBytes(destination, destination.length(), length);
    }

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the current {@code position} and increases the {@code position}
     * by the number of the transferred bytes (= {@code length}).
     *
     * @param destinationIndex the first index of the destination
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code destinationIndex} is less than {@code 0},
     * if {@code length} is greater than {@code this.available()}, or
     * if {@code destinationIndex + length} is greater than
     * {@code destination.capacity}
     */
    public void readBytes(Slice destination, int destinationIndex, int length)
    {
        slice.getBytes(position, destination, destinationIndex, length);
        position += length;
    }

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the current {@code position} until the destination's position
     * reaches its limit, and increases the {@code position} by the
     * number of the transferred bytes.
     *
     * @throws IndexOutOfBoundsException if {@code destination.remaining()} is greater than
     * {@code this.available()}
     */
    public void readBytes(ByteBuffer destination)
    {
        int length = destination.remaining();
        slice.getBytes(position, destination);
        position += length;
    }

    /**
     * Transfers this buffer's data to the specified stream starting at the
     * current {@code position}.
     *
     * @param length the maximum number of bytes to transfer
     * @return the actual number of bytes written out to the specified channel
     * @throws IndexOutOfBoundsException if {@code length} is greater than {@code this.available()}
     * @throws java.io.IOException if the specified channel threw an exception during I/O
     */
    public int readBytes(GatheringByteChannel out, int length)
            throws IOException
    {
        int readBytes = slice.getBytes(position, out, length);
        position += readBytes;
        return readBytes;
    }

    /**
     * Transfers this buffer's data to the specified stream starting at the
     * current {@code position}.
     *
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if {@code length} is greater than {@code this.available()}
     * @throws java.io.IOException if the specified stream threw an exception during I/O
     */
    public void readBytes(OutputStream out, int length)
            throws IOException
    {
        slice.getBytes(position, out, length);
        position += length;
    }

    public int skipBytes(int length)
    {
        length = Math.min(length, available());
        position += length;
        return length;
    }

    /**
     * Returns a slice of this buffer's readable bytes. Modifying the content
     * of the returned buffer or this buffer affects each other's content
     * while they maintain separate indexes and marks.  This method is
     * identical to {@code buf.slice(buf.position(), buf.available()())}.
     * This method does not modify {@code position} or {@code writerIndex} of
     * this buffer.
     */
    public Slice slice()
    {
        return slice.slice(position, available());
    }

    /**
     * Converts this buffer's readable bytes into a NIO buffer.  The returned
     * buffer might or might not share the content with this buffer, while
     * they have separate indexes and marks.  This method is identical to
     * {@code buf.toByteBuffer(buf.position(), buf.available()())}.
     * This method does not modify {@code position} or {@code writerIndex} of
     * this buffer.
     */
    public ByteBuffer toByteBuffer()
    {
        return slice.toByteBuffer(position, available());
    }

    /**
     * Decodes this buffer's readable bytes into a string with the specified
     * character set name.  This method is identical to
     * {@code buf.toString(buf.position(), buf.available()(), charsetName)}.
     * This method does not modify {@code position} or {@code writerIndex} of
     * this buffer.
     *
     * @throws java.nio.charset.UnsupportedCharsetException if the specified character set name is not supported by the
     * current VM
     */
    public String toString(Charset charset)
    {
        return slice.toString(position, available(), charset);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + '(' +
                "ridx=" + position + ", " +
                "cap=" + slice.length() +
                ')';
    }

    //
    // Unsupported operations
    //

    /**
     * Unsupported operation
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public char readChar()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported operation
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public float readFloat()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double readDouble()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported operation
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public String readLine()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported operation
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public String readUTF()
    {
        throw new UnsupportedOperationException();
    }
}
