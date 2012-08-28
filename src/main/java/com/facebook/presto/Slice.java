package com.facebook.presto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;

/**
 * Little Endian slice
 */
public interface Slice
        extends Comparable<Slice>
{
    /**
     * Length of this slice.
     */
    int length();

    /**
     * Gets a byte at the specified absolute {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 1} is greater than {@code this.length()}
     */
    byte getByte(int index);

    /**
     * Gets an unsigned byte at the specified absolute {@code index} in this
     * buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 1} is greater than {@code this.length()}
     */
    short getUnsignedByte(int index);

    /**
     * Gets a 16-bit short integer at the specified absolute {@code index} in
     * this slice.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 2} is greater than {@code this.length()}
     */
    short getShort(int index);

    /**
     * Gets a 32-bit integer at the specified absolute {@code index} in
     * this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 4} is greater than {@code this.length()}
     */
    int getInt(int index);

    /**
     * Gets a 64-bit long integer at the specified absolute {@code index} in
     * this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 8} is greater than {@code this.length()}
     */
    long getLong(int index);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @param destinationIndex the first index of the destination
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code destinationIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code destinationIndex + length} is greater than
     * {@code destination.length()}
     */
    void getBytes(int index, Slice destination, int destinationIndex, int length);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index}.
     *
     * @param destinationIndex the first index of the destination
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code destinationIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code destinationIndex + length} is greater than
     * {@code destination.length}
     */
    void getBytes(int index, byte[] destination, int destinationIndex, int length);

    /**
     * Returns a copy of this buffer as a byte array.
     */
    byte[] getBytes();

    /**
     * Returns a copy of this buffer as a byte array.
     *
     * @param index the absolute index to start at
     * @param length the number of bytes to return
     * @throws IndexOutOfBoundsException if the specified {@code index} is less then {@code 0},
     * or if the specified {@code index + length} is greater than {@code this.length()}
     */
    byte[] getBytes(int index, int length);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index} until the destination's position
     * reaches its limit.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * if {@code index + destination.remaining()} is greater than
     * {@code this.length()}
     */
    void getBytes(int index, ByteBuffer destination);

    /**
     * Transfers this buffer's data to the specified stream starting at the
     * specified absolute {@code index}.
     *
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * if {@code index + length} is greater than
     * {@code this.length()}
     * @throws java.io.IOException if the specified stream threw an exception during I/O
     */
    void getBytes(int index, OutputStream out, int length)
            throws IOException;

    /**
     * Transfers this buffer's data to the specified channel starting at the
     * specified absolute {@code index}.
     *
     * @param length the maximum number of bytes to transfer
     * @return the actual number of bytes written out to the specified channel
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * if {@code index + length} is greater than
     * {@code this.length()}
     * @throws java.io.IOException if the specified channel threw an exception during I/O
     */
    int getBytes(int index, WritableByteChannel out, int length)
            throws IOException;

    /**
     * Sets the specified 16-bit short integer at the specified absolute
     * {@code index} in this buffer.  The 16 high-order bits of the specified
     * value are ignored.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 2} is greater than {@code this.length()}
     */
    void setShort(int index, int value);

    /**
     * Sets the specified 32-bit integer at the specified absolute
     * {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 4} is greater than {@code this.length()}
     */
    void setInt(int index, int value);

    /**
     * Sets the specified 64-bit long integer at the specified absolute
     * {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 8} is greater than {@code this.length()}
     */
    void setLong(int index, long value);

    /**
     * Sets the specified byte at the specified absolute {@code index} in this
     * buffer.  The 24 high-order bits of the specified value are ignored.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 1} is greater than {@code this.length()}
     */
    void setByte(int index, int value);

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the specified absolute {@code index}.
     *
     * @param sourceIndex the first index of the source
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code sourceIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code sourceIndex + length} is greater than
     * {@code source.length()}
     */
    void setBytes(int index, Slice source, int sourceIndex, int length);

    /**
     * Transfers the specified source array's data to this buffer starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code sourceIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code sourceIndex + length} is greater than {@code source.length}
     */
    void setBytes(int index, byte[] source, int sourceIndex, int length);

    /**
     * Transfers the specified source buffer's data to this buffer starting at
     * the specified absolute {@code index} until the source buffer's position
     * reaches its limit.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * if {@code index + source.remaining()} is greater than
     * {@code this.length()}
     */
    void setBytes(int index, ByteBuffer source);

    /**
     * Transfers the content of the specified source stream to this buffer
     * starting at the specified absolute {@code index}.
     *
     * @param length the number of bytes to transfer
     * @return the actual number of bytes read in from the specified channel.
     *         {@code -1} if the specified channel is closed.
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * if {@code index + length} is greater than {@code this.length()}
     * @throws java.io.IOException if the specified stream threw an exception during I/O
     */
    int setBytes(int index, InputStream in, int length)
            throws IOException;

    /**
     * Transfers the content of the specified source channel to this buffer
     * starting at the specified absolute {@code index}.
     *
     * @param length the maximum number of bytes to transfer
     * @return the actual number of bytes read in from the specified channel.
     *         {@code -1} if the specified channel is closed.
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * if {@code index + length} is greater than {@code this.length()}
     * @throws java.io.IOException if the specified channel threw an exception during I/O
     */
    int setBytes(int index, ReadableByteChannel in, int length)
            throws IOException;

    int setBytes(int index, FileChannel in, int position, int length)
            throws IOException;

    /**
     * Returns a copy of this buffer. Modifying the content of the returned
     * buffer or this buffer does not affect each other at all.
     */
    Slice copySlice();

    /**
     * Returns a copy of this buffer's sub-region.  Modifying the content of
     * the returned buffer or this buffer does not affect each other at all.
     */
    Slice copySlice(int index, int length);

    /**
     * Returns a slice of this buffer's sub-region. Modifying the content of
     * the returned buffer or this buffer affects each other's content.
     */
    Slice slice(int index, int length);

    /**
     * Creates an input stream over this slice.
     */
    SliceInput input();

    /**
     * Creates an output stream over this slice.
     */
    SliceOutput output();

    /**
     * Converts this buffer's readable bytes into a NIO buffer.  The returned
     * buffer shares the content with this buffer and is a slice (position
     * is zero and limit is equal to capacity).
     */
    ByteBuffer toByteBuffer();

    /**
     * Converts this buffer's sub-region into a NIO buffer.  The returned
     * buffer shares the content with this buffer and is a slice (position
     * is zero and limit is equal to capacity).
     */
    ByteBuffer toByteBuffer(int index, int length);

    /**
     * Decodes this buffer's readable bytes into a string with the specified
     * character set name.
     */
    String toString(Charset charset);

    /**
     * Decodes this buffer's sub-region into a string with the specified
     * character set.
     */
    String toString(int index, int length, Charset charset);

    /**
     * Compares the content of the specified buffer to the content of this
     * buffer.  This comparison is performed byte by byte using an unsigned
     * comparison.
     */
    int compareTo(Slice that);

    /**
     * Compares the specified object with this slice for equality. All
     * implementations of slice must be equal to any other implementation
     * if the contents of the buffer are the same. In other words, two
     * slices <tt>x</tt> and <tt>y</tt> are equal if
     * <tt>Arrays.equals(x.getBytes(), y.getBytes())</tt>.
     */
    @Override
    boolean equals(Object o);

    /**
     * Returns the hash code of this slice, which is defined as
     * <tt>Arrays.hashCode(getBytes())</tt>, with the exception that
     * the value {@code 0} is converted to a {@code 1} (to make it easier
     * for implementations to cache the hash code).
     */
    @Override
    int hashCode();


    boolean equals(int offset, int length, Slice other, int otherOffset, int otherLength);
}
