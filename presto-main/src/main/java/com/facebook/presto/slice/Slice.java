package com.facebook.presto.slice;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedLongs;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static com.facebook.presto.slice.SizeOf.SIZE_OF_BYTE;
import static com.facebook.presto.slice.SizeOf.SIZE_OF_DOUBLE;
import static com.facebook.presto.slice.SizeOf.SIZE_OF_INT;
import static com.facebook.presto.slice.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.slice.SizeOf.SIZE_OF_SHORT;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.primitives.UnsignedBytes.toInt;

public class Slice
        implements Comparable<Slice>, InputSupplier<SliceInput>, OutputSupplier<SliceOutput>
{
    private static final Unsafe unsafe;
    private static final MethodHandle newByteBuffer;

    static {
        try {
            // fetch theUnsafe object
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe == null) {
                throw new RuntimeException("Unsafe access not available");
            }

            // make sure the VM thinks bytes are only one byte wide
            int byteArrayIndexScale = unsafe.arrayIndexScale(byte[].class);
            if (byteArrayIndexScale != 1) {
                throw new IllegalStateException("Byte array index scale must be 1, but is " + byteArrayIndexScale);
            }

            // fetch a method handle for the hidden constructor for DirectByteBuffer
            Class<?> directByteBufferClass = ClassLoader.getSystemClassLoader().loadClass("java.nio.DirectByteBuffer");
            Constructor<?> constructor = directByteBufferClass.getDeclaredConstructor(long.class, int.class, Object.class);
            constructor.setAccessible(true);
            newByteBuffer = MethodHandles.lookup().unreflectConstructor(constructor).asType(MethodType.methodType(ByteBuffer.class, long.class, int.class, Object.class));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    private static final long BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);

    public static Slice toUnsafeSlice(ByteBuffer byteBuffer)
    {
        Preconditions.checkNotNull(byteBuffer, "byteBuffer is null");
        Preconditions.checkArgument(byteBuffer instanceof DirectBuffer, "byteBuffer is not an instance of %s", DirectBuffer.class.getName());
        DirectBuffer directBuffer = (DirectBuffer) byteBuffer;
        long address = directBuffer.address();
        int capacity = byteBuffer.capacity();
        return new Slice(null, address, capacity, byteBuffer);
    }

    private final Object base;
    private final long address;
    private final int size;
    private final Object reference;
    private int hash;

    /**
     * Creates an empty slice.
     */
    Slice()
    {
        this.base = null;
        this.address = 0;
        this.size = 0;
        this.reference = null;
    }

    /**
     * Creates a slice over the specified array.
     */
    Slice(byte[] base)
    {
        Preconditions.checkNotNull(base, "base is null");
        this.base = base;
        this.address = BYTE_ARRAY_OFFSET;
        this.size = base.length;
        this.reference = null;
    }

    /**
     * Creates a slice for directly accessing the base object.
     */
    Slice(@Nullable Object base, long address, int size, @Nullable Object reference)
    {
        this.reference = reference;
        if (address <= 0) {
            throw new IllegalArgumentException("Invalid address: " + address);
        }
        if (size <= 0) {
            throw new IllegalArgumentException("Invalid size: " + size);
        }
        if (address + size < size) {
            throw new IllegalArgumentException("Address + size is greater than 64 bits");
        }
        this.base = base;
        this.address = address;
        this.size = size;
    }

    /**
     * Length of this slice.
     */
    public int length()
    {
        return size;
    }

    /**
     * Fill the slice with the specified value;
     */
    public void clear()
    {
        int offset = 0;
        int length = size;
        while (length >= SIZE_OF_LONG) {
            unsafe.putLong(base, this.address + offset, 0);
            offset += SIZE_OF_LONG;
            length -= SIZE_OF_LONG;
        }

        while (length > 0) {
            unsafe.putByte(base, this.address + offset, (byte) 0);
            offset++;
            length--;
        }
    }

    /**
     * Gets a byte at the specified absolute {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 1} is greater than {@code this.length()}
     */
    public byte getByte(int index)
    {
        checkIndexLength(index, SIZE_OF_BYTE);
        return unsafe.getByte(base, address + index);
    }

    /**
     * Gets an unsigned byte at the specified absolute {@code index} in this
     * buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 1} is greater than {@code this.length()}
     */
    public short getUnsignedByte(int index)
    {
        return (short) (getByte(index) & 0xFF);
    }

    /**
     * Gets a 16-bit short integer at the specified absolute {@code index} in
     * this slice.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 2} is greater than {@code this.length()}
     */
    public short getShort(int index)
    {
        checkIndexLength(index, SIZE_OF_SHORT);
        return unsafe.getShort(base, address + index);
    }

    /**
     * Gets a 32-bit integer at the specified absolute {@code index} in
     * this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 4} is greater than {@code this.length()}
     */
    public int getInt(int index)
    {
        checkIndexLength(index, SIZE_OF_INT);
        return unsafe.getInt(base, address + index);
    }

    /**
     * Gets a 64-bit long integer at the specified absolute {@code index} in
     * this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 8} is greater than {@code this.length()}
     */
    public long getLong(int index)
    {
        checkIndexLength(index, SIZE_OF_LONG);
        return unsafe.getLong(base, address + index);
    }

    /**
     * Gets a 64-bit double at the specified absolute {@code index} in
     * this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 8} is greater than {@code this.length()}
     */
    public double getDouble(int index)
    {
        checkIndexLength(index, SIZE_OF_DOUBLE);
        return unsafe.getDouble(base, address + index);
    }

    /**
     * Transfers portion of data from this slice into the specified destination starting at
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
    public void getBytes(int index, Slice destination, int destinationIndex, int length)
    {
        destination.setBytes(destinationIndex, this, index, length);
    }

    /**
     * Transfers portion of data from this slice into the specified destination starting at
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
    public void getBytes(int index, byte[] destination, int destinationIndex, int length)
    {
        checkIndexLength(index, length);
        checkPositionIndexes(destinationIndex, destinationIndex + length, destination.length);

        copyMemory(base, address + index, destination, BYTE_ARRAY_OFFSET + destinationIndex, length);
    }

    /**
     * Returns a copy of this buffer as a byte array.
     */
    public byte[] getBytes()
    {
        return getBytes(0, length());
    }

    /**
     * Returns a copy of this buffer as a byte array.
     *
     * @param index the absolute index to start at
     * @param length the number of bytes to return
     * @throws IndexOutOfBoundsException if the specified {@code index} is less then {@code 0},
     * or if the specified {@code index + length} is greater than {@code this.length()}
     */
    public byte[] getBytes(int index, int length)
    {
        byte[] bytes = new byte[length];
        getBytes(index, bytes, 0, length);
        return bytes;
    }

    /**
     * Transfers a portion of data from this slice into the specified stream starting at the
     * specified absolute {@code index}.
     *
     * @param length the number of bytes to transfer
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * if {@code index + length} is greater than
     * {@code this.length()}
     * @throws java.io.IOException if the specified stream threw an exception during I/O
     */
    public void getBytes(int index, OutputStream out, int length)
            throws IOException
    {
        checkIndexLength(index, length);

        byte[] buffer = new byte[4096];
        while (length > 0) {
            int size = Math.min(buffer.length, length);
            getBytes(index, buffer, 0, size);
            out.write(buffer, 0, size);
            length -= size;
            index += size;
        }
    }

    /**
     * Sets the specified byte at the specified absolute {@code index} in this
     * buffer.  The 24 high-order bits of the specified value are ignored.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 1} is greater than {@code this.length()}
     */
    public void setByte(int index, int value)
    {
        checkIndexLength(index, SIZE_OF_BYTE);
        unsafe.putByte(base, address + index, (byte) (value & 0xFF));
    }

    /**
     * Sets the specified 16-bit short integer at the specified absolute
     * {@code index} in this buffer.  The 16 high-order bits of the specified
     * value are ignored.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 2} is greater than {@code this.length()}
     */
    public void setShort(int index, int value)
    {
        checkIndexLength(index, SIZE_OF_SHORT);
        unsafe.putShort(base, address + index, (short) (value & 0xFFFF));
    }

    /**
     * Sets the specified 32-bit integer at the specified absolute
     * {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 4} is greater than {@code this.length()}
     */
    public void setInt(int index, int value)
    {
        checkIndexLength(index, SIZE_OF_INT);
        unsafe.putInt(base, address + index, value);
    }

    /**
     * Sets the specified 64-bit long integer at the specified absolute
     * {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 8} is greater than {@code this.length()}
     */
    public void setLong(int index, long value)
    {
        checkIndexLength(index, SIZE_OF_LONG);
        unsafe.putLong(base, address + index, value);
    }

    /**
     * Sets the specified 64-bit double at the specified absolute
     * {@code index} in this buffer.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0} or
     * {@code index + 8} is greater than {@code this.length()}
     */
    public void setDouble(int index, double value)
    {
        checkIndexLength(index, SIZE_OF_DOUBLE);
        unsafe.putDouble(base, address + index, value);
    }

    /**
     * Transfers data from the specified slice into this buffer starting at
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
    public void setBytes(int index, Slice source, int sourceIndex, int length)
    {
        checkIndexLength(index, length);
        checkPositionIndexes(sourceIndex, sourceIndex + length, source.length());

        copyMemory(source.base, source.address + sourceIndex, base, address + index, length);
    }

    /**
     * Transfers data from the specified array into this buffer starting at
     * the specified absolute {@code index}.
     *
     * @throws IndexOutOfBoundsException if the specified {@code index} is less than {@code 0},
     * if the specified {@code sourceIndex} is less than {@code 0},
     * if {@code index + length} is greater than
     * {@code this.length()}, or
     * if {@code sourceIndex + length} is greater than {@code source.length}
     */
    public void setBytes(int index, byte[] source, int sourceIndex, int length)
    {
        checkPositionIndexes(sourceIndex, sourceIndex + length, source.length);
        copyMemory(source, BYTE_ARRAY_OFFSET + sourceIndex, base, address + index, length);
    }

    /**
     * Transfers data from the specified input stream into this slice starting at
     * the specified absolute {@code index}.
     */
    public int setBytes(int index, InputStream in, int length)
            throws IOException
    {
        checkIndexLength(index, length);
        byte[] bytes = new byte[4096];
        int remaining = length;
        while (remaining > 0) {
            int bytesRead = in.read(bytes, 0, Math.min(bytes.length, remaining));
            if (bytesRead < 0) {
                // if we didn't read anything return -1
                if (remaining == length) {
                    return -1;
                }
                break;
            }
            copyMemory(bytes, BYTE_ARRAY_OFFSET, base, address + index, bytesRead);
            remaining -= bytesRead;
        }
        return length - remaining;
    }

    /**
     * Returns a slice of this buffer's sub-region. Modifying the content of
     * the returned buffer or this buffer affects each other's content.
     */
    public Slice slice(int index, int length)
    {
        if ((index == 0) && (length == length())) {
            return this;
        }
        checkIndexLength(index, length);
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        return new Slice(base, address + index, length, reference);
    }

    /**
     * Compares the content of the specified buffer to the content of this
     * buffer.  This comparison is performed byte by byte using an unsigned
     * comparison.
     */
    public int compareTo(Slice that)
    {
        if (this == that) {
            return 0;
        }

        return compareTo(0, size, that, 0, that.size);
    }

    /**
     * Compares a portion of this slice with a portion of the specified slice.  Equality is
     * solely based on the contents of the slice.
     */
    public int compareTo(int offset, int length, Slice that, int otherOffset, int otherLength)
    {
        if (this == that) {
            return 0;
        }

        while (length >= SIZE_OF_LONG) {
            long thisLong = unsafe.getLong(base, this.address + offset);
            thisLong = Long.reverseBytes(thisLong);
            long thatLong = unsafe.getLong(that.base, that.address + otherOffset);
            thatLong = Long.reverseBytes(thatLong);


            int v = UnsignedLongs.compare(thisLong, thatLong);
            if (v != 0) {
                return v;
            }

            offset += SIZE_OF_LONG;
            otherOffset += SIZE_OF_LONG;
            length -= SIZE_OF_LONG;
        }

        while (length > 0) {
            byte thisByte = unsafe.getByte(base, this.address + offset);
            byte thatByte = unsafe.getByte(that.base, that.address + otherOffset);

            int v = UnsignedBytes.compare(thisByte, thatByte);
            if (v != 0) {
                return v;
            }
            offset++;
            otherOffset++;
            length--;
        }

        return this.size - that.size;
    }

    /**
     * Compares the specified object with this slice for equality.  Equality is
     * solely based on the contents of the slice.
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Slice)) {
            return false;
        }

        Slice that = (Slice) o;
        if (length() != that.length()) {
            return false;
        }

        int offset = 0;
        int length = size;
        while (length >= SIZE_OF_LONG) {
            long thisLong = unsafe.getLong(base, this.address + offset);
            long thatLong = unsafe.getLong(that.base, that.address + offset);

            if (thisLong != thatLong) {
                return false;
            }

            offset += SIZE_OF_LONG;
            length -= SIZE_OF_LONG;
        }

        while (length > 0) {
            byte thisByte = unsafe.getByte(base, this.address + offset);
            byte thatByte = unsafe.getByte(that.base, that.address + offset);
            if (thisByte != thatByte) {
                return false;
            }
            offset++;
            length--;
        }

        return true;
    }

    /**
     * Returns the hash code of this slice.  The hash code is cached once calculated
     * and any future changes to the slice will not effect the hash code.
     */
    @Override
    public int hashCode()
    {
        if (hash != 0) {
            return hash;
        }

        hash = hashCode(0, size);
        return hash;
    }

    /**
     * Returns the hash code of a portion of this slice.
     */
    public int hashCode(int offset, int length)
    {
        checkIndexLength(offset, length);

        int seed = 0;
        int h1;
        int c1 = 0xcc9e2d51;
        int c2 = 0x1b873593;
        final int len = length;
        h1 = seed;


        while (length >= SIZE_OF_INT) {
            int k1 = unsafe.getInt(base, this.address + offset);

            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;

            offset += SIZE_OF_INT;
            length -= SIZE_OF_INT;
        }

        // process remaining bytes
        int k1 = 0;
        switch (length) {
            case 3:
                k1 ^= toInt(unsafe.getByte(base, this.address + offset + 2)) << 16;
                // fall through
            case 2:
                k1 ^= toInt(unsafe.getByte(base, this.address + offset+ 1)) << 8;
                // fall through
            case 1:
                k1 ^= toInt(unsafe.getByte(base, this.address + offset));
                // fall through
            default:
                k1 *= c1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= c2;
                h1 ^= k1;
        }

        // make hash
        h1 ^= len;

        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }

    /**
     * Compares a portion of this slice with a portion of the specified slice.  Equality is
     * solely based on the contents of the slice.
     */
    public boolean equals(int offset, int length, Slice that, int otherOffset, int otherLength)
    {
        if (length != otherLength) {
            return false;
        }

        while (length >= SIZE_OF_LONG) {
            long thisLong = unsafe.getLong(base, this.address + offset);
            long thatLong = unsafe.getLong(that.base, that.address + otherOffset);

            if (thisLong != thatLong) {
                return false;
            }

            offset += SIZE_OF_LONG;
            otherOffset += SIZE_OF_LONG;
            length -= SIZE_OF_LONG;
        }

        while (length > 0) {
            byte thisByte = unsafe.getByte(base, this.address + offset);
            byte thatByte = unsafe.getByte(that.base, that.address + otherOffset);
            if (thisByte != thatByte) {
                return false;
            }
            offset++;
            otherOffset++;
            length--;
        }

        return true;
    }

    /**
     * Creates a slice input backed by this slice.  Any changes to this slice
     * will be immediately visible to the slice input.
     */
    public BasicSliceInput getInput()
    {
        return new BasicSliceInput(this);
    }

    /**
     * Creates a slice output backed by this slice.  Any data written to the
     * slice output will be immediately visible in this slice.
     */
    public SliceOutput getOutput()
    {
        return new BasicSliceOutput(this);
    }

    /**
     * Decodes the contents of this slice into a string with the specified
     * character set name.
     */
    public String toString(Charset charset)
    {
        return toString(0, length(), charset);
    }

    /**
     * Decodes the a portion of this slice into a string with the specified
     * character set name.
     */
    public String toString(int index, int length, Charset charset)
    {
        if (length == 0) {
            return "";
        }
        if (base instanceof byte[]) {
            return new String((byte[]) base, (int) ((address - BYTE_ARRAY_OFFSET) + index), length, charset);
        }
        // direct memory can only be converted to a string using a ByteBuffer
        return Slices.decodeString(toByteBuffer(index, length), charset);
    }

    private ByteBuffer toByteBuffer(int index, int length)
    {
        if (base instanceof byte[]) {
            ByteBuffer buffer = ByteBuffer.wrap((byte[]) base, (int) ((address - BYTE_ARRAY_OFFSET) + index), length);
            return buffer;
        }
        checkIndexLength(index, length);
        try {
            return (ByteBuffer) newByteBuffer.invokeExact(address + index, length, (Object) reference);
        }
        catch (Throwable throwable) {
            throw Throwables.propagate(throwable);
        }
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("length", length())
                .toString();
    }

    private static void copyMemory(Object src, long srcAddress, Object dest, long destAddress, int length)
    {
        int offset = 0;
        while (length >= SIZE_OF_LONG) {
            long srcLong = unsafe.getLong(src, srcAddress + offset);
            unsafe.putLong(dest, destAddress + offset, srcLong);

            offset += SIZE_OF_LONG;
            length -= SIZE_OF_LONG;
        }

        while (length > 0) {
            byte srcLong = unsafe.getByte(src, srcAddress + offset);
            unsafe.putByte(dest, destAddress + offset, srcLong);

            offset++;
            length--;
        }
    }

    protected void checkIndexLength(int index, int length)
    {
        checkPositionIndexes(index, index + length, length());
    }
}
