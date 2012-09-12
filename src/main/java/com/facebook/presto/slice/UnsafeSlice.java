/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.slice;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import static com.facebook.presto.SizeOf.SIZE_OF_BYTE;
import static com.facebook.presto.SizeOf.SIZE_OF_DOUBLE;
import static com.facebook.presto.SizeOf.SIZE_OF_INT;
import static com.facebook.presto.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.SizeOf.SIZE_OF_SHORT;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static java.lang.Math.min;

public class UnsafeSlice extends AbstractSlice
{
    private static final Unsafe unsafe;
    private static final MethodHandle newByteBuffer;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);

            int byteArrayIndexScale = unsafe.arrayIndexScale(byte[].class);
            if (byteArrayIndexScale != 1) {
                throw new IllegalStateException("Byte array index scale must be 1, but is " + byteArrayIndexScale);
            }

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

    public static UnsafeSlice toUnsafeSlice(ByteBuffer byteBuffer)
    {
        Preconditions.checkNotNull(byteBuffer, "byteBuffer is null");
        Preconditions.checkArgument(byteBuffer instanceof DirectBuffer, "byteBuffer is not an instance of %s", DirectBuffer.class.getName());
        DirectBuffer directBuffer = (DirectBuffer) byteBuffer;
        long address = directBuffer.address();
        int capacity = byteBuffer.capacity();
        return new UnsafeSlice(address, capacity, byteBuffer);
    }

    private final long address;
    private final int size;
    private final Object reference;

    static {
        if (unsafe == null) {
            throw new RuntimeException("Unsafe access not available");
        }
    }

    UnsafeSlice(long address, int size, Object reference)
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
        this.address = address;
        this.size = size;
    }

    @Override
    public int length()
    {
        return size;
    }

    @Override
    public byte getByte(int index)
    {
        checkIndexLength(index, SIZE_OF_BYTE);
        return unsafe.getByte(address + index);
    }

    @Override
    public short getUnsignedByte(int index)
    {
        return (short) (getByte(index) & 0xFF);
    }

    @Override
    public short getShort(int index)
    {
        checkIndexLength(index, SIZE_OF_SHORT);
        return unsafe.getShort(address + index);
    }

    @Override
    public int getInt(int index)
    {
        checkIndexLength(index, SIZE_OF_INT);
        return unsafe.getInt(address + index);
    }

    @Override
    public long getLong(int index)
    {
        checkIndexLength(index, SIZE_OF_LONG);
        return unsafe.getLong(address + index);
    }

    @Override
    public double getDouble(int index)
    {
        checkIndexLength(index, SIZE_OF_DOUBLE);
        return unsafe.getDouble(address + index);
    }

    @Override
    public void getBytes(int index, Slice destination, int destinationIndex, int length)
    {
        destination.setBytes(destinationIndex, this, index, length);
    }

    @Override
    public void getBytes(int index, byte[] destination, int destinationIndex, int length)
    {
        checkIndexLength(index, length);
        checkPositionIndexes(destinationIndex, destinationIndex + length, destination.length);

        unsafe.copyMemory(null, address + index, destination, BYTE_ARRAY_OFFSET + destinationIndex, length);
    }

    @Override
    public byte[] getBytes(int index, int length)
    {
        byte[] bytes = new byte[length];
        getBytes(index, bytes, 0, length);
        return bytes;
    }

    @Override
    public void getBytes(int index, ByteBuffer destination)
    {
        int length = min(length() - index, destination.remaining());
        destination.put(toByteBuffer(index, length));
    }

    @Override
    public void getBytes(int index, SliceOutput out, int length)
    {
        out.writeBytes(toByteBuffer(index, length));
    }

    @Override
    public void getBytes(int index, OutputStream out, int length)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getBytes(int index, WritableByteChannel out, int length)
            throws IOException
    {
        return out.write(toByteBuffer(index, length));
    }

    @Override
    public void setByte(int index, int value)
    {
        checkIndexLength(index, SIZE_OF_BYTE);
        unsafe.putByte(address + index, (byte) (value & 0xFF));
    }

    @Override
    public void setShort(int index, int value)
    {
        checkIndexLength(index, SIZE_OF_SHORT);
        unsafe.putShort(address + index, (short) (value & 0xFFFF));
    }

    @Override
    public void setInt(int index, int value)
    {
        checkIndexLength(index, SIZE_OF_INT);
        unsafe.putInt(address + index, value);
    }

    @Override
    public void setLong(int index, long value)
    {
        checkIndexLength(index, SIZE_OF_LONG);
        unsafe.putLong(address + index, value);
    }

    @Override
    public void setDouble(int index, double value)
    {
        checkIndexLength(index, SIZE_OF_DOUBLE);
        unsafe.putDouble(address + index, value);
    }

    @Override
    public void setBytes(int index, Slice source, int sourceIndex, int length)
    {
        ByteBuffer src = source.toByteBuffer(sourceIndex, length);
        ByteBuffer dst = toByteBuffer(index, length);
        dst.put(src);
    }

    @Override
    public void setBytes(int index, byte[] source, int sourceIndex, int length)
    {
        checkPositionIndexes(sourceIndex, sourceIndex + length, source.length);
        toByteBuffer(index, length).put(source, sourceIndex, length);
    }

    @Override
    public void setBytes(int index, ByteBuffer source)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int setBytes(int index, InputStream in, int length)
            throws IOException
    {
        checkIndexLength(index, length);
        byte[] bytes = new byte[4096];
        ByteBuffer dst = toByteBuffer(index, length);
        while (dst.remaining() > 0) {
            int n = in.read(bytes);
            if (n < 0) {
                if (dst.position() == 0) {
                    return -1;
                }
                break;
            }
            dst.put(bytes, 0, n);
        }
        return dst.position();
    }

    @Override
    public int setBytes(int index, ReadableByteChannel in, int length)
            throws IOException
    {
        ByteBuffer dst = toByteBuffer(index, length);
        while (dst.remaining() > 0) {
            int n = channelRead(in, dst);
            if (n < 0) {
                if (dst.position() == 0) {
                    return -1;
                }
                break;
            }
            else if (n == 0) {
                break;
            }
        }
        return dst.position();
    }

    @Override
    public int setBytes(int index, FileChannel in, int position, int length)
            throws IOException
    {
        ByteBuffer dst = toByteBuffer(index, length);
        while (dst.remaining() > 0) {
            int n = channelRead(in, dst, position + dst.position());
            if (n < 0) {
                if (dst.position() == 0) {
                    return -1;
                }
                break;
            }
            else if (n == 0) {
                break;
            }
        }
        return dst.position();
    }

    @Override
    public Slice copySlice(int index, int length)
    {
        ByteBuffer src = toByteBuffer(index, length);
        ByteBuffer dst = allocateSame(src);
        dst.put(src);
        return new ByteBufferSlice(dst);
    }

    private static ByteBuffer allocateSame(ByteBuffer source)
    {
        if (source.isDirect()) {
            return ByteBuffer.allocateDirect(source.capacity());
        }
        return ByteBuffer.allocate(source.capacity());
    }

    @Override
    public Slice slice(int index, int length)
    {
        if ((index == 0) && (length == length())) {
            return this;
        }
        checkIndexLength(index, length);
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        return new UnsafeSlice(address + index, length, reference);
    }

    @Override
    public ByteBuffer toByteBuffer(int index, int length)
    {
        checkIndexLength(index, length);
        try {
            return (ByteBuffer) newByteBuffer.invokeExact(address + index, length, (Object) reference);
        }
        catch (Throwable throwable) {
            throw Throwables.propagate(throwable);
        }
    }

    @Override
    public int compareTo(Slice that)
    {
        if (this == that) {
            return 0;
        }
        return compareByteBuffers(toByteBuffer(), that.toByteBuffer());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Slice)) {
            return false;
        }

        Slice slice = (Slice) o;
        if (length() != slice.length()) {
            return false;
        }

        UnsafeSlice that = (UnsafeSlice) slice;
        int offset = 0;
        int length = size;
        while (length >= 8) {
            long thisLong = unsafe.getLong(this.address + offset);
            long thatLong = unsafe.getLong(that.address + offset);

            if (thisLong != thatLong) {
                return false;
            }

            offset += 8;
            length -= 8;
        }

        while (length > 0) {
            byte thisByte = unsafe.getByte(this.address + offset);
            byte thatByte = unsafe.getByte(that.address + offset);
            if (thisByte != thatByte) {
                return false;
            }
            offset++;
            length--;
        }

        return true;
    }

    private int hash;

    @Override
    public int hashCode()
    {
        if (hash != 0) {
            return hash;
        }

        // see definition in interface
        int result = 1;
        for (int i = 0; i < length(); i++) {
            result = (31 * result) + unsafe.getByte(this.address + i);
        }
        if (result == 0) {
            result = 1;
        }

        hash = result;
        return hash;
    }

    @Override
    public boolean equals(int offset, int length, Slice other, int otherOffset, int otherLength)
    {
        if (length != otherLength) {
            return false;
        }

        if (!(other instanceof UnsafeSlice)) {
            throw new UnsupportedOperationException("not yet implemented");
        }

        UnsafeSlice that = (UnsafeSlice) other;

        while (length >= 8) {
            long thisLong = unsafe.getLong(this.address + offset);
            long thatLong = unsafe.getLong(that.address + otherOffset);

            if (thisLong != thatLong) {
                return false;
            }

            offset += 8;
            otherOffset += 8;
            length -= 8;
        }

        while (length > 0) {
            byte thisByte = unsafe.getByte(this.address + offset);
            byte thatByte = unsafe.getByte(that.address + otherOffset);
            if (thisByte != thatByte) {
                return false;
            }
            offset++;
            otherOffset++;
            length--;
        }

        return true;
    }
}
