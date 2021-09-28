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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.SpookyHashV2;
import io.airlift.slice.XxHash64;

import java.util.Base64;
import java.util.zip.CRC32;

import static com.facebook.presto.operator.scalar.HmacFunctions.computeHash;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;
import static io.airlift.slice.Slices.EMPTY_SLICE;

public final class VarbinaryFunctions
{
    private VarbinaryFunctions() {}

    @Description("length of the given binary")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long length(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return slice.length();
    }

    @Description("encode binary data as base64")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice toBase64(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        if (slice.hasByteArray()) {
            return Slices.wrappedBuffer(Base64.getEncoder().encode(slice.toByteBuffer()));
        }
        return Slices.wrappedBuffer(Base64.getEncoder().encode(slice.getBytes()));
    }

    @Description("decode base64 encoded binary data")
    @ScalarFunction("from_base64")
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice fromBase64Varchar(@SqlType("varchar(x)") Slice slice)
    {
        try {
            if (slice.hasByteArray()) {
                return Slices.wrappedBuffer(Base64.getDecoder().decode(slice.toByteBuffer()));
            }
            return Slices.wrappedBuffer(Base64.getDecoder().decode(slice.getBytes()));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Description("decode base64 encoded binary data")
    @ScalarFunction("from_base64")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice fromBase64Varbinary(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        try {
            if (slice.hasByteArray()) {
                return Slices.wrappedBuffer(Base64.getDecoder().decode(slice.toByteBuffer()));
            }
            return Slices.wrappedBuffer(Base64.getDecoder().decode(slice.getBytes()));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Description("encode binary data as base64 using the URL safe alphabet")
    @ScalarFunction("to_base64url")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice toBase64Url(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return Slices.wrappedBuffer(Base64.getUrlEncoder().encode(slice.toByteBuffer()));
    }

    @Description("decode URL safe base64 encoded binary data")
    @ScalarFunction("from_base64url")
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice fromBase64UrlVarchar(@SqlType("varchar(x)") Slice slice)
    {
        try {
            if (slice.hasByteArray()) {
                return Slices.wrappedBuffer(Base64.getUrlDecoder().decode(slice.toByteBuffer()));
            }
            return Slices.wrappedBuffer(Base64.getUrlDecoder().decode(slice.getBytes()));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Description("decode URL safe base64 encoded binary data")
    @ScalarFunction("from_base64url")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice fromBase64UrlVarbinary(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        try {
            if (slice.hasByteArray()) {
                return Slices.wrappedBuffer(Base64.getUrlDecoder().decode(slice.toByteBuffer()));
            }
            return Slices.wrappedBuffer(Base64.getUrlDecoder().decode(slice.getBytes()));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Description("encode binary data as hex")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice toHex(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        String encoded;
        if (slice.hasByteArray()) {
            encoded = BaseEncoding.base16().encode(slice.byteArray(), slice.byteArrayOffset(), slice.length());
        }
        else {
            encoded = BaseEncoding.base16().encode(slice.getBytes());
        }
        return Slices.utf8Slice(encoded);
    }

    @Description("decode hex encoded binary data")
    @ScalarFunction("from_hex")
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice fromHexVarchar(@SqlType("varchar(x)") Slice slice)
    {
        if (slice.length() % 2 != 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "invalid input length " + slice.length());
        }

        byte[] result = new byte[slice.length() / 2];
        for (int i = 0; i < slice.length(); i += 2) {
            result[i / 2] = (byte) ((hexDigitCharToInt(slice.getByte(i)) << 4) | hexDigitCharToInt(slice.getByte(i + 1)));
        }
        return Slices.wrappedBuffer(result);
    }

    @Description("encode value as a 64-bit 2's complement big endian varbinary")
    @ScalarFunction("to_big_endian_64")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice toBigEndian64(@SqlType(StandardTypes.BIGINT) long value)
    {
        Slice slice = Slices.allocate(Long.BYTES);
        slice.setLong(0, Long.reverseBytes(value));
        return slice;
    }

    @Description("decode bigint value from a 64-bit 2's complement big endian varbinary")
    @ScalarFunction("from_big_endian_64")
    @SqlType(StandardTypes.BIGINT)
    public static long fromBigEndian64(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        if (slice.length() != Long.BYTES) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "expected 8-byte input, but got instead: " + slice.length());
        }
        return Long.reverseBytes(slice.getLong(0));
    }

    @Description("encode value as a 32-bit 2's complement big endian varbinary")
    @ScalarFunction("to_big_endian_32")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice toBigEndian32(@SqlType(StandardTypes.INTEGER) long value)
    {
        Slice slice = Slices.allocate(Integer.BYTES);
        slice.setInt(0, Integer.reverseBytes((int) value));
        return slice;
    }

    @Description("decode bigint value from a 32-bit 2's complement big endian varbinary")
    @ScalarFunction("from_big_endian_32")
    @SqlType(StandardTypes.INTEGER)
    public static long fromBigEndian32(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        if (slice.length() != Integer.BYTES) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "expected 4-byte input, but got instead: " + slice.length());
        }
        return Integer.reverseBytes(slice.getInt(0));
    }

    @Description("encode value as a big endian varbinary according to IEEE 754 single-precision floating-point format")
    @ScalarFunction("to_ieee754_32")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice toIEEE754Binary32(@SqlType(StandardTypes.REAL) long value)
    {
        Slice slice = Slices.allocate(Float.BYTES);
        slice.setInt(0, Integer.reverseBytes((int) value));
        return slice;
    }

    @Description("decode the 32-bit big-endian binary in IEEE 754 single-precision floating-point format")
    @ScalarFunction("from_ieee754_32")
    @SqlType(StandardTypes.REAL)
    public static long fromIEEE754Binary32(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        checkCondition(slice.length() == Integer.BYTES, INVALID_FUNCTION_ARGUMENT, "Input floating-point value must be exactly 4 bytes long");
        return Integer.reverseBytes(slice.getInt(0));
    }

    @Description("encode value as a big endian varbinary according to IEEE 754 double-precision floating-point format")
    @ScalarFunction("to_ieee754_64")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice toIEEE754Binary64(@SqlType(StandardTypes.DOUBLE) double value)
    {
        Slice slice = Slices.allocate(Double.BYTES);
        slice.setLong(0, Long.reverseBytes(Double.doubleToLongBits(value)));
        return slice;
    }

    @Description("decode the 64-bit big-endian binary in IEEE 754 double-precision floating-point format")
    @ScalarFunction("from_ieee754_64")
    @SqlType(StandardTypes.DOUBLE)
    public static double fromIEEE754Binary64(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        checkCondition(slice.length() == Double.BYTES, INVALID_FUNCTION_ARGUMENT, "Input floating-point value must be exactly 8 bytes long");
        return Double.longBitsToDouble(Long.reverseBytes(slice.getLong(0)));
    }

    @Description("compute md5 hash")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice md5(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return computeHash(Hashing.md5(), slice);
    }

    @Description("compute a hash equivalent to MurmurHash3_x64_128 (Murmur3F) in C++")
    @ScalarFunction("murmur3_x64_128")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice murmur3X64128(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return computeHash(Hashing.murmur3_128(), slice);
    }

    @Description("compute sha1 hash")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice sha1(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return computeHash(Hashing.sha1(), slice);
    }

    @Description("compute sha256 hash")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice sha256(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return computeHash(Hashing.sha256(), slice);
    }

    @Description("compute sha512 hash")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice sha512(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return computeHash(Hashing.sha512(), slice);
    }

    private static int hexDigitCharToInt(byte b)
    {
        if (b >= '0' && b <= '9') {
            return b - '0';
        }
        else if (b >= 'a' && b <= 'f') {
            return b - 'a' + 10;
        }
        else if (b >= 'A' && b <= 'F') {
            return b - 'A' + 10;
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "invalid hex character: " + (char) b);
    }

    @Description("compute xxhash64 hash")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice xxhash64(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        Slice hash = Slices.allocate(Long.BYTES);
        hash.setLong(0, Long.reverseBytes(XxHash64.hash(slice)));
        return hash;
    }

    @Description("compute SpookyHashV2 32-bit hash")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice spookyHashV2_32(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        Slice hash = Slices.allocate(Integer.BYTES);
        hash.setInt(0, Integer.reverseBytes(SpookyHashV2.hash32(slice, 0, slice.length(), 0)));
        return hash;
    }

    @Description("compute SpookyHashV2 64-bit hash")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice spookyHashV2_64(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        Slice hash = Slices.allocate(Long.BYTES);
        hash.setLong(0, Long.reverseBytes(SpookyHashV2.hash64(slice, 0, slice.length(), 0)));
        return hash;
    }

    @Description("decode hex encoded binary data")
    @ScalarFunction("from_hex")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice fromHexVarbinary(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return fromHexVarchar(slice);
    }

    @Description("compute CRC-32")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long crc32(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        CRC32 crc32 = new CRC32();
        crc32.update(slice.toByteBuffer());
        return crc32.getValue();
    }

    @Description("compute fnv-1 32 bit")
    @ScalarFunction("fnv1_32")
    @SqlType(StandardTypes.BIGINT)
    public static long fnv1Hash32(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return FnvHash.fnv1Hash32(slice);
    }

    @Description("compute fnv-1 64 bit")
    @ScalarFunction("fnv1_64")
    @SqlType(StandardTypes.BIGINT)
    public static long fnv1Hash64(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return FnvHash.fnv1Hash64(slice);
    }

    @Description("compute fnv-1a 32 bit")
    @ScalarFunction("fnv1a_32")
    @SqlType(StandardTypes.BIGINT)
    public static long fnv1aHash32(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return FnvHash.fnv1aHash32(slice);
    }

    @Description("compute fnv-1a 64 bit")
    @ScalarFunction("fnv1a_64")
    @SqlType(StandardTypes.BIGINT)
    public static long fnv1aHash64(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return FnvHash.fnv1aHash64(slice);
    }

    @Description("suffix starting at given index")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice substr(@SqlType(StandardTypes.VARBINARY) Slice slice, @SqlType(StandardTypes.BIGINT) long start)
    {
        return substr(slice, start, slice.length() - start + 1);
    }

    @Description("substring of given length starting at an index")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice substr(@SqlType(StandardTypes.VARBINARY) Slice slice, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long length)
    {
        if (start == 0 || length <= 0 || slice.length() == 0) {
            return EMPTY_SLICE;
        }

        int startByte = Ints.saturatedCast(start);
        int byteLength = Ints.saturatedCast(length);

        if (startByte > 0) {
            int indexStart = startByte - 1; // index starts with 1.
            if (indexStart >= slice.length()) {
                return EMPTY_SLICE;
            }
            int indexEnd = indexStart + byteLength;
            if (indexEnd > slice.length()) {
                indexEnd = slice.length();
            }
            return slice.slice(indexStart, indexEnd - indexStart);
        }

        // negative start is relative to end of string
        startByte += slice.length();

        // before beginning of string
        if (startByte < 0) {
            return EMPTY_SLICE;
        }

        int indexStart = startByte;
        int indexEnd = indexStart + byteLength;
        if (indexEnd > slice.length()) {
            indexEnd = slice.length();
        }

        return slice.slice(indexStart, indexEnd - indexStart);
    }

    private static Slice pad(Slice inputSlice, long targetLength, Slice padSlice, int paddingOffset)
    {
        checkCondition(
                0 <= targetLength && targetLength <= Integer.MAX_VALUE,
                INVALID_FUNCTION_ARGUMENT,
                "Target length must be in the range [0.." + Integer.MAX_VALUE + "]");
        checkCondition(padSlice.length() > 0, INVALID_FUNCTION_ARGUMENT, "Padding bytes must not be empty");

        int inputLength = inputSlice.length();
        int resultLength = (int) targetLength;

        // if our target length is the same as our string then return our string
        if (inputLength == resultLength) {
            return inputSlice;
        }

        // if our string is bigger than requested then truncate
        if (inputLength > resultLength) {
            return inputSlice.slice(0, resultLength);
        }

        // preallocate the result
        Slice buffer = Slices.allocate(resultLength);

        // fill in the existing string
        int fillLength = resultLength - inputLength;
        int startPointOfExistingText = (paddingOffset + fillLength) % resultLength;
        buffer.setBytes(startPointOfExistingText, inputSlice);

        // assign the pad string while there's enough space for it
        int byteIndex = paddingOffset;
        for (int i = 0; i < fillLength / padSlice.length(); i++) {
            buffer.setBytes(byteIndex, padSlice);
            byteIndex += padSlice.length();
        }

        // handle the tail: at most we assign padStringLength - 1 code points
        buffer.setBytes(byteIndex, padSlice.getBytes(0, paddingOffset + fillLength - byteIndex));

        return buffer;
    }

    @Description("pads a varbinary on the left")
    @ScalarFunction("lpad")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice leftPad(@SqlType("varbinary") Slice inputSlice, @SqlType(StandardTypes.BIGINT) long targetLength, @SqlType("varbinary") Slice padBytes)
    {
        return pad(inputSlice, targetLength, padBytes, 0);
    }

    @Description("pads a varbinary on the right")
    @ScalarFunction("rpad")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice rightPad(@SqlType("varbinary") Slice inputSlice, @SqlType(StandardTypes.BIGINT) long targetLength, @SqlType("varbinary") Slice padBytes)
    {
        return pad(inputSlice, targetLength, padBytes, inputSlice.length());
    }

    @Description("Reverse a given varbinary")
    @ScalarFunction("reverse")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice reverse(@SqlType("varbinary") Slice inputSlice)
    {
        if (inputSlice.length() == 0) {
            return EMPTY_SLICE;
        }
        int length = inputSlice.length();
        Slice reverse = Slices.allocate(length);
        for (int i = 0; i < length; i++) {
            reverse.setByte(i, inputSlice.getByte((length - 1) - i));
        }
        return reverse;
    }
}
