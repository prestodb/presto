/**
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
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
package com.facebook.presto.slice;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.IdentityHashMap;
import java.util.Map;

public final class Slices
{
    public static Slice mapFileReadOnly(File file)
            throws IOException
    {
        return Slice.toUnsafeSlice(Files.map(file));
    }

    /**
     * A slice with size {@code 0}.
     */
    public static final Slice EMPTY_SLICE = new Slice();

    private Slices()
    {
    }

    private static final int SLICE_ALLOC_THRESHOLD = 524_288; // 2^19
    private static final double SLICE_ALLOW_SKEW = 1.25; // must be > 1!

    public static Slice ensureSize(Slice existingSlice, int minWritableBytes)
    {
        if (existingSlice == null) {
            return allocate(minWritableBytes);
        }

        if (minWritableBytes <= existingSlice.length()) {
            return existingSlice;
        }

        int newCapacity;
        if (existingSlice.length() == 0) {
            newCapacity = 1;
        }
        else {
            newCapacity = existingSlice.length();
        }
        int minNewCapacity = existingSlice.length() + minWritableBytes;
        while (newCapacity < minNewCapacity) {
            if (newCapacity < SLICE_ALLOC_THRESHOLD) {
                newCapacity <<= 1;
            }
            else {
                newCapacity *= SLICE_ALLOW_SKEW;
            }
        }

        Slice newSlice = Slices.allocate(newCapacity);
        newSlice.setBytes(0, existingSlice, 0, existingSlice.length());
        return newSlice;
    }

    public static Slice allocate(int capacity)
    {
        if (capacity == 0) {
            return EMPTY_SLICE;
        }
        return new Slice(new byte[capacity]);
    }

    public static Slice wrappedBuffer(byte[] array)
    {
        if (array.length == 0) {
            return EMPTY_SLICE;
        }
        return new Slice(array);
    }

    public static Slice copiedBuffer(String string, Charset charset)
    {
        Preconditions.checkNotNull(string, "string is null");
        Preconditions.checkNotNull(charset, "charset is null");

        return wrappedBuffer(string.getBytes(charset));
    }

    public static String decodeString(ByteBuffer src, Charset charset)
    {
        final CharsetDecoder decoder = getDecoder(charset);
        final CharBuffer dst = CharBuffer.allocate(
                (int) ((double) src.remaining() * decoder.maxCharsPerByte()));
        try {
            CoderResult cr = decoder.decode(src, dst, true);
            if (!cr.isUnderflow()) {
                cr.throwException();
            }
            cr = decoder.flush(dst);
            if (!cr.isUnderflow()) {
                cr.throwException();
            }
        }
        catch (CharacterCodingException x) {
            throw new IllegalStateException(x);
        }
        return dst.flip().toString();
    }

    private static final ThreadLocal<Map<Charset, CharsetDecoder>> decoders =
            new ThreadLocal<Map<Charset, CharsetDecoder>>()
            {
                @Override
                protected Map<Charset, CharsetDecoder> initialValue()
                {
                    return new IdentityHashMap<>();
                }
            };

    /**
     * Returns a cached thread-local {@link CharsetDecoder} for the specified
     * <tt>charset</tt>.
     */
    private static CharsetDecoder getDecoder(Charset charset)
    {
        if (charset == null) {
            throw new NullPointerException("charset");
        }

        Map<Charset, CharsetDecoder> map = decoders.get();
        CharsetDecoder d = map.get(charset);
        if (d != null) {
            d.reset();
            d.onMalformedInput(CodingErrorAction.REPLACE);
            d.onUnmappableCharacter(CodingErrorAction.REPLACE);
            return d;
        }

        d = charset.newDecoder();
        d.onMalformedInput(CodingErrorAction.REPLACE);
        d.onUnmappableCharacter(CodingErrorAction.REPLACE);
        map.put(charset, d);
        return d;
    }
}
