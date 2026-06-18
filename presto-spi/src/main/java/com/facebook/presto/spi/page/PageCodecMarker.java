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
package com.facebook.presto.spi.page;

import java.util.Arrays;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Encodes boolean properties for {@link SerializedPage} by using a bitmasking strategy, allowing
 * up to 8 such properties to be stored in a single byte
 */
public enum PageCodecMarker
{
    COMPRESSED(1),
    ENCRYPTED(2),
    CHECKSUMMED(3);

    private final int mask;

    PageCodecMarker(int bit)
    {
        checkArgument(bit > 0 && bit <= 8, "PageCodecMarker bit must be between 1 and 8. Found: %s", bit);
        this.mask = (1 << (bit - 1));
    }

    public boolean isSet(byte value)
    {
        return (Byte.toUnsignedInt(value) & mask) == mask;
    }

    public byte set(byte value)
    {
        return (byte) (Byte.toUnsignedInt(value) | mask);
    }

    public byte unset(byte value)
    {
        return (byte) (Byte.toUnsignedInt(value) & (~mask));
    }

    /**
     * The byte value of no {@link PageCodecMarker} values set to true
     */
    public static byte none()
    {
        return 0;
    }

    public static String toSummaryString(byte markers)
    {
        if (markers == none()) {
            return "NONE";
        }
        return Arrays.stream(values())
                .filter(marker -> marker.isSet(markers))
                .map(PageCodecMarker::name)
                .collect(Collectors.joining(", "));
    }

    private static void checkArgument(boolean condition, String message, Object... messageArgs)
    {
        if (!condition) {
            throw new IllegalArgumentException(format(message, messageArgs));
        }
    }
}
