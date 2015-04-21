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
package com.teradata.presto.functions;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;

/**
 * Encodes each input character (2 bytes) into four bytes. Each byte
 * is an ASCII HEX code of 4 bits from input character.
 */
class HexEncoder extends CharsetEncoder
{
    protected HexEncoder()
    {
        super(StandardCharsets.US_ASCII, 4, 4);
    }

    @Override
    protected CoderResult encodeLoop(CharBuffer in, ByteBuffer out)
    {
        int position = in.position();
        for (; in.hasRemaining(); ++position) {
            char inputChar = in.get();

            if (out.remaining() < 4) {
                in.position(position);
                return CoderResult.OVERFLOW;
            }
            out.put(encodeToHex((inputChar >> 12)));
            out.put(encodeToHex((inputChar >> 8)));
            out.put(encodeToHex((inputChar >> 4)));
            out.put(encodeToHex(inputChar));
        }
        in.position(position);
        return CoderResult.UNDERFLOW;
    }

    /**
     * @param value
     * @return hex representation of argument's four last bits
     */
    private static byte encodeToHex(int value)
    {
        value &= 0xF;
        if (value > 9) {
            // handle letters
            return (byte) (value + 'A' - 10);
        }
        else {
            // handle digits
            return (byte) (value + '0');
        }
    }
}
