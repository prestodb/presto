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
package io.prestosql.orc.stream;

import java.io.IOException;
import java.io.InputStream;

public final class TestingBitPackingUtils
{
    private TestingBitPackingUtils() {}

    // Old implementation of bit unpacking code from Hive ORC reader
    public static void unpackGeneric(long[] buffer, int offset, int len, int bitSize, InputStream input)
            throws IOException
    {
        int bitsLeft = 0;
        int current = 0;

        for (int i = offset; i < (offset + len); i++) {
            long result = 0;
            int bitsLeftToRead = bitSize;
            while (bitsLeftToRead > bitsLeft) {
                result <<= bitsLeft;
                result |= current & ((1 << bitsLeft) - 1);
                bitsLeftToRead -= bitsLeft;
                current = input.read();
                bitsLeft = 8;
            }

            // handle the left over bits
            if (bitsLeftToRead > 0) {
                result <<= bitsLeftToRead;
                bitsLeft -= bitsLeftToRead;
                result |= (current >> bitsLeft) & ((1 << bitsLeftToRead) - 1);
            }
            buffer[i] = result;
        }
    }
}
