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
package com.facebook.presto.spi.type;

import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static org.testng.Assert.assertEquals;

public class VarcharsTest
{
    @Test
    public void testTruncateToLength()
            throws Exception
    {
        // Single byte code points
        assertEquals(truncateToLength(Slices.utf8Slice("abc"), 0), Slices.utf8Slice(""));
        assertEquals(truncateToLength(Slices.utf8Slice("abc"), 1), Slices.utf8Slice("a"));
        assertEquals(truncateToLength(Slices.utf8Slice("abc"), 4), Slices.utf8Slice("abc"));
        assertEquals(truncateToLength(Slices.utf8Slice("abcde"), 5), Slices.utf8Slice("abcde"));
        // 2 bytes code points
        assertEquals(truncateToLength(Slices.utf8Slice("абв"), 0), Slices.utf8Slice(""));
        assertEquals(truncateToLength(Slices.utf8Slice("абв"), 1), Slices.utf8Slice("а"));
        assertEquals(truncateToLength(Slices.utf8Slice("абв"), 4), Slices.utf8Slice("абв"));
        assertEquals(truncateToLength(Slices.utf8Slice("абвгд"), 5), Slices.utf8Slice("абвгд"));
        // 4 bytes code points
        assertEquals(truncateToLength(Slices.utf8Slice("\uD841\uDF0E\uD841\uDF31\uD841\uDF79\uD843\uDC53\uD843\uDC78"), 0),
                Slices.utf8Slice(""));
        assertEquals(truncateToLength(Slices.utf8Slice("\uD841\uDF0E\uD841\uDF31\uD841\uDF79\uD843\uDC53\uD843\uDC78"), 1),
                Slices.utf8Slice("\uD841\uDF0E"));
        assertEquals(truncateToLength(Slices.utf8Slice("\uD841\uDF0E\uD841\uDF31\uD841\uDF79"), 4),
                Slices.utf8Slice("\uD841\uDF0E\uD841\uDF31\uD841\uDF79"));
        assertEquals(truncateToLength(Slices.utf8Slice("\uD841\uDF0E\uD841\uDF31\uD841\uDF79\uD843\uDC53\uD843\uDC78"), 5),
                Slices.utf8Slice("\uD841\uDF0E\uD841\uDF31\uD841\uDF79\uD843\uDC53\uD843\uDC78"));

        assertEquals(truncateToLength(Slices.utf8Slice("abc"), createVarcharType(1)), Slices.utf8Slice("a"));
        assertEquals(truncateToLength(Slices.utf8Slice("abc"), (Type) createVarcharType(1)), Slices.utf8Slice("a"));
    }
}
