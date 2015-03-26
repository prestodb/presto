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
package com.facebook.presto.orc.metadata;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.orc.metadata.OrcMetadataReader.concatSlices;
import static com.facebook.presto.orc.metadata.OrcMetadataReader.firstSurrogateCharacter;
import static com.facebook.presto.orc.metadata.OrcMetadataReader.getMaxSlice;
import static com.facebook.presto.orc.metadata.OrcMetadataReader.getMinSlice;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.MIN_CODE_POINT;
import static org.testng.Assert.assertEquals;

public class TestOrcMetadataReader
{
    @Test
    public void testGetMinSlice()
            throws Exception
    {
        int startCodePoint = MIN_CODE_POINT;
        int endCodePoint = MAX_CODE_POINT;
        Slice minSlice = Slices.utf8Slice("");

        for (int i = startCodePoint; i < endCodePoint; i++) {
            String value = new String(new int[] { i }, 0, 1);
            if (firstSurrogateCharacter(value) == -1) {
                assertEquals(getMinSlice(value), Slices.utf8Slice(value));
            }
            else {
                assertEquals(getMinSlice(value), minSlice);
            }
        }

        // Test with prefix
        String prefix = "apple";
        for (int i = startCodePoint; i < endCodePoint; i++) {
            String value = prefix + new String(new int[] { i }, 0, 1);
            if (firstSurrogateCharacter(value) == -1) {
                assertEquals(getMinSlice(value), Slices.utf8Slice(value));
            }
            else {
                assertEquals(getMinSlice(value), Slices.utf8Slice(prefix));
            }
        }
    }

    @Test
    public void testGetMaxSlice()
            throws Exception
    {
        int startCodePoint = MIN_CODE_POINT;
        int endCodePoint = MAX_CODE_POINT;
        Slice maxByte = Slices.wrappedBuffer(new byte[] { (byte) 0xFF });

        for (int i = startCodePoint; i < endCodePoint; i++) {
            String value = new String(new int[] { i }, 0, 1);
            if (firstSurrogateCharacter(value) == -1) {
                assertEquals(getMaxSlice(value), Slices.utf8Slice(value));
            }
            else {
                assertEquals(getMaxSlice(value), maxByte);
            }
        }

        // Test with prefix
        String prefix = "apple";
        Slice maxSlice = concatSlices(Slices.utf8Slice(prefix), maxByte);
        for (int i = startCodePoint; i < endCodePoint; i++) {
            String value = prefix + new String(new int[] { i }, 0, 1);
            if (firstSurrogateCharacter(value) == -1) {
                assertEquals(getMaxSlice(value), Slices.utf8Slice(value));
            }
            else {
                assertEquals(getMaxSlice(value), maxSlice);
            }
        }
    }
}
