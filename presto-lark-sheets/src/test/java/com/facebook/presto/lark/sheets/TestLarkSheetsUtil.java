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

package com.facebook.presto.lark.sheets;

import com.facebook.airlift.json.JsonCodec;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.facebook.presto.lark.sheets.LarkSheetsUtil.RADIX;
import static com.facebook.presto.lark.sheets.LarkSheetsUtil.columnIndexToColumnLabel;
import static com.facebook.presto.lark.sheets.LarkSheetsUtil.columnLabelToColumnIndex;
import static com.facebook.presto.lark.sheets.LarkSheetsUtil.mask;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public class TestLarkSheetsUtil
{
    @Test
    public void testLoadAppSecret()
    {
        final String appSecret = "faking_app_secret";

        // create a secret file
        final Path secretFilePath;
        try {
            secretFilePath = Files.createTempFile("app-secret-", ".json");
            JsonCodec<Map<String, String>> jsonCodec = JsonCodec.mapJsonCodec(String.class, String.class);
            byte[] bytes = jsonCodec.toBytes(ImmutableMap.of("app-secret", appSecret));
            Files.write(secretFilePath, bytes);
        }
        catch (IOException e) {
            fail("Cannot create a secret file", e);
            throw new UncheckedIOException(e);
        }

        // test loadAppSecret
        String loadedAppSecret = LarkSheetsUtil.loadAppSecret(secretFilePath.toString());
        assertEquals(loadedAppSecret, appSecret);
    }

    @Test
    public void testMask()
    {
        assertNull(mask(null));
        assertEquals("", mask(""));
        assertEquals("123", mask("123"));
        assertEquals("123456", mask("123456"));
        assertEquals("****567890", mask("1234567890"));
    }

    @Test
    public void testColumnLabelToColumnIndex()
    {
        assertEquals(columnLabelToColumnIndex("A"), 0);
        assertEquals(columnLabelToColumnIndex("B"), 1);
        assertEquals(columnLabelToColumnIndex("Z"), RADIX - 1);
        assertEquals(columnLabelToColumnIndex("AA"), RADIX);
        assertEquals(columnLabelToColumnIndex("AB"), RADIX + 1);
        assertEquals(columnLabelToColumnIndex("AZ"), RADIX + RADIX - 1);
        assertEquals(columnLabelToColumnIndex("BA"), RADIX + RADIX);
        assertEquals(columnLabelToColumnIndex("ZZ"), RADIX + RADIX * RADIX - 1);
        assertEquals(columnLabelToColumnIndex("AAA"), RADIX + RADIX * RADIX);
        assertEquals(columnLabelToColumnIndex("AAB"), RADIX + RADIX * RADIX + 1);
        assertEquals(columnLabelToColumnIndex("ZZZ"), RADIX + RADIX * RADIX + RADIX * RADIX * RADIX - 1);
        assertEquals(columnLabelToColumnIndex("AAAA"), RADIX + RADIX * RADIX + RADIX * RADIX * RADIX);
        assertEquals(columnLabelToColumnIndex("AAAB"), RADIX + RADIX * RADIX + RADIX * RADIX * RADIX + 1);
    }

    @Test
    public void testColumnIndexToColumnLabel()
    {
        assertEquals(columnIndexToColumnLabel(0), "A");
        assertEquals(columnIndexToColumnLabel(1), "B");
        assertEquals(columnIndexToColumnLabel(RADIX - 1), "Z");
        assertEquals(columnIndexToColumnLabel(RADIX), "AA");
        assertEquals(columnIndexToColumnLabel(RADIX + 1), "AB");
        assertEquals(columnIndexToColumnLabel(RADIX + RADIX - 1), "AZ");
        assertEquals(columnIndexToColumnLabel(RADIX + RADIX), "BA");
        assertEquals(columnIndexToColumnLabel(RADIX + RADIX + 1), "BB");
        assertEquals(columnIndexToColumnLabel(RADIX * RADIX + RADIX - 1), "ZZ");
        assertEquals(columnIndexToColumnLabel(RADIX * RADIX + RADIX), "AAA");
        assertEquals(columnIndexToColumnLabel(RADIX * RADIX + RADIX + 1), "AAB");
        assertEquals(columnIndexToColumnLabel(RADIX * RADIX * RADIX + RADIX * RADIX + RADIX - 1), "ZZZ");
        assertEquals(columnIndexToColumnLabel(RADIX * RADIX * RADIX + RADIX * RADIX + RADIX), "AAAA");
        assertEquals(columnIndexToColumnLabel(RADIX * RADIX * RADIX + RADIX * RADIX + RADIX + 1), "AAAB");
    }

    @Test
    public void testColumnIndexLabelRoundTrip()
    {
        for (int i = 0; i < 65535; i++) {
            assertEquals(columnLabelToColumnIndex(columnIndexToColumnLabel(i)), i);
        }
    }
}
