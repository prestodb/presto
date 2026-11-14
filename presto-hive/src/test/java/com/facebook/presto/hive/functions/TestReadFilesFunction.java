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
package com.facebook.presto.hive.functions;

import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestReadFilesFunction
{
    @Test
    public void testParseFormatAcceptsParquet()
    {
        assertEquals(ReadFilesFunction.parseFormat("PARQUET"), HiveStorageFormat.PARQUET);
        assertEquals(ReadFilesFunction.parseFormat("parquet"), HiveStorageFormat.PARQUET);
        assertEquals(ReadFilesFunction.parseFormat("Parquet"), HiveStorageFormat.PARQUET);
    }

    @Test
    public void testParseFormatRejectsAvro()
    {
        assertRejected("avro");
    }

    @Test
    public void testParseFormatRejectsCsv()
    {
        assertRejected("csv");
    }

    @Test
    public void testParseFormatRejectsJson()
    {
        assertRejected("json");
    }

    @Test
    public void testParseFormatRejectsOrc()
    {
        assertRejected("orc");
    }

    @Test
    public void testParseFormatRejectsDwrf()
    {
        assertRejected("dwrf");
    }

    @Test
    public void testParseFormatRejectsBogus()
    {
        assertRejected("foo");
    }

    private static void assertRejected(String formatString)
    {
        try {
            ReadFilesFunction.parseFormat(formatString);
            fail("Expected PrestoException for format: " + formatString);
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
            assertTrue(e.getMessage().toUpperCase().contains("PARQUET"),
                    "Expected error to mention PARQUET as the supported format, got: " + e.getMessage());
        }
    }
}
