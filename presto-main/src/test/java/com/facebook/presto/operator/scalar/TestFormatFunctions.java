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

import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestFormatFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testFormatCount()
    {
        assertFunction("format_count(TINYINT -1)", VARCHAR, "-1");
        assertFunction("format_count(TINYINT 123)", VARCHAR, "123");

        assertFunction("format_count(SMALLINT -1)", VARCHAR, "-1");
        assertFunction("format_count(SMALLINT 123)", VARCHAR, "123");
        assertFunction("format_count(SMALLINT 1234)", VARCHAR, "1.23K");
        assertFunction("format_count(SMALLINT 12345)", VARCHAR, "12.3K");

        assertFunction("format_count(-1)", VARCHAR, "-1");

        assertFunction("format_count(-1)", VARCHAR, "-1");
        assertFunction("format_count(123)", VARCHAR, "123");
        assertFunction("format_count(1234)", VARCHAR, "1.23K");
        assertFunction("format_count(12345)", VARCHAR, "12.3K");
        assertFunction("format_count(123456)", VARCHAR, "123K");
        assertFunction("format_count(1234567)", VARCHAR, "1.23M");
        assertFunction("format_count(12345678)", VARCHAR, "12.3M");
        assertFunction("format_count(123456789)", VARCHAR, "123M");
        assertFunction("format_count(1234567890)", VARCHAR, "1.23B");

        assertFunction("format_count(BIGINT -1)", VARCHAR, "-1");
        assertFunction("format_count(BIGINT 123)", VARCHAR, "123");
        assertFunction("format_count(BIGINT 78900)", VARCHAR, "78.9K");
        assertFunction("format_count(BIGINT 12345678901)", VARCHAR, "12.3B");
        assertFunction("format_count(BIGINT 123456789012)", VARCHAR, "123B");
        assertFunction("format_count(BIGINT 1234567890123)", VARCHAR, "1.23T");
        assertFunction("format_count(BIGINT 1234567890123456)", VARCHAR, "1.23Q");
        assertFunction("format_count(BIGINT 1234567890123456789)", VARCHAR, "1235Q");

        assertFunction("format_count(12300000)", VARCHAR, "12.3M");
        assertFunction("format_count(12300000000)", VARCHAR, "12.3B");

        assertFunction("format_count(DOUBLE 123.0)", VARCHAR, "123");
        assertFunction("format_count(DOUBLE 12345.0)", VARCHAR, "12.3K");
        assertFunction("format_count(DOUBLE 1234567.0)", VARCHAR, "1.23M");

        assertFunction("format_count(REAL 123.0)", VARCHAR, "123");
        assertFunction("format_count(REAL 12345.0)", VARCHAR, "12.3K");
        assertFunction("format_count(REAL 1234567.0)", VARCHAR, "1.23M");

        assertFunction("format_count(DECIMAL 123.0)", VARCHAR, "123");
        assertFunction("format_count(DECIMAL 12345.0)", VARCHAR, "12.3K");
        assertFunction("format_count(DECIMAL 1234567.0)", VARCHAR, "1.23M");

        assertFunction("format_count(CAST(NULL AS TINYINT))", VARCHAR, null);
        assertFunction("format_count(CAST(NULL AS SMALLINT))", VARCHAR, null);
        assertFunction("format_count(CAST(NULL AS INTEGER))", VARCHAR, null);
        assertFunction("format_count(CAST(NULL AS DOUBLE))", VARCHAR, null);
        assertFunction("format_count(CAST(NULL AS REAL))", VARCHAR, null);
    }
}
