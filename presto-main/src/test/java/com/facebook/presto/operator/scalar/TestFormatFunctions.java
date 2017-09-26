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
        assertFunction("formatCount(TINYINT '-1')", VARCHAR, "-1");
        assertFunction("formatCount(TINYINT '123')", VARCHAR, "123");

        assertFunction("formatCount(SMALLINT '-1')", VARCHAR, "-1");
        assertFunction("formatCount(SMALLINT '123')", VARCHAR, "123");
        assertFunction("formatCount(SMALLINT '1234')", VARCHAR, "1.23K");
        assertFunction("formatCount(SMALLINT '12345')", VARCHAR, "12.3K");

        assertFunction("formatCount(-1)", VARCHAR, "-1");
        assertFunction("formatCount(123)", VARCHAR, "123");
        assertFunction("formatCount(1234)", VARCHAR, "1.23K");
        assertFunction("formatCount(12345)", VARCHAR, "12.3K");
        assertFunction("formatCount(123456)", VARCHAR, "123K");
        assertFunction("formatCount(1234567)", VARCHAR, "1.23M");
        assertFunction("formatCount(12345678)", VARCHAR, "12.3M");
        assertFunction("formatCount(123456789)", VARCHAR, "123M");
        assertFunction("formatCount(1234567890)", VARCHAR, "1.23B");

        assertFunction("formatCount(BIGINT '-1')", VARCHAR, "-1");
        assertFunction("formatCount(BIGINT '123')", VARCHAR, "123");
        assertFunction("formatCount(BIGINT '78900')", VARCHAR, "78.9K");
        assertFunction("formatCount(BIGINT '12345678901')", VARCHAR, "12.3B");
        assertFunction("formatCount(BIGINT '123456789012')", VARCHAR, "123B");
        assertFunction("formatCount(BIGINT '1234567890123')", VARCHAR, "1.23T");
        assertFunction("formatCount(BIGINT '1234567890123456')", VARCHAR, "1.23Q");
        assertFunction("formatCount(BIGINT '1234567890123456789')", VARCHAR, "1235Q");

        assertFunction("formatCount(12300000)", VARCHAR, "12.3M");
        assertFunction("formatCount(12300000000)", VARCHAR, "12.3B");

        assertFunction("formatCount(DOUBLE '123.0')", VARCHAR, "123");
        assertFunction("formatCount(DOUBLE '12345.0')", VARCHAR, "12.3K");
        assertFunction("formatCount(DOUBLE '1234567.0')", VARCHAR, "1.23M");

        assertFunction("formatCount(REAL '123.0')", VARCHAR, "123");
        assertFunction("formatCount(REAL '12345.0')", VARCHAR, "12.3K");
        assertFunction("formatCount(REAL '1234567.0')", VARCHAR, "1.23M");

        assertFunction("formatCount(DECIMAL '123.0')", VARCHAR, "123");
        assertFunction("formatCount(DECIMAL '12345.0')", VARCHAR, "12.3K");
        assertFunction("formatCount(DECIMAL '1234567.0')", VARCHAR, "1.23M");

        assertFunction("formatCount(CAST(NULL AS TINYINT))", VARCHAR, null);
        assertFunction("formatCount(CAST(NULL AS SMALLINT))", VARCHAR, null);
        assertFunction("formatCount(CAST(NULL AS INTEGER))", VARCHAR, null);
        assertFunction("formatCount(CAST(NULL AS DOUBLE))", VARCHAR, null);
        assertFunction("formatCount(CAST(NULL AS REAL))", VARCHAR, null);
    }

// TODO: test  ARRAY[1, 2, 3]
//    https://reviewable.io/reviews/prestodb/presto/6437#-KXyDY9RJQ5VpXEJezqD-r1-68
//    https://prestodb.io/docs/current/language/types.html
}
