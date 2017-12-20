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

import com.facebook.presto.spi.type.VarcharType;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class TestUnitFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testSuccinctDuration()
    {
        assertFunction("succinct_duration(123, 'ns')", VarcharType.VARCHAR, "123.00ns");
        assertFunction("succinct_duration(123.45, 'ns')", VarcharType.VARCHAR, "123.45ns");
        assertFunction("succinct_duration(123.45, 'us')", VarcharType.VARCHAR, "123.45us");
        assertFunction("succinct_duration(123.45, 'ms')", VarcharType.VARCHAR, "123.45ms");
        assertFunction("succinct_duration(123.45, 's')", VarcharType.VARCHAR, "2.06m");
        assertFunction("succinct_duration(12.34, 'm')", VarcharType.VARCHAR, "12.34m");
        assertFunction("succinct_duration(12.34, 'h')", VarcharType.VARCHAR, "12.34h");
        assertFunction("succinct_duration(12.34, 'd')", VarcharType.VARCHAR, "12.34d");
        assertFunction("succinct_duration(3600, 's')", VarcharType.VARCHAR, "1.00h");

        assertFunction("succinct_nanos(123)", VarcharType.VARCHAR, "123.00ns");
        assertFunction("succinct_nanos(123.4)", VarcharType.VARCHAR, "123.40ns");
        assertFunction("succinct_nanos(123456)", VarcharType.VARCHAR, "123.46us");

        assertFunction("succinct_millis(123)", VarcharType.VARCHAR, "123.00ms");
        assertFunction("succinct_millis(123.4)", VarcharType.VARCHAR, "123.40ms");
        assertFunction("succinct_millis(1234)", VarcharType.VARCHAR, "1.23s");

        assertInvalidFunction("succinct_duration(-1, 'ns')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_duration(123, 'sec')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_nanos(-123)", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_millis(-123)", INVALID_FUNCTION_ARGUMENT);

        assertInvalidFunction("succinct_duration(-1.2, 'ns')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_duration(123.45, 'sec')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_nanos(-123.4)", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_millis(-123.4)", INVALID_FUNCTION_ARGUMENT);

        assertInvalidFunction("succinct_duration(nan(), 'ns')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_duration(infinity(), 'ns')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_duration(-infinity(), 'ns')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_nanos(nan())", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_nanos(infinity())", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_nanos(-infinity())", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_millis(nan())", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_millis(infinity())", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_millis(-infinity())", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testSuccinctDataSize()
    {
        assertFunction("succinct_data_size(123, 'B')", VarcharType.VARCHAR, "123B");
        assertFunction("succinct_data_size(123, 'kB')", VarcharType.VARCHAR, "123kB");
        assertFunction("succinct_data_size(123, 'MB')", VarcharType.VARCHAR, "123MB");
        assertFunction("succinct_data_size(123, 'TB')", VarcharType.VARCHAR, "123TB");
        assertFunction("succinct_data_size(123, 'PB')", VarcharType.VARCHAR, "123PB");

        assertFunction("succinct_data_size(5.5 * 1024, 'kB')", VarcharType.VARCHAR, "5.50MB");
        assertFunction("succinct_data_size(2048, 'MB')", VarcharType.VARCHAR, "2GB");

        assertFunction("succinct_bytes(1024 * 1024)", VarcharType.VARCHAR, "1MB");
        assertFunction("succinct_bytes(123)", VarcharType.VARCHAR, "123B");
        assertFunction("succinct_bytes(123.45)", VarcharType.VARCHAR, "123.45B");
        assertFunction("succinct_bytes(5.5 * 1024)", VarcharType.VARCHAR, "5.50kB");
        assertFunction("succinct_bytes(12345)", VarcharType.VARCHAR, "12.06kB");
        assertFunction("succinct_bytes(123456789)", VarcharType.VARCHAR, "117.74MB");
        assertFunction("succinct_bytes(1000000000)", VarcharType.VARCHAR, "953.67MB");
        assertFunction("succinct_bytes(1000000000000)", VarcharType.VARCHAR, "931.32GB");
        assertFunction("succinct_bytes(1000000000000000)", VarcharType.VARCHAR, "909.49TB");
        assertFunction("succinct_bytes(1000000000000000000)", VarcharType.VARCHAR, "888.18PB");

        assertInvalidFunction("succinct_data_size(123, 'xx')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_data_size(-1, 'B')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_bytes(-1)", INVALID_FUNCTION_ARGUMENT);
    }
}
