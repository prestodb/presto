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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.Unit.PETABYTE;
import static io.airlift.units.DataSize.Unit.TERABYTE;
import static io.airlift.units.Duration.succinctDuration;
import static io.airlift.units.Duration.succinctNanos;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestUnitFunctions
        extends AbstractTestFunctions
{
    private void testDuration(String sqlExpr, Duration expected)
    {
        assertFunction(sqlExpr, VarcharType.VARCHAR, expected.toString());
    }

    @Test
    public void testSuccinctDuration()
    {
        testDuration("succinct_duration(123, 'ns')", succinctNanos(123));
        testDuration("succinct_duration(123.45, 'ns')", succinctDuration(123.45, NANOSECONDS));
        testDuration("succinct_duration(123.45, 'us')", succinctDuration(123.45, MICROSECONDS));
        testDuration("succinct_duration(123.45, 'ms')", succinctDuration(123.45, MILLISECONDS));
        testDuration("succinct_duration(123.45, 's')", succinctDuration(123.45, SECONDS));
        testDuration("succinct_duration(12.34, 'm')", succinctDuration(12.34, MINUTES));
        testDuration("succinct_duration(12.34, 'h')", succinctDuration(12.34, HOURS));
        testDuration("succinct_duration(12.34, 'd')", succinctDuration(12.34, DAYS));
        testDuration("succinct_duration(3600, 's')", new Duration(1.00, HOURS));

        testDuration("succinct_nanos(123)", succinctNanos(123));
        testDuration("succinct_nanos(123.4)", succinctDuration(123.4, NANOSECONDS));
        testDuration("succinct_nanos(123456)", new Duration(123.456, MICROSECONDS));
        testDuration("succinct_millis(123)", succinctDuration(123, MILLISECONDS));
        testDuration("succinct_millis(123.4)", succinctDuration(123.4, MILLISECONDS));
        testDuration("succinct_millis(1234)", new Duration(1.234, SECONDS));

        assertInvalidFunction("succinct_duration(-1, 'ns')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_duration(123, 'sec')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_nanos(-123)", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_millis(-123)", INVALID_FUNCTION_ARGUMENT);
    }

    private void testDataSize(String sqlExpr, DataSize expected)
    {
        assertFunction(sqlExpr, VarcharType.VARCHAR, expected.toString());
    }

    @Test
    public void testSuccinctDataSize()
    {
        testDataSize("succinct_data_size(123, 'B')", DataSize.succinctDataSize(123, BYTE));
        testDataSize("succinct_data_size(123, 'kB')", DataSize.succinctDataSize(123, KILOBYTE));
        testDataSize("succinct_data_size(123, 'MB')", DataSize.succinctDataSize(123, MEGABYTE));
        testDataSize("succinct_data_size(123, 'TB')", DataSize.succinctDataSize(123, TERABYTE));
        testDataSize("succinct_data_size(123, 'PB')", DataSize.succinctDataSize(123, PETABYTE));

        testDataSize("succinct_data_size(5.5 * 1024, 'kB')", DataSize.succinctDataSize(5.5, MEGABYTE));
        testDataSize("succinct_data_size(2048, 'MB')", new DataSize(2.00, GIGABYTE));

        testDataSize("succinct_bytes(1024 * 1024)", new DataSize(1.00, MEGABYTE));
        testDataSize("succinct_bytes(123)", DataSize.succinctBytes(123));
        testDataSize("succinct_bytes(123.45)", DataSize.succinctDataSize(123.45, BYTE));
        testDataSize("succinct_bytes(5.5 * 1024)", DataSize.succinctBytes((long) (5.5 * 1024)));
        testDataSize("succinct_bytes(12345)", DataSize.succinctBytes(12345));
        testDataSize("succinct_bytes(123456789)", DataSize.succinctBytes(123456789));
        testDataSize("succinct_bytes(1000000000)", DataSize.succinctBytes(1_000_000_000L));
        testDataSize("succinct_bytes(1000000000000)", DataSize.succinctBytes(1_000_000_000_000L));
        testDataSize("succinct_bytes(1000000000000000)", DataSize.succinctBytes(1_000_000_000_000_000L));
        testDataSize("succinct_bytes(1000000000000000000)", DataSize.succinctBytes(1_000_000_000_000_000_000L));

        assertInvalidFunction("succinct_data_size(123, 'xx')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_data_size(-1, 'B')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("succinct_bytes(-1)", INVALID_FUNCTION_ARGUMENT);
    }
}
