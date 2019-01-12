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
package io.prestosql.operator.scalar;

import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;

public class TestDataSizeFunctions
        extends AbstractTestFunctions
{
    private static final Type DECIMAL = createDecimalType(38, 0);

    @Test
    public void testParseDataSize()
    {
        assertFunction("parse_presto_data_size('0B')", DECIMAL, decimal("0"));
        assertFunction("parse_presto_data_size('1B')", DECIMAL, decimal("1"));
        assertFunction("parse_presto_data_size('1.2B')", DECIMAL, decimal("1"));
        assertFunction("parse_presto_data_size('1.9B')", DECIMAL, decimal("1"));
        assertFunction("parse_presto_data_size('2.2kB')", DECIMAL, decimal("2252"));
        assertFunction("parse_presto_data_size('2.23kB')", DECIMAL, decimal("2283"));
        assertFunction("parse_presto_data_size('2.23kB')", DECIMAL, decimal("2283"));
        assertFunction("parse_presto_data_size('2.234kB')", DECIMAL, decimal("2287"));
        assertFunction("parse_presto_data_size('3MB')", DECIMAL, decimal("3145728"));
        assertFunction("parse_presto_data_size('4GB')", DECIMAL, decimal("4294967296"));
        assertFunction("parse_presto_data_size('4TB')", DECIMAL, decimal("4398046511104"));
        assertFunction("parse_presto_data_size('5PB')", DECIMAL, decimal("5629499534213120"));
        assertFunction("parse_presto_data_size('6EB')", DECIMAL, decimal("6917529027641081856"));
        assertFunction("parse_presto_data_size('7ZB')", DECIMAL, decimal("8264141345021879123968"));
        assertFunction("parse_presto_data_size('8YB')", DECIMAL, decimal("9671406556917033397649408"));
        assertFunction("parse_presto_data_size('6917529027641081856EB')", DECIMAL, decimal("7975367974709495237422842361682067456"));
        assertFunction("parse_presto_data_size('69175290276410818560EB')", DECIMAL, decimal("79753679747094952374228423616820674560"));

        assertInvalidFunction("parse_presto_data_size('')", INVALID_FUNCTION_ARGUMENT, "Invalid data size: ''");
        assertInvalidFunction("parse_presto_data_size('0')", INVALID_FUNCTION_ARGUMENT, "Invalid data size: '0'");
        assertInvalidFunction("parse_presto_data_size('10KB')", INVALID_FUNCTION_ARGUMENT, "Invalid data size: '10KB'");
        assertInvalidFunction("parse_presto_data_size('KB')", INVALID_FUNCTION_ARGUMENT, "Invalid data size: 'KB'");
        assertInvalidFunction("parse_presto_data_size('-1B')", INVALID_FUNCTION_ARGUMENT, "Invalid data size: '-1B'");
        assertInvalidFunction("parse_presto_data_size('12345K')", INVALID_FUNCTION_ARGUMENT, "Invalid data size: '12345K'");
        assertInvalidFunction("parse_presto_data_size('A12345B')", INVALID_FUNCTION_ARGUMENT, "Invalid data size: 'A12345B'");
        assertInvalidFunction("parse_presto_data_size('99999999999999YB')", NUMERIC_VALUE_OUT_OF_RANGE, "Value out of range: '99999999999999YB' ('120892581961461708544797985370825293824B')");
    }
}
