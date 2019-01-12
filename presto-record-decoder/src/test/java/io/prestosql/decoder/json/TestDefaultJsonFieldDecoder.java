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
package io.prestosql.decoder.json;

import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.math.BigInteger;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static java.util.Arrays.asList;

public class TestDefaultJsonFieldDecoder
{
    private JsonFieldDecoderTester tester = new JsonFieldDecoderTester();

    @Test
    public void testDecode()
    {
        // test decoding using default field decoder
        tester.assertDecodedAs("0.5", DOUBLE, 0.5);
        tester.assertDecodedAs("-0.5", DOUBLE, -0.5);
        tester.assertDecodedAs("10", DOUBLE, 10.0);
        tester.assertDecodedAs("0", DOUBLE, 0.0);
        tester.assertDecodedAs("\"0.4\"", DOUBLE, 0.4);

        tester.assertDecodedAs("" + Byte.MIN_VALUE, TINYINT, Byte.MIN_VALUE);
        tester.assertDecodedAs("" + Byte.MAX_VALUE, TINYINT, Byte.MAX_VALUE);
        tester.assertDecodedAs("0", TINYINT, 0);
        tester.assertDecodedAs("\"10\"", TINYINT, 10);
        assertCouldNotParse("" + (Byte.MIN_VALUE - 1), TINYINT);
        assertCouldNotParse("" + (Byte.MAX_VALUE + 1), TINYINT);

        tester.assertDecodedAs("" + Short.MIN_VALUE, SMALLINT, Short.MIN_VALUE);
        tester.assertDecodedAs("" + Short.MAX_VALUE, SMALLINT, Short.MAX_VALUE);
        tester.assertDecodedAs("0", SMALLINT, 0);
        tester.assertDecodedAs("\"1000\"", SMALLINT, 1000);
        assertCouldNotParse("" + (Short.MIN_VALUE - 1), SMALLINT);
        assertCouldNotParse("" + (Short.MAX_VALUE + 1), SMALLINT);

        tester.assertDecodedAs("" + Integer.MIN_VALUE, INTEGER, Integer.MIN_VALUE);
        tester.assertDecodedAs("" + Integer.MAX_VALUE, INTEGER, Integer.MAX_VALUE);
        tester.assertDecodedAs("0", INTEGER, 0);
        tester.assertDecodedAs("\"1000\"", INTEGER, 1000);
        assertCouldNotParse("" + (Integer.MIN_VALUE - 1L), INTEGER);
        assertCouldNotParse("" + (Integer.MAX_VALUE + 1L), INTEGER);

        tester.assertDecodedAs("" + Long.MIN_VALUE, BIGINT, Long.MIN_VALUE);
        tester.assertDecodedAs("" + Long.MAX_VALUE, BIGINT, Long.MAX_VALUE);
        tester.assertDecodedAs("0", BIGINT, 0);
        tester.assertDecodedAs("\"1000\"", BIGINT, 1000);
        assertCouldNotParse("" + (BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE)), BIGINT);
        assertCouldNotParse("" + (BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)), BIGINT);

        tester.assertDecodedAs("false", BOOLEAN, false);
        tester.assertDecodedAs("true", BOOLEAN, true);
        tester.assertDecodedAs("\"false\"", BOOLEAN, false);
        tester.assertDecodedAs("\"true\"", BOOLEAN, true);
        tester.assertDecodedAs("\"blah\"", BOOLEAN, false);
        tester.assertDecodedAs("0", BOOLEAN, false);
        tester.assertDecodedAs("1", BOOLEAN, true);
        tester.assertDecodedAs("10", BOOLEAN, true);
        tester.assertDecodedAs("\"0\"", BOOLEAN, false);
        tester.assertDecodedAs("\"1\"", BOOLEAN, false);
    }

    @Test
    public void testDecodeNulls()
    {
        for (Type type : asList(TINYINT, SMALLINT, INTEGER, BIGINT, DOUBLE, BOOLEAN)) {
            tester.assertDecodedAsNull("null", type);
            tester.assertMissingDecodedAsNull(type);
        }
    }

    @Test
    public void decodeNonValue()
    {
        for (Type type : asList(TINYINT, SMALLINT, INTEGER, BIGINT, DOUBLE, BOOLEAN)) {
            assertCouldNotParse("{}", type);
            assertCouldNotParse("[]", type);
        }
    }

    private void assertCouldNotParse(String jsonValue, Type type)
    {
        tester.assertInvalidInput(jsonValue, type, ".*could not parse.*");
    }
}
