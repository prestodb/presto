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
package com.facebook.presto.type;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.type.SqlVarbinary;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.IpAddressType.IPADDRESS;
import static com.google.common.io.BaseEncoding.base16;

public class TestIpAddressOperators
        extends AbstractTestFunctions
{
    @Test
    public void testVarcharToIpAddressCast()
    {
        assertFunction("CAST('::ffff:1.2.3.4' AS IPADDRESS)", IPADDRESS, "1.2.3.4");
        assertFunction("CAST('1.2.3.4' AS IPADDRESS)", IPADDRESS, "1.2.3.4");
        assertFunction("CAST('192.168.0.0' AS IPADDRESS)", IPADDRESS, "192.168.0.0");
        assertFunction("CAST('2001:0db8:0000:0000:0000:ff00:0042:8329' AS IPADDRESS)", IPADDRESS, "2001:db8::ff00:42:8329");
        assertFunction("CAST('2001:db8::ff00:42:8329' AS IPADDRESS)", IPADDRESS, "2001:db8::ff00:42:8329");
        assertFunction("CAST('2001:db8:0:0:1:0:0:1' AS IPADDRESS)", IPADDRESS, "2001:db8::1:0:0:1");
        assertFunction("CAST('2001:db8:0:0:1::1' AS IPADDRESS)", IPADDRESS, "2001:db8::1:0:0:1");
        assertFunction("CAST('2001:db8::1:0:0:1' AS IPADDRESS)", IPADDRESS, "2001:db8::1:0:0:1");
        assertFunction("CAST('2001:DB8::FF00:ABCD:12EF' AS IPADDRESS)", IPADDRESS, "2001:db8::ff00:abcd:12ef");
        assertFunction("IPADDRESS '10.0.0.0'", IPADDRESS, "10.0.0.0");
        assertFunction("IPADDRESS '64:ff9b::10.0.0.0'", IPADDRESS, "64:ff9b::a00:0");
        assertInvalidCast("CAST('facebook.com' AS IPADDRESS)", "Cannot cast value to IPADDRESS: facebook.com");
        assertInvalidCast("CAST('localhost' AS IPADDRESS)", "Cannot cast value to IPADDRESS: localhost");
        assertInvalidCast("CAST('2001:db8::1::1' AS IPADDRESS)", "Cannot cast value to IPADDRESS: 2001:db8::1::1");
        assertInvalidCast("CAST('2001:zxy::1::1' AS IPADDRESS)", "Cannot cast value to IPADDRESS: 2001:zxy::1::1");
        assertInvalidCast("CAST('789.1.1.1' AS IPADDRESS)", "Cannot cast value to IPADDRESS: 789.1.1.1");
    }

    @Test
    public void testIpAddressToVarcharCast()
    {
        assertFunction("CAST(IPADDRESS '::ffff:1.2.3.4' AS VARCHAR)", VARCHAR, "1.2.3.4");
        assertFunction("CAST(IPADDRESS '::ffff:102:304' AS VARCHAR)", VARCHAR, "1.2.3.4");
        assertFunction("CAST(IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329' AS VARCHAR)", VARCHAR, "2001:db8::ff00:42:8329");
        assertFunction("CAST(IPADDRESS '2001:db8::ff00:42:8329' AS VARCHAR)", VARCHAR, "2001:db8::ff00:42:8329");
        assertFunction("CAST(IPADDRESS '2001:db8:0:0:1:0:0:1' AS VARCHAR)", VARCHAR, "2001:db8::1:0:0:1");
        assertFunction("CAST(CAST('1.2.3.4' AS IPADDRESS) AS VARCHAR)", VARCHAR, "1.2.3.4");
        assertFunction("CAST(CAST('2001:db8:0:0:1::1' AS IPADDRESS) AS VARCHAR)", VARCHAR, "2001:db8::1:0:0:1");
        assertFunction("CAST(CAST('64:ff9b::10.0.0.0' AS IPADDRESS) AS VARCHAR)", VARCHAR, "64:ff9b::a00:0");
    }

    @Test
    public void testVarbinaryToIpAddressCast()
    {
        assertFunction("CAST(x'00000000000000000000ffff01020304' AS IPADDRESS)", IPADDRESS, "1.2.3.4");
        assertFunction("CAST(x'01020304' AS IPADDRESS)", IPADDRESS, "1.2.3.4");
        assertFunction("CAST(x'c0a80000' AS IPADDRESS)", IPADDRESS, "192.168.0.0");
        assertFunction("CAST(x'20010db8000000000000ff0000428329' AS IPADDRESS)", IPADDRESS, "2001:db8::ff00:42:8329");
        assertInvalidCast("CAST(x'f000001100' AS IPADDRESS)", "Invalid IP address binary length: 5");
    }

    @Test
    public void testIpAddressToVarbinaryCast()
    {
        assertFunction("CAST(IPADDRESS '::ffff:1.2.3.4' AS VARBINARY)", VARBINARY, new SqlVarbinary(base16().decode("00000000000000000000FFFF01020304")));
        assertFunction("CAST(IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329' AS VARBINARY)", VARBINARY, new SqlVarbinary(base16().decode("20010DB8000000000000FF0000428329")));
        assertFunction("CAST(IPADDRESS '2001:db8::ff00:42:8329' AS VARBINARY)", VARBINARY, new SqlVarbinary(base16().decode("20010DB8000000000000FF0000428329")));
    }

    @Test
    public void testEquals()
    {
        assertFunction("IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329' = IPADDRESS '2001:db8::ff00:42:8329'", BOOLEAN, true);
        assertFunction("CAST('1.2.3.4' AS IPADDRESS) = CAST('::ffff:1.2.3.4' AS IPADDRESS)", BOOLEAN, true);
        assertFunction("IPADDRESS '192.168.0.0' = IPADDRESS '::ffff:192.168.0.0'", BOOLEAN, true);
        assertFunction("IPADDRESS '10.0.0.0' = IPADDRESS '::ffff:a00:0'", BOOLEAN, true);
        assertFunction("IPADDRESS '2001:db8::ff00:42:8329' = IPADDRESS '2001:db8::ff00:42:8300'", BOOLEAN, false);
        assertFunction("CAST('1.2.3.4' AS IPADDRESS) = IPADDRESS '1.2.3.5'", BOOLEAN, false);
    }

    @Test
    public void testDistinctFrom()
    {
        assertFunction("IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329' IS DISTINCT FROM IPADDRESS '2001:db8::ff00:42:8329'", BOOLEAN, false);
        assertFunction("CAST(NULL AS IPADDRESS) IS DISTINCT FROM CAST(NULL AS IPADDRESS)", BOOLEAN, false);
        assertFunction("IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329' IS DISTINCT FROM IPADDRESS '2001:db8::ff00:42:8328'", BOOLEAN, true);
        assertFunction("IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329' IS DISTINCT FROM CAST(NULL AS IPADDRESS)", BOOLEAN, true);
        assertFunction("CAST(NULL AS IPADDRESS) IS DISTINCT FROM IPADDRESS '2001:db8::ff00:42:8328'", BOOLEAN, true);
    }

    @Test
    public void testNotEquals()
    {
        assertFunction("IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329' != IPADDRESS '1.2.3.4'", BOOLEAN, true);
        assertFunction("CAST('1.2.3.4' AS IPADDRESS) <> CAST('1.2.3.5' AS IPADDRESS)", BOOLEAN, true);
        assertFunction("CAST('1.2.3.4' AS IPADDRESS) != IPADDRESS '1.2.3.4'", BOOLEAN, false);
        assertFunction("IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329' <> IPADDRESS '2001:db8::ff00:42:8329'", BOOLEAN, false);
        assertFunction("CAST('1.2.3.4' AS IPADDRESS) <> CAST('::ffff:1.2.3.4' AS IPADDRESS)", BOOLEAN, false);
    }

    @Test
    public void testOrderOperators()
    {
        assertFunction("IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329' > IPADDRESS '1.2.3.4'", BOOLEAN, true);
        assertFunction("IPADDRESS '1.2.3.4' > IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329'", BOOLEAN, false);

        assertFunction("CAST('1.2.3.4' AS IPADDRESS) < CAST('1.2.3.5' AS IPADDRESS)", BOOLEAN, true);
        assertFunction("CAST('1.2.3.5' AS IPADDRESS) < CAST('1.2.3.4' AS IPADDRESS)", BOOLEAN, false);

        assertFunction("IPADDRESS '::1' <= CAST('1.2.3.5' AS IPADDRESS)", BOOLEAN, true);
        assertFunction("IPADDRESS '1.2.3.5' <= CAST('1.2.3.5' AS IPADDRESS)", BOOLEAN, true);
        assertFunction("IPADDRESS '1.2.3.6' <= CAST('1.2.3.5' AS IPADDRESS)", BOOLEAN, false);

        assertFunction("IPADDRESS '::1' >= IPADDRESS '::'", BOOLEAN, true);
        assertFunction("IPADDRESS '::1' >= IPADDRESS '::1'", BOOLEAN, true);
        assertFunction("IPADDRESS '::' >= IPADDRESS '::1'", BOOLEAN, false);

        assertFunction("IPADDRESS '::1' BETWEEN IPADDRESS '::' AND IPADDRESS '::1234'", BOOLEAN, true);
        assertFunction("IPADDRESS '::2222' BETWEEN IPADDRESS '::' AND IPADDRESS '::1234'", BOOLEAN, false);
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "CAST(null AS IPADDRESS)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "IPADDRESS '::2222'", BOOLEAN, false);
    }
}
