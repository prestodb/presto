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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.IpAddressType.IPADDRESS;
import static com.facebook.presto.type.IpPrefixType.IPPREFIX;
import static java.lang.System.arraycopy;

public class TestIpPrefixOperators
        extends AbstractTestFunctions
{
    @Test
    public void testVarcharToIpPrefixCast()
    {
        assertFunction("CAST('::ffff:1.2.3.4/24' AS IPPREFIX)", IPPREFIX, "1.2.3.0/24");
        assertFunction("CAST('192.168.0.0/24' AS IPPREFIX)", IPPREFIX, "192.168.0.0/24");
        assertFunction("CAST('255.2.3.4/0' AS IPPREFIX)", IPPREFIX, "0.0.0.0/0");
        assertFunction("CAST('255.2.3.4/1' AS IPPREFIX)", IPPREFIX, "128.0.0.0/1");
        assertFunction("CAST('255.2.3.4/2' AS IPPREFIX)", IPPREFIX, "192.0.0.0/2");
        assertFunction("CAST('255.2.3.4/4' AS IPPREFIX)", IPPREFIX, "240.0.0.0/4");
        assertFunction("CAST('1.2.3.4/8' AS IPPREFIX)", IPPREFIX, "1.0.0.0/8");
        assertFunction("CAST('1.2.3.4/16' AS IPPREFIX)", IPPREFIX, "1.2.0.0/16");
        assertFunction("CAST('1.2.3.4/24' AS IPPREFIX)", IPPREFIX, "1.2.3.0/24");
        assertFunction("CAST('1.2.3.255/25' AS IPPREFIX)", IPPREFIX, "1.2.3.128/25");
        assertFunction("CAST('1.2.3.255/26' AS IPPREFIX)", IPPREFIX, "1.2.3.192/26");
        assertFunction("CAST('1.2.3.255/28' AS IPPREFIX)", IPPREFIX, "1.2.3.240/28");
        assertFunction("CAST('1.2.3.255/30' AS IPPREFIX)", IPPREFIX, "1.2.3.252/30");
        assertFunction("CAST('1.2.3.255/32' AS IPPREFIX)", IPPREFIX, "1.2.3.255/32");
        assertFunction("CAST('2001:0db8:0000:0000:0000:ff00:0042:8329/128' AS IPPREFIX)", IPPREFIX, "2001:db8::ff00:42:8329/128");
        assertFunction("CAST('2001:db8::ff00:42:8329/128' AS IPPREFIX)", IPPREFIX, "2001:db8::ff00:42:8329/128");
        assertFunction("CAST('2001:db8:0:0:1:0:0:1/128' AS IPPREFIX)", IPPREFIX, "2001:db8::1:0:0:1/128");
        assertFunction("CAST('2001:db8:0:0:1::1/128' AS IPPREFIX)", IPPREFIX, "2001:db8::1:0:0:1/128");
        assertFunction("CAST('2001:db8::1:0:0:1/128' AS IPPREFIX)", IPPREFIX, "2001:db8::1:0:0:1/128");
        assertFunction("CAST('2001:DB8::FF00:ABCD:12EF/128' AS IPPREFIX)", IPPREFIX, "2001:db8::ff00:abcd:12ef/128");
        assertFunction("CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/0' AS IPPREFIX)", IPPREFIX, "::/0");
        assertFunction("CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/1' AS IPPREFIX)", IPPREFIX, "8000::/1");
        assertFunction("CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/2' AS IPPREFIX)", IPPREFIX, "c000::/2");
        assertFunction("CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/4' AS IPPREFIX)", IPPREFIX, "f000::/4");
        assertFunction("CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/8' AS IPPREFIX)", IPPREFIX, "ff00::/8");
        assertFunction("CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/16' AS IPPREFIX)", IPPREFIX, "ffff::/16");
        assertFunction("CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/32' AS IPPREFIX)", IPPREFIX, "ffff:ffff::/32");
        assertFunction("CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/48' AS IPPREFIX)", IPPREFIX, "ffff:ffff:ffff::/48");
        assertFunction("CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/64' AS IPPREFIX)", IPPREFIX, "ffff:ffff:ffff:ffff::/64");
        assertFunction("CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/80' AS IPPREFIX)", IPPREFIX, "ffff:ffff:ffff:ffff:ffff::/80");
        assertFunction("CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/96' AS IPPREFIX)", IPPREFIX, "ffff:ffff:ffff:ffff:ffff:ffff::/96");
        assertFunction("CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/112' AS IPPREFIX)", IPPREFIX, "ffff:ffff:ffff:ffff:ffff:ffff:ffff:0/112");
        assertFunction("CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/120' AS IPPREFIX)", IPPREFIX, "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ff00/120");
        assertFunction("CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/124' AS IPPREFIX)", IPPREFIX, "ffff:ffff:ffff:ffff:ffff:ffff:ffff:fff0/124");
        assertFunction("CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/126' AS IPPREFIX)", IPPREFIX, "ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffc/126");
        assertFunction("CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/127' AS IPPREFIX)", IPPREFIX, "ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe/127");
        assertFunction("CAST('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128' AS IPPREFIX)", IPPREFIX, "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128");
        assertFunction("IPPREFIX '10.0.0.0/32'", IPPREFIX, "10.0.0.0/32");
        assertFunction("IPPREFIX '64:ff9b::10.0.0.0/128'", IPPREFIX, "64:ff9b::a00:0/128");
        assertInvalidCast("CAST('facebook.com/32' AS IPPREFIX)", "Cannot cast value to IPPREFIX: facebook.com/32");
        assertInvalidCast("CAST('localhost/32' AS IPPREFIX)", "Cannot cast value to IPPREFIX: localhost/32");
        assertInvalidCast("CAST('2001:db8::1::1/128' AS IPPREFIX)", "Cannot cast value to IPPREFIX: 2001:db8::1::1/128");
        assertInvalidCast("CAST('2001:zxy::1::1/128' AS IPPREFIX)", "Cannot cast value to IPPREFIX: 2001:zxy::1::1/128");
        assertInvalidCast("CAST('789.1.1.1/32' AS IPPREFIX)", "Cannot cast value to IPPREFIX: 789.1.1.1/32");
        assertInvalidCast("CAST('192.1.1.1' AS IPPREFIX)", "Cannot cast value to IPPREFIX: 192.1.1.1");
        assertInvalidCast("CAST('192.1.1.1/128' AS IPPREFIX)", "Cannot cast value to IPPREFIX: 192.1.1.1/128");
    }

    @Test
    public void testIpPrefixToVarcharCast()
    {
        assertFunction("CAST(IPPREFIX '::ffff:1.2.3.4/32' AS VARCHAR)", VARCHAR, "1.2.3.4/32");
        assertFunction("CAST(IPPREFIX '::ffff:102:304/32' AS VARCHAR)", VARCHAR, "1.2.3.4/32");
        assertFunction("CAST(IPPREFIX '2001:0db8:0000:0000:0000:ff00:0042:8329/128' AS VARCHAR)", VARCHAR, "2001:db8::ff00:42:8329/128");
        assertFunction("CAST(IPPREFIX '2001:db8::ff00:42:8329/128' AS VARCHAR)", VARCHAR, "2001:db8::ff00:42:8329/128");
        assertFunction("CAST(IPPREFIX '2001:db8:0:0:1:0:0:1/128' AS VARCHAR)", VARCHAR, "2001:db8::1:0:0:1/128");
        assertFunction("CAST(CAST('1.2.3.4/32' AS IPPREFIX) AS VARCHAR)", VARCHAR, "1.2.3.4/32");
        assertFunction("CAST(CAST('2001:db8:0:0:1::1/128' AS IPPREFIX) AS VARCHAR)", VARCHAR, "2001:db8::1:0:0:1/128");
        assertFunction("CAST(CAST('64:ff9b::10.0.0.0/128' AS IPPREFIX) AS VARCHAR)", VARCHAR, "64:ff9b::a00:0/128");
    }

    @Test
    public void testIpPrefixToIpAddressCast()
    {
        assertFunction("CAST(IPPREFIX '1.2.3.4/32' AS IPADDRESS)", IPADDRESS, "1.2.3.4");
        assertFunction("CAST(IPPREFIX '1.2.3.4/24' AS IPADDRESS)", IPADDRESS, "1.2.3.0");
        assertFunction("CAST(IPPREFIX '::1/128' AS IPADDRESS)", IPADDRESS, "::1");
        assertFunction("CAST(IPPREFIX '2001:db8::ff00:42:8329/128' AS IPADDRESS)", IPADDRESS, "2001:db8::ff00:42:8329");
        assertFunction("CAST(IPPREFIX '2001:db8::ff00:42:8329/64' AS IPADDRESS)", IPADDRESS, "2001:db8::");
    }

    @Test
    public void testIpAddressToIpPrefixCast()
    {
        assertFunction("CAST(IPADDRESS '1.2.3.4' AS IPPREFIX)", IPPREFIX, "1.2.3.4/32");
        assertFunction("CAST(IPADDRESS '::ffff:102:304' AS IPPREFIX)", IPPREFIX, "1.2.3.4/32");
        assertFunction("CAST(IPADDRESS '::1' AS IPPREFIX)", IPPREFIX, "::1/128");
        assertFunction("CAST(IPADDRESS '2001:db8::ff00:42:8329' AS IPPREFIX)", IPPREFIX, "2001:db8::ff00:42:8329/128");
    }

    @Test
    public void testEquals()
    {
        assertFunction("IPPREFIX '2001:0db8:0000:0000:0000:ff00:0042:8329/128' = IPPREFIX '2001:db8::ff00:42:8329/128'", BOOLEAN, true);
        assertFunction("CAST('1.2.3.4/32' AS IPPREFIX) = CAST('::ffff:1.2.3.4/32' AS IPPREFIX)", BOOLEAN, true);
        assertFunction("IPPREFIX '192.168.0.0/32' = IPPREFIX '::ffff:192.168.0.0/32'", BOOLEAN, true);
        assertFunction("IPPREFIX '10.0.0.0/32' = IPPREFIX '::ffff:a00:0/32'", BOOLEAN, true);
        assertFunction("CAST('1.2.3.4/24' AS IPPREFIX) = IPPREFIX '1.2.3.5/24'", BOOLEAN, true);
        assertFunction("IPPREFIX '2001:db8::ff00:42:8329/128' = IPPREFIX '2001:db8::ff00:42:8300/128'", BOOLEAN, false);
        assertFunction("CAST('1.2.3.4/32' AS IPPREFIX) = IPPREFIX '1.2.3.5/32'", BOOLEAN, false);
        assertFunction("CAST('1.2.0.0/24' AS IPPREFIX) = IPPREFIX '1.2.0.0/25'", BOOLEAN, false);
    }

    @Test
    public void testDistinctFrom()
    {
        assertFunction("IPPREFIX '2001:0db8:0000:0000:0000:ff00:0042:8329/128' IS DISTINCT FROM IPPREFIX '2001:db8::ff00:42:8329/128'", BOOLEAN, false);
        assertFunction("CAST(NULL AS IPPREFIX) IS DISTINCT FROM CAST(NULL AS IPPREFIX)", BOOLEAN, false);
        assertFunction("IPPREFIX '2001:0db8:0000:0000:0000:ff00:0042:8329/128' IS DISTINCT FROM IPPREFIX '2001:db8::ff00:42:8328/128'", BOOLEAN, true);
        assertFunction("IPPREFIX '2001:0db8:0000:0000:0000:ff00:0042:8329/128' IS DISTINCT FROM CAST(NULL AS IPPREFIX)", BOOLEAN, true);
        assertFunction("CAST(NULL AS IPPREFIX) IS DISTINCT FROM IPPREFIX '2001:db8::ff00:42:8328/128'", BOOLEAN, true);
    }

    @Test
    public void testNotEquals()
    {
        assertFunction("IPPREFIX '2001:0db8:0000:0000:0000:ff00:0042:8329/128' != IPPREFIX '1.2.3.4/32'", BOOLEAN, true);
        assertFunction("CAST('1.2.3.4/32' AS IPPREFIX) <> CAST('1.2.3.5/32' AS IPPREFIX)", BOOLEAN, true);
        assertFunction("CAST('1.2.3.4/32' AS IPPREFIX) != IPPREFIX '1.2.3.4/32'", BOOLEAN, false);
        assertFunction("IPPREFIX '2001:0db8:0000:0000:0000:ff00:0042:8329/128' <> IPPREFIX '2001:db8::ff00:42:8329/128'", BOOLEAN, false);
        assertFunction("CAST('1.2.3.4/32' AS IPPREFIX) <> CAST('::ffff:1.2.3.4/32' AS IPPREFIX)", BOOLEAN, false);
    }

    @Test
    public void testOrderOperators()
    {
        assertFunction("IPPREFIX '2001:0db8:0000:0000:0000:ff00:0042:8329/128' > IPPREFIX '1.2.3.4/32'", BOOLEAN, true);
        assertFunction("IPPREFIX '1.2.3.4/32' > IPPREFIX '2001:0db8:0000:0000:0000:ff00:0042:8329/128'", BOOLEAN, false);

        assertFunction("CAST('1.2.3.4/32' AS IPPREFIX) < CAST('1.2.3.5/32' AS IPPREFIX)", BOOLEAN, true);
        assertFunction("CAST('1.2.3.5/32' AS IPPREFIX) < CAST('1.2.3.4/32' AS IPPREFIX)", BOOLEAN, false);

        assertFunction("CAST('1.2.0.0/24' AS IPPREFIX) < CAST('1.2.0.0/25' AS IPPREFIX)", BOOLEAN, true);

        assertFunction("IPPREFIX '::1/128' <= CAST('1.2.3.5/32' AS IPPREFIX)", BOOLEAN, true);
        assertFunction("IPPREFIX '1.2.3.5/32' <= CAST('1.2.3.5/32' AS IPPREFIX)", BOOLEAN, true);
        assertFunction("IPPREFIX '1.2.3.6/32' <= CAST('1.2.3.5/32' AS IPPREFIX)", BOOLEAN, false);

        assertFunction("IPPREFIX '::1/128' >= IPPREFIX '::/128'", BOOLEAN, true);
        assertFunction("IPPREFIX '::1/128' >= IPPREFIX '::1/128'", BOOLEAN, true);
        assertFunction("IPPREFIX '::/128' >= IPPREFIX '::1/128'", BOOLEAN, false);

        assertFunction("IPPREFIX '::1/128' BETWEEN IPPREFIX '::/128' AND IPPREFIX '::1234/128'", BOOLEAN, true);
        assertFunction("IPPREFIX '::2222/128' BETWEEN IPPREFIX '::/128' AND IPPREFIX '::1234/128'", BOOLEAN, false);
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "CAST(null AS IPPREFIX)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "IPPREFIX '::2222/128'", BOOLEAN, false);
    }

    @Test
    public void testHash()
    {
        assertOperator(HASH_CODE, "CAST(null AS IPPREFIX)", BIGINT, null);
        assertOperator(HASH_CODE, "IPPREFIX '::2222/128'", BIGINT, hashFromType("::2222/128"));
    }

    private static long hashFromType(String address)
    {
        BlockBuilder blockBuilder = IPPREFIX.createBlockBuilder(null, 1);
        String[] parts = address.split("/");
        byte[] bytes = new byte[IPPREFIX.getFixedSize()];
        byte[] addressBytes = InetAddresses.forString(parts[0]).getAddress();
        arraycopy(addressBytes, 0, bytes, 0, 16);
        bytes[IPPREFIX.getFixedSize() - 1] = (byte) Integer.parseInt(parts[1]);
        IPPREFIX.writeSlice(blockBuilder, Slices.wrappedBuffer(bytes));
        Block block = blockBuilder.build();
        return IPPREFIX.hash(block, 0);
    }
}
