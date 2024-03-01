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

import com.facebook.presto.common.type.ArrayType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.type.IpAddressType.IPADDRESS;
import static com.facebook.presto.type.IpPrefixType.IPPREFIX;

public class TestIpPrefixFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testIpAddressIpPrefix()
    {
        assertFunction("IP_PREFIX(IPADDRESS '1.2.3.4', 24)", IPPREFIX, "1.2.3.0/24");
        assertFunction("IP_PREFIX(IPADDRESS '1.2.3.4', 32)", IPPREFIX, "1.2.3.4/32");
        assertFunction("IP_PREFIX(IPADDRESS '1.2.3.4', 0)", IPPREFIX, "0.0.0.0/0");
        assertFunction("IP_PREFIX(IPADDRESS '::ffff:1.2.3.4', 24)", IPPREFIX, "1.2.3.0/24");
        assertFunction("IP_PREFIX(IPADDRESS '64:ff9b::17', 64)", IPPREFIX, "64:ff9b::/64");
        assertFunction("IP_PREFIX(IPADDRESS '64:ff9b::17', 127)", IPPREFIX, "64:ff9b::16/127");
        assertFunction("IP_PREFIX(IPADDRESS '64:ff9b::17', 128)", IPPREFIX, "64:ff9b::17/128");
        assertFunction("IP_PREFIX(IPADDRESS '64:ff9b::17', 0)", IPPREFIX, "::/0");
        assertInvalidFunction("IP_PREFIX(IPADDRESS '::ffff:1.2.3.4', -1)", "IPv4 subnet size must be in range [0, 32]");
        assertInvalidFunction("IP_PREFIX(IPADDRESS '::ffff:1.2.3.4', 33)", "IPv4 subnet size must be in range [0, 32]");
        assertInvalidFunction("IP_PREFIX(IPADDRESS '64:ff9b::10', -1)", "IPv6 subnet size must be in range [0, 128]");
        assertInvalidFunction("IP_PREFIX(IPADDRESS '64:ff9b::10', 129)", "IPv6 subnet size must be in range [0, 128]");
    }

    @Test
    public void testStringIpPrefix()
    {
        assertFunction("IP_PREFIX('1.2.3.4', 24)", IPPREFIX, "1.2.3.0/24");
        assertFunction("IP_PREFIX('1.2.3.4', 32)", IPPREFIX, "1.2.3.4/32");
        assertFunction("IP_PREFIX('1.2.3.4', 0)", IPPREFIX, "0.0.0.0/0");
        assertFunction("IP_PREFIX('::ffff:1.2.3.4', 24)", IPPREFIX, "1.2.3.0/24");
        assertFunction("IP_PREFIX('64:ff9b::17', 64)", IPPREFIX, "64:ff9b::/64");
        assertFunction("IP_PREFIX('64:ff9b::17', 127)", IPPREFIX, "64:ff9b::16/127");
        assertFunction("IP_PREFIX('64:ff9b::17', 128)", IPPREFIX, "64:ff9b::17/128");
        assertFunction("IP_PREFIX('64:ff9b::17', 0)", IPPREFIX, "::/0");
        assertInvalidFunction("IP_PREFIX('::ffff:1.2.3.4', -1)", "IPv4 subnet size must be in range [0, 32]");
        assertInvalidFunction("IP_PREFIX('::ffff:1.2.3.4', 33)", "IPv4 subnet size must be in range [0, 32]");
        assertInvalidFunction("IP_PREFIX('64:ff9b::10', -1)", "IPv6 subnet size must be in range [0, 128]");
        assertInvalidFunction("IP_PREFIX('64:ff9b::10', 129)", "IPv6 subnet size must be in range [0, 128]");
        assertInvalidCast("IP_PREFIX('localhost', 24)", "Cannot cast value to IPADDRESS: localhost");
        assertInvalidCast("IP_PREFIX('64::ff9b::10', 24)", "Cannot cast value to IPADDRESS: 64::ff9b::10");
        assertInvalidCast("IP_PREFIX('64:face:book::10', 24)", "Cannot cast value to IPADDRESS: 64:face:book::10");
        assertInvalidCast("IP_PREFIX('123.456.789.012', 24)", "Cannot cast value to IPADDRESS: 123.456.789.012");
    }

    @Test
    public void testIpSubnetMin()
    {
        assertFunction("IP_SUBNET_MIN(IPPREFIX '1.2.3.4/24')", IPADDRESS, "1.2.3.0");
        assertFunction("IP_SUBNET_MIN(IPPREFIX '1.2.3.4/32')", IPADDRESS, "1.2.3.4");
        assertFunction("IP_SUBNET_MIN(IPPREFIX '64:ff9b::17/64')", IPADDRESS, "64:ff9b::");
        assertFunction("IP_SUBNET_MIN(IPPREFIX '64:ff9b::17/127')", IPADDRESS, "64:ff9b::16");
        assertFunction("IP_SUBNET_MIN(IPPREFIX '64:ff9b::17/128')", IPADDRESS, "64:ff9b::17");
        assertFunction("IP_SUBNET_MIN(IPPREFIX '64:ff9b::17/0')", IPADDRESS, "::");
    }

    @Test
    public void testIpSubnetMax()
    {
        assertFunction("IP_SUBNET_MAX(IPPREFIX '1.2.3.128/26')", IPADDRESS, "1.2.3.191");
        assertFunction("IP_SUBNET_MAX(IPPREFIX '192.168.128.4/32')", IPADDRESS, "192.168.128.4");
        assertFunction("IP_SUBNET_MAX(IPPREFIX '10.1.16.3/9')", IPADDRESS, "10.127.255.255");
        assertFunction("IP_SUBNET_MAX(IPPREFIX '2001:db8::16/127')", IPADDRESS, "2001:db8::17");
        assertFunction("IP_SUBNET_MAX(IPPREFIX '2001:db8::16/128')", IPADDRESS, "2001:db8::16");
        assertFunction("IP_SUBNET_MAX(IPPREFIX '64:ff9b::17/64')", IPADDRESS, "64:ff9b::ffff:ffff:ffff:ffff");
        assertFunction("IP_SUBNET_MAX(IPPREFIX '64:ff9b::17/72')", IPADDRESS, "64:ff9b::ff:ffff:ffff:ffff");
        assertFunction("IP_SUBNET_MAX(IPPREFIX '64:ff9b::17/0')", IPADDRESS, "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff");
    }

    @Test
    public void testIpSubnetRange()
    {
        assertFunction("IP_SUBNET_RANGE(IPPREFIX '1.2.3.160/24')", new ArrayType(IPADDRESS), ImmutableList.of("1.2.3.0", "1.2.3.255"));
        assertFunction("IP_SUBNET_RANGE(IPPREFIX '1.2.3.128/31')", new ArrayType(IPADDRESS), ImmutableList.of("1.2.3.128", "1.2.3.129"));
        assertFunction("IP_SUBNET_RANGE(IPPREFIX '10.1.6.46/32')", new ArrayType(IPADDRESS), ImmutableList.of("10.1.6.46", "10.1.6.46"));
        assertFunction("IP_SUBNET_RANGE(IPPREFIX '10.1.6.46/0')", new ArrayType(IPADDRESS), ImmutableList.of("0.0.0.0", "255.255.255.255"));
        assertFunction("IP_SUBNET_RANGE(IPPREFIX '64:ff9b::17/64')", new ArrayType(IPADDRESS), ImmutableList.of("64:ff9b::", "64:ff9b::ffff:ffff:ffff:ffff"));
        assertFunction("IP_SUBNET_RANGE(IPPREFIX '64:ff9b::52f4/120')", new ArrayType(IPADDRESS), ImmutableList.of("64:ff9b::5200", "64:ff9b::52ff"));
        assertFunction("IP_SUBNET_RANGE(IPPREFIX '64:ff9b::17/128')", new ArrayType(IPADDRESS), ImmutableList.of("64:ff9b::17", "64:ff9b::17"));
    }

    @Test
    public void testIsSubnetOf()
    {
        assertFunction("IS_SUBNET_OF(IPPREFIX '1.2.3.128/26', IPADDRESS '1.2.3.129')", BOOLEAN, true);
        assertFunction("IS_SUBNET_OF(IPPREFIX '1.2.3.128/26', IPADDRESS '1.2.5.1')", BOOLEAN, false);
        assertFunction("IS_SUBNET_OF(IPPREFIX '1.2.3.128/32', IPADDRESS '1.2.3.128')", BOOLEAN, true);
        assertFunction("IS_SUBNET_OF(IPPREFIX '1.2.3.128/0', IPADDRESS '192.168.5.1')", BOOLEAN, true);
        assertFunction("IS_SUBNET_OF(IPPREFIX '64:ff9b::17/64', IPADDRESS '64:ff9b::ffff:ff')", BOOLEAN, true);
        assertFunction("IS_SUBNET_OF(IPPREFIX '64:ff9b::17/64', IPADDRESS '64:ffff::17')", BOOLEAN, false);

        assertFunction("IS_SUBNET_OF(IPPREFIX '192.168.3.131/26', IPPREFIX '192.168.3.144/30')", BOOLEAN, true);
        assertFunction("IS_SUBNET_OF(IPPREFIX '1.2.3.128/26', IPPREFIX '1.2.5.1/30')", BOOLEAN, false);
        assertFunction("IS_SUBNET_OF(IPPREFIX '1.2.3.128/26', IPPREFIX '1.2.3.128/26')", BOOLEAN, true);
        assertFunction("IS_SUBNET_OF(IPPREFIX '64:ff9b::17/64', IPPREFIX '64:ff9b::ff:25/80')", BOOLEAN, true);
        assertFunction("IS_SUBNET_OF(IPPREFIX '64:ff9b::17/64', IPPREFIX '64:ffff::17/64')", BOOLEAN, false);
        assertFunction("IS_SUBNET_OF(IPPREFIX '2804:431:b000::/37', IPPREFIX '2804:431:b000::/38')", BOOLEAN, true);
        assertFunction("IS_SUBNET_OF(IPPREFIX '2804:431:b000::/38', IPPREFIX '2804:431:b000::/37')", BOOLEAN, false);
        assertFunction("IS_SUBNET_OF(IPPREFIX '170.0.52.0/22', IPPREFIX '170.0.52.0/24')", BOOLEAN, true);
        assertFunction("IS_SUBNET_OF(IPPREFIX '170.0.52.0/24', IPPREFIX '170.0.52.0/22')", BOOLEAN, false);
    }
}
