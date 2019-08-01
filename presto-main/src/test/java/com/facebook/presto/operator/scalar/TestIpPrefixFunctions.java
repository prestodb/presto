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
}
