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
package com.facebook.presto.nativetests.operator.scalar;

import com.facebook.presto.tests.operator.scalar.AbstractTestIpPrefix;
import org.testng.annotations.Test;

public class TestIpPrefixFunctions
        extends AbstractTestNativeFunctions
        implements AbstractTestIpPrefix
{
    @Test
    public void testInvalidIpAddressIpPrefix()
    {
        assertInvalidFunction("IP_PREFIX(IPADDRESS '::ffff:1.2.3.4', -1)", "IPv4 subnet size must be in range [0, 32]");
        assertInvalidFunction("IP_PREFIX(IPADDRESS '::ffff:1.2.3.4', 33)", "IPv4 subnet size must be in range [0, 32]");
        assertInvalidFunction("IP_PREFIX(IPADDRESS '64:ff9b::10', -1)", "IPv6 subnet size must be in range [0, 128]");
        assertInvalidFunction("IP_PREFIX(IPADDRESS '64:ff9b::10', 129)", "IPv6 subnet size must be in range [0, 128]");
    }

    @Test
    public void testInvalidStringIpPrefix()
    {
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
    public void testIpPrefixCollapseNoNullPrefixesError()
    {
        assertInvalidFunction("IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '192.168.0.0/22', CAST(NULL AS IPPREFIX)])",
                "ip_prefix_collapse does not support null elements");
    }

    @Test
    public void testIpPrefixCollapseMixedIpVersionError()
    {
        assertInvalidFunction("IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '192.168.0.0/22', IPPREFIX '2409:4043:251a:d200::/56'])",
                "All IPPREFIX elements must be the same IP version.");
    }

    @Test
    public void testIpPrefixSubnetsInvalidPrefixLengths()
    {
        assertInvalidFunction("IP_PREFIX_SUBNETS(IPPREFIX '192.168.0.0/24', -1)", "Invalid prefix length for IPv4: -1");
        assertInvalidFunction("IP_PREFIX_SUBNETS(IPPREFIX '192.168.0.0/24', 33)", "Invalid prefix length for IPv4: 33");
        assertInvalidFunction("IP_PREFIX_SUBNETS(IPPREFIX '64:ff9b::17/64', -1)", "Invalid prefix length for IPv6: -1");
        assertInvalidFunction("IP_PREFIX_SUBNETS(IPPREFIX '64:ff9b::17/64', 129)", "Invalid prefix length for IPv6: 129");
    }
}
