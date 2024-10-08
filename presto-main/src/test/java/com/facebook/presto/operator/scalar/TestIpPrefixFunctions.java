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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.type.IpAddressType.IPADDRESS;
import static com.facebook.presto.type.IpPrefixType.IPPREFIX;

public class TestIpPrefixFunctions
        extends AbstractTestFunctions
{
    @DataProvider(name = "public-ip-provider")
    public Object[] publicIpProvider()
    {
        return new Object[] {
                "6.7.8.9",
                "157.240.200.99",
                "8.8.8.8",
                "128.1.2.8",
                "2a03:2880:f031:12:face:b00c:0:2",
                "2600:1406:6c00::173c:ad43",
                "2607:f8b0:4007:818::2004"
        };
    }

    @DataProvider(name = "private-ip-provider")
    public Object[][] privateIpProvider()
    {
        return new Object[][] {
                // The first and last IP address in each private range
                {"0.0.0.0"}, {"0.255.255.255"},         // 0.0.0.0/8 RFC1122: "This host on this network"
                {"10.0.0.0"}, {"10.255.255.255"},       // 10.0.0.0/8 RFC1918: Private-Use
                {"100.64.0.0"}, {"100.127.255.255"},    // 100.64.0.0/10 RFC6598: Shared Address Space
                {"127.0.0.0"}, {"127.255.255.255"},     // 127.0.0.0/8 RFC1122: Loopback
                {"169.254.0.0"}, {"169.254.255.255"},   // 169.254.0.0/16 RFC3927: Link Local
                {"172.16.0.0"}, {"172.31.255.255"},     // 172.16.0.0/12 RFC1918: Private-Use
                {"192.0.0.0"}, {"192.0.0.255"},         // 192.0.0.0/24 RFC6890: IETF Protocol Assignments
                {"192.0.2.0"}, {"192.0.2.255"},         // 192.0.2.0/24 RFC5737: Documentation (TEST-NET-1)
                {"192.88.99.0"}, {"192.88.99.255"},     // 192.88.99.0/24 RFC3068: 6to4 Relay anycast
                {"192.168.0.0"}, {"192.168.255.255"},   // 192.168.0.0/16 RFC1918: Private-Use
                {"198.18.0.0"}, {"198.19.255.255"},     // 198.18.0.0/15 RFC2544: Benchmarking
                {"198.51.100.0"}, {"198.51.100.255"},   // 198.51.100.0/24 RFC5737: Documentation (TEST-NET-2)
                {"203.0.113.0"}, {"203.0.113.255"},     // 203.0.113.0/24 RFC5737: Documentation (TEST-NET-3)
                {"240.0.0.0"}, {"255.255.255.255"},     // 240.0.0.0/4 RFC1112: Reserved
                {"::"}, {"::"},                         // ::/128 RFC4291: Unspecified address
                {"::1"}, {"::1"},                       // ::1/128 RFC4291: Loopback address
                {"100::"}, {"100::ffff:ffff:ffff:ffff"},                        // 100::/64 RFC6666: Discard-Only Address Block
                {"64:ff9b:1::"}, {"64:ff9b:1:ffff:ffff:ffff:ffff:ffff"},        // 64:ff9b:1::/48 RFC8215: IPv4-IPv6 Translation
                {"2001:2::"}, {"2001:2:0:ffff:ffff:ffff:ffff:ffff"},            // 2001:2::/48 RFC5180,RFC Errata 1752: Benchmarking
                {"2001:db8::"}, {"2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"},     // 2001:db8::/32 RFC3849: Documentation
                {"2001::"}, {"2001:1ff:ffff:ffff:ffff:ffff:ffff:ffff"},         // 2001::/23 RFC2928: IETF Protocol Assignments
                {"5f00::"}, {"5f00:ffff:ffff:ffff:ffff:ffff:ffff:ffff"},        // 5f00::/16 RFC-ietf-6man-sids-06: Segment Routing (SRv6)
                {"fe80::"}, {"febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff"},        // fe80::/10 RFC4291: Link-Local Unicast
                {"fc00::"}, {"fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"},        // fc00::/7 RFC4193, RFC8190: Unique Local
                // some IPs in the middle of ranges
                {"10.1.2.3"},
                {"100.64.3.2"},
                {"192.168.55.99"},
                {"2001:0DB8:0000:0000:face:b00c:0000:0000"},
                {"0100:0000:0000:0000:ffff:ffff:0000:0000"}
        };
    }

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

    @Test
    public void testIpv4PrefixCollapse()
    {
        // simple
        assertFunction("IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '192.168.0.0/24', IPPREFIX '192.168.1.0/24'])",
                new ArrayType(IPPREFIX),
                ImmutableList.of("192.168.0.0/23"));

        // unsorted input, 1 adjacent prefix that cannot be aggregated, and one disjoint.
        assertFunction("IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '192.168.1.0/24', IPPREFIX '192.168.0.0/24', IPPREFIX '192.168.2.0/24', IPPREFIX '192.168.9.0/24'])",
                new ArrayType(IPPREFIX),
                ImmutableList.of("192.168.0.0/23", "192.168.2.0/24", "192.168.9.0/24"));
    }

    @Test
    public void testIpv6PrefixCollapse()
    {
        // simple
        assertFunction("IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '2620:10d:c090::/48', IPPREFIX '2620:10d:c091::/48'])",
                new ArrayType(IPPREFIX),
                ImmutableList.of("2620:10d:c090::/47"));

        // unsorted input, 1 adjacent prefix that cannot be aggregated, and one disjoint.
        assertFunction("IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '2804:13c:4d6:e200::/56', IPPREFIX '2804:13c:4d6:dd00::/56', IPPREFIX '2804:13c:4d6:dc00::/56', IPPREFIX '2804:13c:4d6:de00::/56'])",
                new ArrayType(IPPREFIX),
                ImmutableList.of("2804:13c:4d6:dc00::/55", "2804:13c:4d6:de00::/56", "2804:13c:4d6:e200::/56"));
    }

    @Test
    public void testIpPrefixCollapseIpv4SingleIPs()
    {
        assertFunction("IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '192.168.0.1/32', IPPREFIX '192.168.33.1/32'])",
                new ArrayType(IPPREFIX),
                ImmutableList.of("192.168.0.1/32", "192.168.33.1/32"));
    }

    @Test
    public void testIpPrefixCollapseIpv6SingleIPs()
    {
        assertFunction("IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '2620:10d:c090:400::5:a869/128', IPPREFIX '2620:10d:c091:400::5:a869/128'])",
                new ArrayType(IPPREFIX),
                ImmutableList.of("2620:10d:c090:400::5:a869/128", "2620:10d:c091:400::5:a869/128"));
    }

    @Test
    public void testIpPrefixCollapseSinglePrefixReturnsSamePrefix()
    {
        assertFunction("IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '192.168.0.0/22'])",
                new ArrayType(IPPREFIX),
                ImmutableList.of("192.168.0.0/22"));
    }

    @Test
    public void testIpPrefixCollapseOverlappingPrefixes()
    {
        assertFunction("IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '192.168.0.0/22', IPPREFIX '192.168.0.0/24'])",
                new ArrayType(IPPREFIX),
                ImmutableList.of("192.168.0.0/22"));
        assertFunction("IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '192.168.0.0/22', IPPREFIX '192.168.2.0/24'])",
                new ArrayType(IPPREFIX),
                ImmutableList.of("192.168.0.0/22"));
        assertFunction("IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '192.168.0.0/22', IPPREFIX '192.168.3.0/24'])",
                new ArrayType(IPPREFIX),
                ImmutableList.of("192.168.0.0/22"));
        assertFunction("IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '10.0.64.0/18', IPPREFIX '10.2.0.0/15', IPPREFIX '10.0.0.0/8', IPPREFIX '11.0.0.0/8', IPPREFIX '172.168.32.0/20', IPPREFIX '172.168.0.0/18'])",
                new ArrayType(IPPREFIX),
                ImmutableList.of("10.0.0.0/7", "172.168.0.0/18"));
        assertFunction("IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '10.0.0.0/8', IPPREFIX '10.0.0.0/7'])",
                new ArrayType(IPPREFIX),
                ImmutableList.of("10.0.0.0/7"));
    }

    @Test
    public void testIpPrefixCollapseEmptyArrayInput()
    {
        assertFunction("IP_PREFIX_COLLAPSE(CAST(ARRAY[] AS ARRAY(IPPREFIX)))", new ArrayType(IPPREFIX), ImmutableList.of());
    }

    @Test
    public void testIpPrefixCollapseNullInput()
    {
        assertFunction("IP_PREFIX_COLLAPSE(CAST(NULL AS ARRAY(IPPREFIX)))", new ArrayType(IPPREFIX), null);
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

    @Test (dataProvider = "private-ip-provider")
    public void testIsPrivateTrue(String ipAddress)
    {
        assertFunction("IS_PRIVATE_IP(IPADDRESS '" + ipAddress + "')", BOOLEAN, true);
    }

    @Test (dataProvider = "public-ip-provider")
    public void testIsPrivateIpFalse(String ipAddress)
    {
        assertFunction("IS_PRIVATE_IP(IPADDRESS '" + ipAddress + "')", BOOLEAN, false);
    }

    @Test
    public void testIsPrivateIpNull()
    {
        assertFunction("IS_PRIVATE_IP(NULL)", BOOLEAN, null);
    }

    @Test
    public void testIpPrefixSubnets()
    {
        assertFunction("IP_PREFIX_SUBNETS(IPPREFIX '192.168.1.0/24', 25)", new ArrayType(IPPREFIX), ImmutableList.of("192.168.1.0/25", "192.168.1.128/25"));
        assertFunction("IP_PREFIX_SUBNETS(IPPREFIX '192.168.0.0/24', 26)", new ArrayType(IPPREFIX), ImmutableList.of("192.168.0.0/26", "192.168.0.64/26", "192.168.0.128/26", "192.168.0.192/26"));
        assertFunction("IP_PREFIX_SUBNETS(IPPREFIX '2A03:2880:C000::/34', 37)",
                new ArrayType(IPPREFIX),
                ImmutableList.of("2a03:2880:c000::/37", "2a03:2880:c800::/37", "2a03:2880:d000::/37", "2a03:2880:d800::/37", "2a03:2880:e000::/37", "2a03:2880:e800::/37", "2a03:2880:f000::/37", "2a03:2880:f800::/37"));
    }

    @Test
    public void testIpPrefixSubnetsReturnSelf()
    {
        assertFunction("IP_PREFIX_SUBNETS(IPPREFIX '192.168.1.0/24', 24)", new ArrayType(IPPREFIX), ImmutableList.of("192.168.1.0/24"));
        assertFunction("IP_PREFIX_SUBNETS(IPPREFIX '2804:431:b000::/38', 38)", new ArrayType(IPPREFIX), ImmutableList.of("2804:431:b000::/38"));
    }

    @Test
    public void testIpPrefixSubnetsNewPrefixLengthLongerReturnsEmpty()
    {
        assertFunction("IP_PREFIX_SUBNETS(IPPREFIX '192.168.0.0/24', 23)", new ArrayType(IPPREFIX), ImmutableList.of());
        assertFunction("IP_PREFIX_SUBNETS(IPPREFIX '64:ff9b::17/64', 48)", new ArrayType(IPPREFIX), ImmutableList.of());
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
