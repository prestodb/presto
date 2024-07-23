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
package com.facebook.presto.operator.scalar.sql;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;

public class TestIPSqlFunctions
        extends AbstractTestFunctions
{
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

    @Test (dataProvider = "private-ip-provider")
    public void testIsPrivateTrue(String ipAddress)
    {
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '" + ipAddress + "')",
                BOOLEAN,
                true);
    }

    @Test
    public void testIsReservedFalse()
    {
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '157.240.200.99')",
                BOOLEAN,
                false);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '2a03:2880:f031:12:face:b00c:0:2')",
                BOOLEAN,
                false);
    }

    @Test
    public void testIsReservedIpNull()
    {
        assertFunction(
                "IS_PRIVATE_IP(NULL)",
                BOOLEAN,
                null);
    }
}
