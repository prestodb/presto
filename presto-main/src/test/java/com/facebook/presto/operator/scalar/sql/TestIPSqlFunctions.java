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
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;

public class TestIPSqlFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testIsPrivateTrue()
    {
        /*
         * IPv4
         */

        // 0.0.0.0/8 RFC1122: "This host on this network"
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '0.0.0.0')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '0.255.255.255')",
                BOOLEAN,
                true);

        // 10.0.0.0/8 RFC1918: Private-Use
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '10.0.0.0')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '10.255.255.255')",
                BOOLEAN,
                true);

        // 100.64.0.0/10 RFC6598: Shared Address Space
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '100.64.0.0')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '100.127.255.255')",
                BOOLEAN,
                true);

        // 127.0.0.0/8 RFC1122: Loopback
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '127.0.0.0')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '127.255.255.255')",
                BOOLEAN,
                true);

        // 169.254.0.0/16 RFC3927: Link Local
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '169.254.0.0')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '169.254.255.255')",
                BOOLEAN,
                true);

        // 172.16.0.0/12 RFC1918: Private-Use
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '172.16.0.0')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '172.31.255.255')",
                BOOLEAN,
                true);

        // 192.0.0.0/24 RFC6890: IETF Protocol Assignments
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '192.0.0.0')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '192.0.0.255')",
                BOOLEAN,
                true);

        // 192.0.2.0/24 RFC5737: Documentation (TEST-NET-1)
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '192.0.2.0')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '192.0.2.255')",
                BOOLEAN,
                true);

        // 192.88.99.0/24 RFC3068: 6to4 Relay anycast
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '192.88.99.0')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '192.88.99.255')",
                BOOLEAN,
                true);

        // 192.168.0.0/16 RFC1918: Private-Use
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '192.168.0.0')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '192.168.255.255')",
                BOOLEAN,
                true);

        // 198.18.0.0/15 RFC2544: Benchmarking
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '198.18.0.0')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '198.19.255.255')",
                BOOLEAN,
                true);

        // 198.51.100.0/24 RFC5737: Documentation (TEST-NET-2)
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '198.51.100.0')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '198.51.100.255')",
                BOOLEAN,
                true);

        // 203.0.113.0/24 RFC5737: Documentation (TEST-NET-3)
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '203.0.113.0')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '203.0.113.255')",
                BOOLEAN,
                true);

        // 240.0.0.0/4 RFC1112: Reserved
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '240.0.0.0')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '255.255.255.255')",
                BOOLEAN,
                true);


        // ::/128 RFC4291: Loopback and Unspecified address
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '::')",
                BOOLEAN,
                true);

        // ::1/128 RFC4291: Loopback and Unspecified address
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '::1')",
                BOOLEAN,
                true);

        // 100::/64 RFC6666: Discard-Only Address Block
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '100::')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '100::ffff:ffff:ffff:ffff')",
                BOOLEAN,
                true);

        // 64:ff9b:1::/48 RFC8215: IPv4-IPv6 Translation
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '64:ff9b:1::')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '64:ff9b:1:ffff:ffff:ffff:ffff:ffff')",
                BOOLEAN,
                true);

        // 2001:2::/48 RFC5180,RFC Errata 1752: Benchmarking
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '2001:2::')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '2001:2:0:ffff:ffff:ffff:ffff:ffff')",
                BOOLEAN,
                true);

        // 2001:db8::/32 RFC3849: Documentation
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '2001:db8::')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '2001:db8:ffff:ffff:ffff:ffff:ffff:ffff')",
                BOOLEAN,
                true);

        // 2001::/23 RFC2928: IETF Protocol Assignments
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '2001::')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '2001:1ff:ffff:ffff:ffff:ffff:ffff:ffff')",
                BOOLEAN,
                true);

        // 5f00::/16 RFC-ietf-6man-sids-06: Segment Routing (SRv6)
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '5f00::')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '5f00:ffff:ffff:ffff:ffff:ffff:ffff:ffff')",
                BOOLEAN,
                true);

        // fe80::/10 RFC4291: Link-Local Unicast
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS 'fe80::')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS 'febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff')",
                BOOLEAN,
                true);

        // fc00::/7 RFC4193, RFC8190: Unique Local
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS 'fc00::')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS 'fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')",
                BOOLEAN,
                true);


        // test a few IP addresses in the middle of the ranges
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '10.1.2.3')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '100.64.3.2')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '192.168.55.99')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '2001:0DB8:0000:0000:face:b00c:0000:0000')",
                BOOLEAN,
                true);
        assertFunction(
                "IS_PRIVATE_IP(IPADDRESS '0100:0000:0000:0000:ffff:ffff:0000:0000')",
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
