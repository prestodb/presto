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

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.SqlInvokedScalarFunction;
import com.facebook.presto.spi.function.SqlParameter;
import com.facebook.presto.spi.function.SqlType;

public class IPSqlFunctions
{
    private IPSqlFunctions() {}

    @SqlInvokedScalarFunction(value = "is_private_ip", deterministic = true, calledOnNullInput = false)
    @Description("Returns whether ip is a private or reserved IP address that is not globally reachable.")
    @SqlParameter(name = "ip", type = "IPADDRESS")
    @SqlType("boolean")
    public static String isPrivateIpAddress()
    {
        /*
         We take our definitions from what IANA considers *not* "globally reachable" in
         https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry.xhtml and
         https://www.iana.org/assignments/iana-ipv6-special-registry/iana-ipv6-special-registry.xhtml.
        */
        return "RETURN " +
                "IF(STRPOS(CAST(ip AS VARCHAR), '.') > 0, " +  // if IPv4

        /*
         Creating IPPREFIX objects are relatively expensive, so we prefer to
         perform the cheaper STARTS_WITH prefix check when possible.
         Note that '.'s are only trailing octets of length less than 3.
        */
                "STARTS_WITH(CAST(ip AS VARCHAR), '0.') OR " +           // RFC1122: "This host on this network"
                "STARTS_WITH(CAST(ip AS VARCHAR), '10.') OR " +          // RFC1918: Private-Use
                "STARTS_WITH(CAST(ip AS VARCHAR), '127') OR " +          // RFC1122: Loopback
                "STARTS_WITH(CAST(ip AS VARCHAR), '169.254') OR " +      // RFC3927: Link Local
                "STARTS_WITH(CAST(ip AS VARCHAR), '192.0.0.') OR " +     // RFC6890: IETF Protocol Assignments
                "STARTS_WITH(CAST(ip AS VARCHAR), '192.0.2.') OR " +     // RFC5737: Documentation (TEST-NET-1)
                "STARTS_WITH(CAST(ip AS VARCHAR), '192.88.99.') OR " +   // RFC3068: 6to4 Relay anycast
                "STARTS_WITH(CAST(ip AS VARCHAR), '192.168') OR " +      // RFC1918: Private-Use
                "STARTS_WITH(CAST(ip AS VARCHAR), '198.51.100') OR " +   // RFC5737: Documentation (TEST-NET-2)
                "STARTS_WITH(CAST(ip AS VARCHAR), '203.0.113') OR " +    // RFC5737: Documentation (TEST-NET-3)

                "TRY_CAST(IP_PREFIX(ip, 10) AS VARCHAR) = '100.64.0.0/10' OR " +    // RFC6598: Shared Address Space
                "TRY_CAST(IP_PREFIX(ip, 12) AS VARCHAR) = '172.16.0.0/12' OR " +    // RFC1918: Private-Use
                "TRY_CAST(IP_PREFIX(ip, 15) AS VARCHAR) = '198.18.0.0/15' OR " +    // RFC2544: Benchmarking
                "TRY_CAST(IP_PREFIX(ip, 4) AS VARCHAR) = '240.0.0.0/4' " +   // RFC1112: Multicast, RFC1112: Reserved, RFC0919: Limited Broadcast

                ", " + // ends the first (IPv4) condition of the IF block

                // IPv6 conditions
                "TRY_CAST(IP_PREFIX(ip, 128) AS VARCHAR) IN ('::/128','::1/128') OR " +     // RFC4291: Loopback and Unspecified address
                "TRY_CAST(IP_PREFIX(ip, 96) AS VARCHAR) = '::ffff:0:0/96' OR " +            // RFC4291: IPv4-mapped Address
                "TRY_CAST(IP_PREFIX(ip, 64) AS VARCHAR) = '100::/64' OR " +                 // RFC6666: Discard-Only Address Block
                "TRY_CAST(IP_PREFIX(ip, 48) AS VARCHAR) IN ('64:ff9b:1::/48','2001:2::/48') OR " +  // RFC8215: IPv4-IPv6 Translation and RFC5180,RFC Errata 1752: Benchmarking
                "TRY_CAST(IP_PREFIX(ip, 32) AS VARCHAR) = '2001:db8::/32' OR " +            // RFC3849: Documentation
                "TRY_CAST(IP_PREFIX(ip, 23) AS VARCHAR) = '2001::/23' OR " +                // RFC2928: IETF Protocol Assignments
                "TRY_CAST(IP_PREFIX(ip, 16) AS VARCHAR) = '5f00::/16' OR " +                // RFC-ietf-6man-sids-06: Segment Routing (SRv6)
                "TRY_CAST(IP_PREFIX(ip, 10) AS VARCHAR) = 'fe80::/10' OR " +                // RFC4291: Link-Local Unicast
                "TRY_CAST(IP_PREFIX(ip, 7) AS VARCHAR) = 'fc00::/7'" +                      // RFC4193, RFC8190: Unique Local

                ")";  // closes IF condition
    }
}
