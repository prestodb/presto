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

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;

public class TestIpPrefixFunctions
        extends AbstractTestNativeFunctions
        implements AbstractTestIpPrefix
{
    // IP_PREFIX() returns IPPREFIX, which Velox represents as ROW<IPADDRESS, TINYINT>.
    // The expression optimizer returns it as a ROW_CONSTRUCTOR special form rather than a
    // scalar ConstantExpression, so assertFunction cannot be used directly. assertIpPrefixFunction
    // handles the ROW_CONSTRUCTOR unwrapping.
    //
    // Velox maps all VELOX_USER_CHECK / VELOX_USER_FAIL errors to GENERIC_USER_ERROR
    // (kErrorSourceUser/kInvalidArgument → 0x00000000), whereas Presto Java uses
    // INVALID_FUNCTION_ARGUMENT (0x00000007) and INVALID_CAST_ARGUMENT for the same
    // failure cases.
    @Override
    @Test
    public void testIpAddressIpPrefix()
    {
        assertIpPrefixFunction("IP_PREFIX(IPADDRESS '1.2.3.4', 24)", "1.2.3.0/24");
        assertIpPrefixFunction("IP_PREFIX(IPADDRESS '1.2.3.4', 32)", "1.2.3.4/32");
        assertIpPrefixFunction("IP_PREFIX(IPADDRESS '1.2.3.4', 0)", "0.0.0.0/0");
        assertIpPrefixFunction("IP_PREFIX(IPADDRESS '::ffff:1.2.3.4', 24)", "1.2.3.0/24");
        assertIpPrefixFunction("IP_PREFIX(IPADDRESS '64:ff9b::17', 64)", "64:ff9b::/64");
        assertIpPrefixFunction("IP_PREFIX(IPADDRESS '64:ff9b::17', 127)", "64:ff9b::16/127");
        assertIpPrefixFunction("IP_PREFIX(IPADDRESS '64:ff9b::17', 128)", "64:ff9b::17/128");
        assertIpPrefixFunction("IP_PREFIX(IPADDRESS '64:ff9b::17', 0)", "::/0");
        assertInvalidFunction("IP_PREFIX(IPADDRESS '::ffff:1.2.3.4', -1)", GENERIC_USER_ERROR, "IPv4 subnet size must be in range [0, 32]");
        assertInvalidFunction("IP_PREFIX(IPADDRESS '::ffff:1.2.3.4', 33)", GENERIC_USER_ERROR, "IPv4 subnet size must be in range [0, 32]");
        assertInvalidFunction("IP_PREFIX(IPADDRESS '64:ff9b::10', -1)", GENERIC_USER_ERROR, "IPv6 subnet size must be in range [0, 128]");
        assertInvalidFunction("IP_PREFIX(IPADDRESS '64:ff9b::10', 129)", GENERIC_USER_ERROR, "IPv6 subnet size must be in range [0, 128]");
    }

    @Override
    @Test
    public void testStringIpPrefix()
    {
        assertIpPrefixFunction("IP_PREFIX('1.2.3.4', 24)", "1.2.3.0/24");
        assertIpPrefixFunction("IP_PREFIX('1.2.3.4', 32)", "1.2.3.4/32");
        assertIpPrefixFunction("IP_PREFIX('1.2.3.4', 0)", "0.0.0.0/0");
        assertIpPrefixFunction("IP_PREFIX('::ffff:1.2.3.4', 24)", "1.2.3.0/24");
        assertIpPrefixFunction("IP_PREFIX('64:ff9b::17', 64)", "64:ff9b::/64");
        assertIpPrefixFunction("IP_PREFIX('64:ff9b::17', 127)", "64:ff9b::16/127");
        assertIpPrefixFunction("IP_PREFIX('64:ff9b::17', 128)", "64:ff9b::17/128");
        assertIpPrefixFunction("IP_PREFIX('64:ff9b::17', 0)", "::/0");
        assertInvalidFunction("IP_PREFIX('::ffff:1.2.3.4', -1)", GENERIC_USER_ERROR, "IPv4 subnet size must be in range [0, 32]");
        assertInvalidFunction("IP_PREFIX('::ffff:1.2.3.4', 33)", GENERIC_USER_ERROR, "IPv4 subnet size must be in range [0, 32]");
        assertInvalidFunction("IP_PREFIX('64:ff9b::10', -1)", GENERIC_USER_ERROR, "IPv6 subnet size must be in range [0, 128]");
        assertInvalidFunction("IP_PREFIX('64:ff9b::10', 129)", GENERIC_USER_ERROR, "IPv6 subnet size must be in range [0, 128]");
        assertInvalidFunction("IP_PREFIX('localhost', 24)", GENERIC_USER_ERROR, "Cannot cast value to IPADDRESS: localhost");
        assertInvalidFunction("IP_PREFIX('64::ff9b::10', 24)", GENERIC_USER_ERROR, "Cannot cast value to IPADDRESS: 64::ff9b::10");
        assertInvalidFunction("IP_PREFIX('64:face:book::10', 24)", GENERIC_USER_ERROR, "Cannot cast value to IPADDRESS: 64:face:book::10");
        assertInvalidFunction("IP_PREFIX('123.456.789.012', 24)", GENERIC_USER_ERROR, "Cannot cast value to IPADDRESS: 123.456.789.012");
    }

    @Override
    @Test
    public void testIpPrefixCollapseNoNullPrefixesError()
    {
        assertInvalidFunction(
                "IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '192.168.0.0/22', CAST(NULL AS IPPREFIX)])",
                GENERIC_USER_ERROR,
                "ip_prefix_collapse does not support null elements");
    }

    @Override
    @Test
    public void testIpPrefixCollapseMixedIpVersionError()
    {
        assertInvalidFunction(
                "IP_PREFIX_COLLAPSE(ARRAY[IPPREFIX '192.168.0.0/22', IPPREFIX '2409:4043:251a:d200::/56'])",
                GENERIC_USER_ERROR,
                "All IPPREFIX elements must be the same IP version.");
    }

    @Override
    @Test
    public void testIpPrefixSubnetsInvalidPrefixLengths()
    {
        assertInvalidFunction("IP_PREFIX_SUBNETS(IPPREFIX '192.168.0.0/24', -1)", GENERIC_USER_ERROR, "Invalid prefix length for IPv4: -1");
        assertInvalidFunction("IP_PREFIX_SUBNETS(IPPREFIX '192.168.0.0/24', 33)", GENERIC_USER_ERROR, "Invalid prefix length for IPv4: 33");
        assertInvalidFunction("IP_PREFIX_SUBNETS(IPPREFIX '64:ff9b::17/64', -1)", GENERIC_USER_ERROR, "Invalid prefix length for IPv6: -1");
        assertInvalidFunction("IP_PREFIX_SUBNETS(IPPREFIX '64:ff9b::17/64', 129)", GENERIC_USER_ERROR, "Invalid prefix length for IPv6: 129");
    }
}
