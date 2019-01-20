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

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;

public class TestIpAddressFunction
        extends AbstractTestFunctions
{
    @Test
    public void testIpAddressContains()
    {
        assertFunction("contains('10.0.0.1/0', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('10.0.0.1/0', IPADDRESS '255.255.255.255')", BOOLEAN, true);

        assertFunction("contains('10.0.0.1/8', IPADDRESS '9.255.255.255')", BOOLEAN, false);
        assertFunction("contains('10.0.0.1/8', IPADDRESS '10.0.0.0')", BOOLEAN, true);
        assertFunction("contains('10.0.0.1/8', IPADDRESS '10.255.255.255')", BOOLEAN, true);
        assertFunction("contains('10.0.0.1/8', IPADDRESS '11.255.255.255')", BOOLEAN, false);

        assertFunction("contains('127.0.0.1/32', IPADDRESS '127.0.0.1')", BOOLEAN, true);
        assertFunction("contains('127.0.0.1/32', IPADDRESS '127.0.0.2')", BOOLEAN, false);

        assertFunction("contains('::ffff:1.2.3.4/0', IPADDRESS '::0:0:0:0:0:0')", BOOLEAN, true);
        assertFunction("contains('::ffff:1.2.3.4/0', IPADDRESS '::f:f:f:f:f:f')", BOOLEAN, true);

        assertFunction("contains('64:ff9b::10.0.0.0/64', IPADDRESS '64:ff9a:f:f:f:f:f:f')", BOOLEAN, false);
        assertFunction("contains('64:ff9b::10.0.0.0/64', IPADDRESS '64:ff9b:0:0:0:0:0:0')", BOOLEAN, true);
        assertFunction("contains('64:ff9b::10.0.0.0/64', IPADDRESS '64:ff9b:0:0:f:f:f:f')", BOOLEAN, true);
        assertFunction("contains('64:ff9b::10.0.0.0/64', IPADDRESS '64:ff9b:0:1:0:0:0:0')", BOOLEAN, false);

        assertFunction("contains('2001:0db8:0:0:0:ff00:0042:8329/128', IPADDRESS '2001:0db8:0:0:0:ff00:0042:8329')", BOOLEAN, true);
        assertFunction("contains('2001:0db8:0:0:0:ff00:0042:8329/128', IPADDRESS '2001:0db8:0:0:0:ff00:0042:8330')", BOOLEAN, false);

        assertFunction("contains('10.0.0.1/0', cast(NULL as IPADDRESS))", BOOLEAN, null);
        assertFunction("contains('::ffff:1.2.3.4/0', cast(NULL as IPADDRESS))", BOOLEAN, null);

        assertInvalidFunction("contains('10.0.0.1/-1', IPADDRESS '0.0.0.0')", "Invalid CIDR value: 10.0.0.1/-1");
        assertInvalidFunction("contains('10.0.0.1/33', IPADDRESS '0.0.0.0')", "Invalid CIDR value: 10.0.0.1/33");
        assertInvalidFunction("contains('64:ff9b::10.0.0.0/-1', IPADDRESS '0.0.0.0')", "Invalid CIDR value: 64:ff9b::10.0.0.0/-1");
        assertInvalidFunction("contains('64:ff9b::10.0.0.0/129', IPADDRESS '0.0.0.0')", "Invalid CIDR value: 64:ff9b::10.0.0.0/129");
        assertInvalidFunction("contains('x', IPADDRESS '0.0.0.0')", "Invalid CIDR value: x");
    }
}
