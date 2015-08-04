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
package com.facebook.presto.spi;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHostAddress
{
    @Test
    public void testEquality()
            throws Exception
    {
        HostAddress address1 = HostAddress.fromParts("[1111:2222:3333:4444:5555:6666:7777:8888]", 1234);
        HostAddress address1NoBrackets = HostAddress.fromParts("1111:2222:3333:4444:5555:6666:7777:8888", 1234);
        Assert.assertEquals(address1, address1NoBrackets);

        HostAddress address1FromString = HostAddress.fromString("[1111:2222:3333:4444:5555:6666:7777:8888]:1234");
        Assert.assertEquals(address1, address1FromString);

        HostAddress address2 = HostAddress.fromParts("[1111:2222:3333:4444:5555:6666:7777:9999]", 1234);
        Assert.assertNotEquals(address1, address2);

        HostAddress address3 = HostAddress.fromParts("[1111:2222:3333:4444:5555:6666:7777:8888]", 1235);
        Assert.assertNotEquals(address1, address3);
    }

    @Test
    public void testRoundTrip()
            throws Exception
    {
        HostAddress address = HostAddress.fromParts("[1111:2222:3333:4444:5555:6666:7777:8888]", 1234);
        HostAddress fromParts = HostAddress.fromParts(address.getHostText(), address.getPort());
        Assert.assertEquals(address, fromParts);

        HostAddress fromString = HostAddress.fromString(address.toString());
        Assert.assertEquals(address, fromString);

        Assert.assertEquals(fromParts, fromString);
    }
}
