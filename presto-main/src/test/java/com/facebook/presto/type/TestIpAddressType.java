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
package com.facebook.presto.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.type.IpAddressType.IPADDRESS;
import static org.testng.Assert.assertEquals;

public class TestIpAddressType
        extends AbstractTestType
{
    public TestIpAddressType()
    {
        super(IPADDRESS, String.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = IPADDRESS.createBlockBuilder(null, 1);
        IPADDRESS.writeSlice(blockBuilder, getSliceForAddress("2001:db8::ff00:42:8320"));
        IPADDRESS.writeSlice(blockBuilder, getSliceForAddress("2001:db8::ff00:42:8321"));
        IPADDRESS.writeSlice(blockBuilder, getSliceForAddress("2001:db8::ff00:42:8322"));
        IPADDRESS.writeSlice(blockBuilder, getSliceForAddress("2001:db8::ff00:42:8323"));
        IPADDRESS.writeSlice(blockBuilder, getSliceForAddress("2001:db8::ff00:42:8324"));
        IPADDRESS.writeSlice(blockBuilder, getSliceForAddress("2001:db8::ff00:42:8325"));
        IPADDRESS.writeSlice(blockBuilder, getSliceForAddress("2001:db8::ff00:42:8326"));
        IPADDRESS.writeSlice(blockBuilder, getSliceForAddress("2001:db8::ff00:42:8327"));
        IPADDRESS.writeSlice(blockBuilder, getSliceForAddress("2001:db8::ff00:42:8328"));
        IPADDRESS.writeSlice(blockBuilder, getSliceForAddress("2001:db8::ff00:42:8329"));
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        byte[] address = ((Slice) value).getBytes();
        address[0] = (byte) 0xff;
        address[15] = (byte) 0x0;
        return Slices.wrappedBuffer(address);
    }

    @Override
    protected Object getNonNullValue()
    {
        return Slices.wrappedBuffer(InetAddresses.forString("::").getAddress());
    }

    @Test
    public void testDisplayName()
    {
        assertEquals((IPADDRESS).getDisplayName(), "ipaddress");
    }

    private static Slice getSliceForAddress(String address)
    {
        return Slices.wrappedBuffer(InetAddresses.forString(address).getAddress());
    }
}
