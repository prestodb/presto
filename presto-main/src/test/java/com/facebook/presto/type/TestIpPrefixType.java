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

import static com.facebook.presto.type.IpPrefixType.IPPREFIX;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.System.arraycopy;
import static org.testng.Assert.assertEquals;

public class TestIpPrefixType
        extends AbstractTestType
{
    public TestIpPrefixType()
    {
        super(IPPREFIX, String.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = IPPREFIX.createBlockBuilder(null, 1);
        IPPREFIX.writeSlice(blockBuilder, getSliceForPrefix("2001:db8::ff00:42:8320/48"));
        IPPREFIX.writeSlice(blockBuilder, getSliceForPrefix("2001:db8::ff00:42:8321/48"));
        IPPREFIX.writeSlice(blockBuilder, getSliceForPrefix("2001:db8::ff00:42:8322/48"));
        IPPREFIX.writeSlice(blockBuilder, getSliceForPrefix("2001:db8::ff00:42:8323/48"));
        IPPREFIX.writeSlice(blockBuilder, getSliceForPrefix("2001:db8::ff00:42:8324/48"));
        IPPREFIX.writeSlice(blockBuilder, getSliceForPrefix("2001:db8::ff00:42:8325/48"));
        IPPREFIX.writeSlice(blockBuilder, getSliceForPrefix("2001:db8::ff00:42:8326/48"));
        IPPREFIX.writeSlice(blockBuilder, getSliceForPrefix("2001:db8::ff00:42:8327/48"));
        IPPREFIX.writeSlice(blockBuilder, getSliceForPrefix("2001:db8::ff00:42:8328/48"));
        IPPREFIX.writeSlice(blockBuilder, getSliceForPrefix("2001:db8::ff00:42:8329/48"));
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        byte[] prefix = ((Slice) value).getBytes();
        checkState(++prefix[prefix.length - 2] != 0, "Last byte of address is 0xff");
        return Slices.wrappedBuffer(prefix);
    }

    @Override
    protected Object getNonNullValue()
    {
        byte[] bytes = new byte[IPPREFIX.getFixedSize()];
        byte[] ipAddress = InetAddresses.forString("::").getAddress();
        arraycopy(ipAddress, 0, bytes, 0, 16);
        bytes[IPPREFIX.getFixedSize() - 1] = (byte) 0x20;
        return Slices.wrappedBuffer(bytes);
    }

    @Test
    public void testDisplayName()
    {
        assertEquals((IPPREFIX).getDisplayName(), "ipprefix");
    }

    private static Slice getSliceForPrefix(String prefix)
    {
        String[] parts = prefix.split("/");
        byte[] bytes = new byte[IPPREFIX.getFixedSize()];
        byte[] ipAddress = InetAddresses.forString(parts[0]).getAddress();
        arraycopy(ipAddress, 0, bytes, 0, 16);
        bytes[IPPREFIX.getFixedSize() - 1] = (byte) Integer.parseInt(parts[1]);
        return Slices.wrappedBuffer(bytes);
    }
}
