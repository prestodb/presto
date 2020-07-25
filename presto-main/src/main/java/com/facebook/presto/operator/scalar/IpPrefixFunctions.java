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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.type.IpAddressOperators.between;
import static com.facebook.presto.type.IpAddressOperators.castFromVarcharToIpAddress;
import static com.facebook.presto.type.IpAddressType.IPADDRESS;
import static com.facebook.presto.type.IpPrefixOperators.castFromIpPrefixToIpAddress;
import static com.facebook.presto.type.IpPrefixOperators.castFromVarcharToIpPrefix;
import static com.facebook.presto.type.IpPrefixType.IPPREFIX;
import static com.facebook.presto.util.Failures.checkCondition;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;

public final class IpPrefixFunctions
{
    private IpPrefixFunctions() {}

    @Description("IP prefix for a given IP address and subnet size")
    @ScalarFunction("ip_prefix")
    @SqlType(StandardTypes.IPPREFIX)
    public static Slice ipPrefix(@SqlType(StandardTypes.IPADDRESS) Slice value, @SqlType(StandardTypes.BIGINT) long subnetSize)
    {
        InetAddress address = toInetAddress(value);
        int addressLength = address.getAddress().length;
        if (addressLength == 4) {
            checkCondition(0 <= subnetSize && subnetSize <= 32, INVALID_FUNCTION_ARGUMENT, "IPv4 subnet size must be in range [0, 32]");
        }
        else if (addressLength == 16) {
            checkCondition(0 <= subnetSize && subnetSize <= 128, INVALID_FUNCTION_ARGUMENT, "IPv6 subnet size must be in range [0, 128]");
        }
        else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Invalid InetAddress length: " + addressLength);
        }

        return castFromVarcharToIpPrefix(utf8Slice(InetAddresses.toAddrString(address) + "/" + subnetSize));
    }

    @Description("IP prefix for a given IP address and subnet size")
    @ScalarFunction("ip_prefix")
    @LiteralParameters("x")
    @SqlType(StandardTypes.IPPREFIX)
    public static Slice stringIpPrefix(@SqlType("varchar(x)") Slice slice, @SqlType(StandardTypes.BIGINT) long subnetSize)
    {
        return ipPrefix(castFromVarcharToIpAddress(slice), subnetSize);
    }

    @Description("Smallest IP address for a given IP prefix")
    @ScalarFunction("ip_subnet_min")
    @SqlType(StandardTypes.IPADDRESS)
    public static Slice ipSubnetMin(@SqlType(StandardTypes.IPPREFIX) Slice value)
    {
        return castFromIpPrefixToIpAddress(value);
    }

    @Description("Largest IP address for a given IP prefix")
    @ScalarFunction("ip_subnet_max")
    @SqlType(StandardTypes.IPADDRESS)
    public static Slice ipSubnetMax(@SqlType(StandardTypes.IPPREFIX) Slice value)
    {
        byte[] address = toInetAddress(value.slice(0, IPADDRESS.getFixedSize())).getAddress();
        int subnetSize = value.getByte(IPPREFIX.getFixedSize() - 1) & 0xFF;

        if (address.length == 4) {
            for (int i = 0; i < 4; i++) {
                address[3 - i] |= (byte) ~(~0 << min(max((32 - subnetSize) - 8 * i, 0), 8));
            }
            byte[] bytes = new byte[16];
            bytes[10] = (byte) 0xff;
            bytes[11] = (byte) 0xff;
            arraycopy(address, 0, bytes, 12, 4);
            address = bytes;
        }
        else if (address.length == 16) {
            for (int i = 0; i < 16; i++) {
                address[15 - i] |= (byte) ~(~0 << min(max((128 - subnetSize) - 8 * i, 0), 8));
            }
        }
        return wrappedBuffer(address);
    }

    @Description("Array of smallest and largest IP address in the subnet of the given IP prefix")
    @ScalarFunction("ip_subnet_range")
    @SqlType("array(IPADDRESS)")
    public static Block ipSubnetRange(@SqlType(StandardTypes.IPPREFIX) Slice value)
    {
        BlockBuilder blockBuilder = IPADDRESS.createBlockBuilder(null, 2);
        IPADDRESS.writeSlice(blockBuilder, ipSubnetMin(value));
        IPADDRESS.writeSlice(blockBuilder, ipSubnetMax(value));
        return blockBuilder.build();
    }

    @Description("Is the IP address in the subnet of IP prefix")
    @ScalarFunction("is_subnet_of")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isSubnetOf(@SqlType(StandardTypes.IPPREFIX) Slice ipPrefix, @SqlType(StandardTypes.IPADDRESS) Slice ipAddress)
    {
        toInetAddress(ipAddress);
        return between(ipAddress, ipSubnetMin(ipPrefix), ipSubnetMax(ipPrefix));
    }

    @Description("Is the second IP prefix in the subnet of the first IP prefix")
    @ScalarFunction("is_subnet_of")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isPrefixSubnetOf(@SqlType(StandardTypes.IPPREFIX) Slice first, @SqlType(StandardTypes.IPPREFIX) Slice second)
    {
        return between(ipSubnetMin(second), ipSubnetMin(first), ipSubnetMax(first)) && between(ipSubnetMax(second), ipSubnetMin(first), ipSubnetMax(first));
    }

    private static InetAddress toInetAddress(Slice ipAddress)
    {
        try {
            return InetAddress.getByAddress(ipAddress.getBytes());
        }
        catch (UnknownHostException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid IP address binary: " + ipAddress.toStringUtf8(), e);
        }
    }
}
