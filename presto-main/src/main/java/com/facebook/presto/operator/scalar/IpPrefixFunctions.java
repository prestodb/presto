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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.type.IpAddressOperators.castFromVarcharToIpAddress;
import static com.facebook.presto.type.IpPrefixOperators.castFromVarcharToIpPrefix;
import static com.facebook.presto.util.Failures.checkCondition;
import static io.airlift.slice.Slices.utf8Slice;

public final class IpPrefixFunctions
{
    private IpPrefixFunctions() {}

    @Description("IP prefix for a given IP address and subnet size")
    @ScalarFunction("ip_prefix")
    @SqlType(StandardTypes.IPPREFIX)
    public static Slice ipPrefix(@SqlType(StandardTypes.IPADDRESS) Slice value, @SqlType(StandardTypes.BIGINT) long subnetSize)
    {
        InetAddress address;
        try {
            address = InetAddress.getByAddress(value.getBytes());
        }
        catch (UnknownHostException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid IP address binary: " + value.toStringUtf8(), e);
        }

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
}
