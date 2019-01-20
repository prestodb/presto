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
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.IpAddressOperators;
import com.facebook.presto.util.CIDR;
import io.airlift.slice.Slice;

import java.net.UnknownHostException;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.type.IpAddressOperators.castFromVarcharToIpAddress;
import static io.airlift.slice.Slices.utf8Slice;

public final class IpAddressFunctions
{
    private IpAddressFunctions() {}

    @Description("Determines whether given IP address exists in the CIDR")
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean contains(@SqlType(StandardTypes.VARCHAR) Slice cidrSlice, @SqlType(StandardTypes.IPADDRESS) Slice address)
    {
        String format = cidrSlice.toStringUtf8();
        CIDR cidr;
        try {
            cidr = CIDR.newCIDR(format);
        }
        catch (UnknownHostException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid CIDR value: " + format);
        }
        if (cidr.getMask() == 0) {
            return true;
        }

        String baseAddress = cidr.getBaseAddress().getHostAddress();
        Slice min = castFromVarcharToIpAddress(utf8Slice(baseAddress));

        String endAddress = cidr.getEndAddress().getHostAddress();
        Slice max = castFromVarcharToIpAddress(utf8Slice(endAddress));

        return IpAddressOperators.between(address, min, max);
    }
}
