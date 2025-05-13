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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.facebook.presto.common.function.OperatorType.BETWEEN;
import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.function.OperatorType.XX_HASH_64;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.type.IpAddressType.IPADDRESS;
import static com.facebook.presto.type.IpPrefixType.IPPREFIX;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;

public final class IpPrefixOperators
{
    private IpPrefixOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType(StandardTypes.IPPREFIX) Slice left, @SqlType(StandardTypes.IPPREFIX) Slice right)
    {
        return left.equals(right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType(StandardTypes.IPPREFIX) Slice left, @SqlType(StandardTypes.IPPREFIX) Slice right)
    {
        return !left.equals(right);
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean lessThan(@SqlType(StandardTypes.IPPREFIX) Slice left, @SqlType(StandardTypes.IPPREFIX) Slice right)
    {
        return left.compareTo(right) < 0;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean lessThanOrEqual(@SqlType(StandardTypes.IPPREFIX) Slice left, @SqlType(StandardTypes.IPPREFIX) Slice right)
    {
        return left.compareTo(right) <= 0;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean greaterThan(@SqlType(StandardTypes.IPPREFIX) Slice left, @SqlType(StandardTypes.IPPREFIX) Slice right)
    {
        return left.compareTo(right) > 0;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean greaterThanOrEqual(@SqlType(StandardTypes.IPPREFIX) Slice left, @SqlType(StandardTypes.IPPREFIX) Slice right)
    {
        return left.compareTo(right) >= 0;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean between(@SqlType(StandardTypes.IPPREFIX) Slice value, @SqlType(StandardTypes.IPPREFIX) Slice min, @SqlType(StandardTypes.IPPREFIX) Slice max)
    {
        return min.compareTo(value) <= 0 && value.compareTo(max) <= 0;
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.IPPREFIX) Slice value)
    {
        return XxHash64.hash(value);
    }

    @ScalarOperator(XX_HASH_64)
    @SqlType(StandardTypes.BIGINT)
    public static long xxHash64(@SqlType(StandardTypes.IPPREFIX) Slice value)
    {
        return XxHash64.hash(value);
    }

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.IPPREFIX)
    public static Slice castFromVarcharToIpPrefix(@SqlType("varchar(x)") Slice slice)
    {
        byte[] address;
        int subnetSize;
        String string = slice.toStringUtf8();
        if (!string.contains("/")) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Cannot cast value to IPPREFIX: " + slice.toStringUtf8());
        }
        String[] parts = string.split("/");
        try {
            address = InetAddresses.forString(parts[0]).getAddress();
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Cannot cast value to IPPREFIX: " + slice.toStringUtf8());
        }
        subnetSize = Integer.parseInt(parts[1]);

        byte[] bytes = new byte[IPPREFIX.getFixedSize()];
        if (address.length == 4) {
            if (subnetSize < 0 || 32 < subnetSize) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, "Cannot cast value to IPPREFIX: " + slice.toStringUtf8());
            }

            for (int i = 0; i < 4; i++) {
                address[3 - i] &= -0x1 << min(max((32 - subnetSize) - 8 * i, 0), 8);
            }
            bytes[10] = (byte) 0xff;
            bytes[11] = (byte) 0xff;
            arraycopy(address, 0, bytes, 12, 4);
            bytes[IPPREFIX.getFixedSize() - 1] = (byte) subnetSize;
        }
        else if (address.length == 16) {
            if (subnetSize < 0 || 128 < subnetSize) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, "Cannot cast value to IPPREFIX: " + slice.toStringUtf8());
            }

            for (int i = 0; i < 16; i++) {
                address[15 - i] &= (byte) -0x1 << min(max((128 - subnetSize) - 8 * i, 0), 8);
            }
            arraycopy(address, 0, bytes, 0, 16);
            bytes[IPPREFIX.getFixedSize() - 1] = (byte) subnetSize;
        }
        else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Invalid InetAddress length: " + address.length);
        }

        return wrappedBuffer(bytes);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castFromIpPrefixToVarchar(@SqlType(StandardTypes.IPPREFIX) Slice slice)
    {
        try {
            String addrString = InetAddresses.toAddrString(InetAddress.getByAddress(slice.getBytes(0, 2 * Long.BYTES)));
            String prefixString = Integer.toString(slice.getByte(2 * Long.BYTES) & 0xff);
            return utf8Slice(addrString + "/" + prefixString);
        }
        catch (UnknownHostException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Invalid IP address binary length: " + slice.length(), e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.IPADDRESS)
    public static Slice castFromIpPrefixToIpAddress(@SqlType(StandardTypes.IPPREFIX) Slice slice)
    {
        return wrappedBuffer(slice.getBytes(0, IPADDRESS.getFixedSize()));
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.IPPREFIX)
    public static Slice castFromIpAddressToIpPrefix(@SqlType(StandardTypes.IPADDRESS) Slice slice)
    {
        byte[] address;
        try {
            address = InetAddress.getByAddress(slice.getBytes()).getAddress();
        }
        catch (UnknownHostException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Invalid IP address binary: " + slice.toStringUtf8(), e);
        }

        byte[] bytes = new byte[IPPREFIX.getFixedSize()];
        arraycopy(slice.getBytes(), 0, bytes, 0, IPPREFIX.getFixedSize() - 1);
        if (address.length == 4) {
            bytes[IPPREFIX.getFixedSize() - 1] = (byte) 32;
        }
        else if (address.length == 16) {
            bytes[IPPREFIX.getFixedSize() - 1] = (byte) 128;
        }
        else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Invalid InetAddress length: " + address.length);
        }

        return wrappedBuffer(bytes);
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    public static class IpPrefixDistinctFromOperator
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @SqlType(StandardTypes.IPPREFIX) Slice left,
                @IsNull boolean leftNull,
                @SqlType(StandardTypes.IPPREFIX) Slice right,
                @IsNull boolean rightNull)
        {
            if (leftNull != rightNull) {
                return true;
            }
            if (leftNull) {
                return false;
            }
            return notEqual(left, right);
        }

        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @BlockPosition @SqlType(value = StandardTypes.IPPREFIX, nativeContainerType = Slice.class) Block left,
                @BlockIndex int leftPosition,
                @BlockPosition @SqlType(value = StandardTypes.IPPREFIX, nativeContainerType = Slice.class) Block right,
                @BlockIndex int rightPosition)
        {
            if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
                return true;
            }
            if (left.isNull(leftPosition)) {
                return false;
            }
            return left.compareTo(leftPosition, 0, IPPREFIX.getFixedSize(), right, rightPosition, 0, IPPREFIX.getFixedSize()) != 0;
        }
    }

    @ScalarOperator(INDETERMINATE)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean indeterminate(@SqlType(StandardTypes.IPPREFIX) Slice value, @IsNull boolean isNull)
    {
        return isNull;
    }
}
