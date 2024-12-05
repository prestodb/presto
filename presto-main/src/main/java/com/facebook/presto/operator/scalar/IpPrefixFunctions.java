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

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static com.facebook.presto.operator.scalar.ArraySortFunction.sort;
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
    private static final BigInteger TWO = BigInteger.valueOf(2);

    private static final Block EMPTY_BLOCK = IPPREFIX.createBlockBuilder(null, 0).build();

    /**
     * Our definitions for what IANA considers not "globally reachable" are taken from the docs at
     * https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry.xhtml and
     * https://www.iana.org/assignments/iana-ipv6-special-registry/iana-ipv6-special-registry.xhtml.
     * Java's InetAddress.isSiteLocalAddress only covers three of these: 10.0.0.0/8, 172.16.0.0/12, and 192.168.0.0/16,
     * so we operate over the complete list below.
     */
    private static final String[] privatePrefixes = new String[] {
            // IPv4 private ranges
            "0.0.0.0/8",        // RFC1122: "This host on this network"
            "10.0.0.0/8",       // RFC1918: Private-Use
            "100.64.0.0/10",    // RFC6598: Shared Address Space
            "127.0.0.0/8",      // RFC1122: Loopback
            "169.254.0.0/16",   // RFC3927: Link Local
            "172.16.0.0/12",    // RFC1918: Private-Use
            "192.0.0.0/24",     // RFC6890: IETF Protocol Assignments
            "192.0.2.0/24",     // RFC5737: Documentation (TEST-NET-1)
            "192.88.99.0/24",   // RFC3068: 6to4 Relay anycast
            "192.168.0.0/16",   // RFC1918: Private-Use
            "198.18.0.0/15",    // RFC2544: Benchmarking
            "198.51.100.0/24",  // RFC5737: Documentation (TEST-NET-2)
            "203.0.113.0/24",   // RFC5737: Documentation (TEST-NET-3)
            "240.0.0.0/4",      // RFC1112: Reserved
            // IPv6 private ranges
            "::/127",           // RFC4291: Unspecified address and Loopback address
            "64:ff9b:1::/48",   // RFC8215: IPv4-IPv6 Translation
            "100::/64",         // RFC6666: Discard-Only Address Block
            "2001:2::/48",      // RFC5180, RFC Errata 1752: Benchmarking
            "2001:db8::/32",    // RFC3849: Documentation
            "2001::/23",        // RFC2928: IETF Protocol Assignments
            "5f00::/16",        // RFC-ietf-6man-sids-06: Segment Routing (SRv6)
            "fe80::/10",        // RFC4291: Link-Local Unicast
            "fc00::/7",         // RFC4193, RFC8190: Unique Local
    };

    private static final List<BigInteger[]> privateIPv4AddressRanges;
    private static final List<BigInteger[]> privateIPv6AddressRanges;

    static {
        privateIPv4AddressRanges = new ArrayList<>();
        privateIPv6AddressRanges = new ArrayList<>();
        // convert the private prefixes into the first and last BigInteger ranges
        for (String privatePrefix : privatePrefixes) {
            Slice ipPrefixSlice = castFromVarcharToIpPrefix(utf8Slice(privatePrefix));
            Slice startingIpAddress = ipSubnetMin(ipPrefixSlice);
            Slice endingIpAddress = ipSubnetMax(ipPrefixSlice);

            BigInteger startingIpAsBigInt = toBigInteger(startingIpAddress);
            BigInteger endingIpAsBigInt = toBigInteger(endingIpAddress);

            BigInteger[] privateRange = new BigInteger[]{startingIpAsBigInt, endingIpAsBigInt};

            if (isIpv4(ipPrefixSlice)) {
                privateIPv4AddressRanges.add(privateRange);
            }
            else {
                privateIPv6AddressRanges.add(privateRange);
            }
        }
        privateIPv4AddressRanges.sort(Comparator.comparing(e -> e[0]));
        privateIPv6AddressRanges.sort(Comparator.comparing(e -> e[0]));
    }

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

    @Description("Combines the input set of IP prefixes into the fewest contiguous CIDR ranges possible.")
    @ScalarFunction("ip_prefix_collapse")
    @SqlType("array(IPPREFIX)")
    public static Block collapseIpPrefixes(@SqlType("array(IPPREFIX)") Block unsortedIpPrefixArray)
    {
        int inputPrefixCount = unsortedIpPrefixArray.getPositionCount();

        // If we get an empty array or an array non-null single element, just return the original array.
        if (inputPrefixCount == 0 || (inputPrefixCount == 1 && !unsortedIpPrefixArray.isNull(0))) {
            return unsortedIpPrefixArray;
        }

        // Sort prefixes. lessThanFunction is never used. NULLs are placed at the end.
        // Prefixes are ordered by first IP and then prefix length.
        // Example:
        //  Input:  10.0.0.0/8, 9.255.255.0/24, 10.0.0.0/7, 10.1.0.0/24, 10.10.0.0/16
        //  Output: 9.255.255.0/24, 10.0.0.0/7, 10.0.0.0/8, 10.1.0.0/24, 10.10.0.0/16
        Block ipPrefixArray = sort(null, IPPREFIX, unsortedIpPrefixArray);

        // throw if anything is null
        if (ipPrefixArray.isNull(0) || ipPrefixArray.isNull(inputPrefixCount - 1)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "ip_prefix_collapse does not support null elements");
        }

        // check the first and last prefixes in the array to make sure their IP versions match.
        Slice firstIpPrefix = IPPREFIX.getSlice(ipPrefixArray, 0);
        boolean v4 = isIpv4(firstIpPrefix);
        Slice lastIpPrefix = IPPREFIX.getSlice(ipPrefixArray, inputPrefixCount - 1);
        if (isIpv4(lastIpPrefix) != v4) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "All IPPREFIX elements must be the same IP version.");
        }

        List<List<Slice>> outputIpPrefixes = new ArrayList<>();
        int outputPrefixCount = 0;
        int ipMaxBitLength = v4 ? 32 : 128;

        List<BigInteger[]> mergedIpRanges = mergeIpRanges(ipPrefixArray);
        for (BigInteger[] ipRange : mergedIpRanges) {
            List<Slice> ipPrefixes = generateMinIpPrefixes(ipRange[0], ipRange[1], ipMaxBitLength);
            outputIpPrefixes.add(ipPrefixes);
            outputPrefixCount += ipPrefixes.size();
        }

        BlockBuilder blockBuilder = IPPREFIX.createBlockBuilder(null, outputPrefixCount);
        for (List<Slice> ipPrefixSlices : outputIpPrefixes) {
            for (Slice ipPrefix : ipPrefixSlices) {
                IPPREFIX.writeSlice(blockBuilder, ipPrefix);
            }
        }

        return blockBuilder.build();
    }

    @Description("Returns whether ipAddress is a private or reserved IP address that is not globally reachable.")
    @ScalarFunction("is_private_ip")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isPrivateIpAddress(@SqlType(StandardTypes.IPADDRESS) Slice ipAddress)
    {
        BigInteger ipAsBigInt = toBigInteger(ipAddress);
        boolean isIPv4 = isIpv4(ipAddress);

        List<BigInteger[]> rangesToCheck = isIPv4 ? privateIPv4AddressRanges : privateIPv6AddressRanges;

        // rangesToCheck is sorted
        for (BigInteger[] privateAddressRange : rangesToCheck) {
            BigInteger startIp = privateAddressRange[0];
            BigInteger endIp = privateAddressRange[1];

            if (ipAsBigInt.compareTo(startIp) < 0) {
                return false;  // current and subsequent ranges are all higher values, so we can fail fast here.
            }

            // ipAddress at least >= to startIp

            if (ipAsBigInt.compareTo(endIp) <= 0) {
                return true;  // if ipAsBigInt is in between startIp and endIp of private range then return true
            }
        }

        return false;
    }

    @Description("Split the input prefix into subnets the size of the new prefix length.")
    @ScalarFunction("ip_prefix_subnets")
    @SqlType("array(IPPREFIX)")
    public static Block ipPrefixSubnets(@SqlType(StandardTypes.IPPREFIX) Slice prefix, @SqlType(StandardTypes.BIGINT) long newPrefixLength)
    {
        boolean inputIsIpV4 = isIpv4(prefix);

        if (newPrefixLength < 0 || (inputIsIpV4 && newPrefixLength > 32) || (!inputIsIpV4 && newPrefixLength > 128)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid prefix length for IPv" + (inputIsIpV4 ? "4" : "6") + ": " + newPrefixLength);
        }

        int inputPrefixLength = getPrefixLength(prefix);
        // An IP prefix is a 'network', or group of contiguous IP addresses. The common format for describing IP prefixes is
        // uses 2 parts separated by a '/': (1) the IP address part and the (2) prefix length part (also called subnet size or CIDR).
        // For example, in 9.255.255.0/24, 9.255.255.0 is the IP address part and 24 is the prefix length.
        // The prefix length describes how many IP addresses the prefix contains in terms of the leading number of bits required. A higher number of bits
        // means smaller number of IP addresses. Subnets inherently mean smaller groups of IP addresses.
        // We can only disaggregate a prefix if the prefix length is the same length or longer (more-specific) than the length of the input prefix.
        // E.g., if the input prefix is 9.255.255.0/24, the prefix length can be /24, /25, /26, etc... but not 23 or larger value than 24.

        int newPrefixCount = 0;  // if inputPrefixLength > newPrefixLength, there are no new prefixes and we will return an empty array.
        if (inputPrefixLength <= newPrefixLength) {
            // Next, count how many new prefixes we will generate. In general, every difference in prefix length doubles the number new prefixes.
            // For example if we start with 9.255.255.0/24, and want to split into /25s, we would have 2 new prefixes. If we wanted to split into /26s,
            // we would have 4 new prefixes, and /27 would have 8 prefixes etc....
            newPrefixCount = 1 << (newPrefixLength - inputPrefixLength);  // 2^N
        }

        if (newPrefixCount == 0) {
            return EMPTY_BLOCK;
        }

        BlockBuilder blockBuilder = IPPREFIX.createBlockBuilder(null, newPrefixCount);

        if (newPrefixCount == 1) {
            IPPREFIX.writeSlice(blockBuilder, prefix); // just return the original prefix in an array
            return blockBuilder.build(); // returns empty or single entry
        }

        int ipVersionMaxBits = inputIsIpV4 ? 32 : 128;
        BigInteger newPrefixIpCount = TWO.pow(ipVersionMaxBits - (int) newPrefixLength);

        Slice startingIpAddressAsSlice = ipSubnetMin(prefix);
        BigInteger currentIpAddress = toBigInteger(startingIpAddressAsSlice);

        try {
            for (int i = 0; i < newPrefixCount; i++) {
                InetAddress asInetAddress = bigIntegerToIpAddress(currentIpAddress);
                Slice ipPrefixAsSlice = castFromVarcharToIpPrefix(utf8Slice(InetAddresses.toAddrString(asInetAddress) + "/" + newPrefixLength));
                IPPREFIX.writeSlice(blockBuilder, ipPrefixAsSlice);
                currentIpAddress = currentIpAddress.add(newPrefixIpCount);   // increment to start of next new prefix
            }
        }
        catch (UnknownHostException ex) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unable to convert " + currentIpAddress + " to IP prefix", ex);
        }

        return blockBuilder.build();
    }

    private static int getPrefixLength(Slice ipPrefix)
    {
        return ipPrefix.getByte(IPPREFIX.getFixedSize() - 1) & 0xFF;
    }

    private static List<Slice> generateMinIpPrefixes(BigInteger firstIpAddress, BigInteger lastIpAddress, int ipVersionMaxBits)
    {
        List<Slice> ipPrefixSlices = new ArrayList<>();

        // i.e., while firstIpAddress <= lastIpAddress
        while (firstIpAddress.compareTo(lastIpAddress) <= 0) {
            long rangeBits = findRangeBits(firstIpAddress, lastIpAddress); // find the number of bits for the next prefix in the range
            int prefixLength = (int) (ipVersionMaxBits - rangeBits);

            try {
                InetAddress asInetAddress = bigIntegerToIpAddress(firstIpAddress); // convert firstIpAddress from BigInt to Slice
                Slice ipPrefixAsSlice = castFromVarcharToIpPrefix(utf8Slice(InetAddresses.toAddrString(asInetAddress) + "/" + prefixLength));
                ipPrefixSlices.add(ipPrefixAsSlice);
            }
            catch (UnknownHostException ex) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unable to convert " + firstIpAddress + " to IP prefix", ex);
            }

            BigInteger ipCount = TWO.pow(ipVersionMaxBits - prefixLength);
            firstIpAddress = firstIpAddress.add(ipCount);  // move to the next prefix in the range
        }

        return ipPrefixSlices;
    }

    private static long findRangeBits(BigInteger firstIpAddress, BigInteger lastIpAddress)
    {
        // The number of IP addresses in the range
        BigInteger ipCount = lastIpAddress.subtract(firstIpAddress).add(BigInteger.ONE);

        // We have two possibilities for determining the right prefix boundary

        // Case 1. Find the largest possible prefix that firstIpAddress can be.
        //     Say we have an input range of 192.168.0.0 to 192.184.0.0.
        //     The number of IP addresses in the range is 1048576 = 2^20, so we would need a /12 (32-20).
        //     to cover that many IP addresses but the largest valid prefix that can start from 192.168.0.0 is /13.
        int firstAddressMaxBits = firstIpAddress.getLowestSetBit();

        // Case 2. Find the largest prefix length to cover N IP addresses.
        //     The number of IP addresses within a valid prefix must be a power of 2 but the IP count
        //     in our IP ranges may not be a power of 2. If it isn't exactly a power of 2, we find the
        //     highest power of 2 that the doesn't overrun the ipCount.

        // If ipCount's bitLength is greater than the number of IP addresses (i.e., not a power of 2), then use 1 bit less.
        int ipRangeMaxBits = (TWO.pow(ipCount.bitLength()).compareTo(ipCount) > 0) ? ipCount.bitLength() - 1 : ipCount.bitLength();

        return min(firstAddressMaxBits, ipRangeMaxBits);
    }

    private static List<BigInteger[]> mergeIpRanges(Block ipPrefixArray)
    {
        List<BigInteger[]> mergedRanges = new ArrayList<>();

        Slice startingIpPrefix = IPPREFIX.getSlice(ipPrefixArray, 0);
        BigInteger firstIpAddress = toBigInteger(ipSubnetMin(startingIpPrefix));
        BigInteger lastIpAddress = toBigInteger(ipSubnetMax(startingIpPrefix));

        /*
         There are four cases to cover for two IP ranges where range1.startIp <= range2.startIp

         1. Could be equal/duplicates.
             [-------]
             [-------]
             In this case, we just ignore the second one.

         2. Second could be subnet/contained within first.
             [-------]  OR  [-------]  OR  [-------]
               [---]        [----]            [----]
            In this case we ignore the second one.

         3. Second could be adjacent/contiguous with the first.
             [-------]
                      [-------]
            In this case we extend the range to include the last IP address of the second one.

         4. Second can be disjoint from the first.
             [-------]
                        [-------]
            In this case the first range is finalized, and the second range becomes the current one.
        */

        for (int i = 1; i < ipPrefixArray.getPositionCount(); i++) {
            Slice ipPrefix = IPPREFIX.getSlice(ipPrefixArray, i);
            BigInteger nextFirstIpAddress = toBigInteger(ipSubnetMin(ipPrefix));
            BigInteger nextLastIpAddress = toBigInteger(ipSubnetMax(ipPrefix));

            // If nextFirstIpAddress <= lastIpAddress then there is overlap.
            // However, based on the properties of the input sorted array, this will
            // always mean that the next* range is a subnet of [firstIpAddress, lastIpAddress].
            // We just ignore these prefixes since they are already covered (case 1 and case 2).
            if (lastIpAddress.compareTo(nextFirstIpAddress) < 0) {  // i.e. nextFirstIpAddress > lastIpAddress -- the next range does not overlap the first
                // If they are not contiguous (case 4), finalize the range.
                // Otherwise, extend the current range (case 3).
                if (lastIpAddress.add(BigInteger.ONE).compareTo(nextFirstIpAddress) != 0) {
                    BigInteger[] finalizedRange = {firstIpAddress, lastIpAddress};
                    mergedRanges.add(finalizedRange);
                    firstIpAddress = nextFirstIpAddress;
                }
                lastIpAddress = nextLastIpAddress;
            }
        }

        // Add the last range
        BigInteger[] finalizedRange = {firstIpAddress, lastIpAddress};
        mergedRanges.add(finalizedRange);

        return mergedRanges;
    }

    private static byte[] bigIntegerToIPAddressBytes(BigInteger ipAddress)
    {
        byte[] ipAddressBytes = ipAddress.toByteArray();

        // Covers IPv4 (4 bytes) and IPv6 (16 bytes) plus an additional 0-value byte for sign
        if ((ipAddressBytes.length == 5 || ipAddressBytes.length == 17) && ipAddressBytes[0] == 0) {
            ipAddressBytes = Arrays.copyOfRange(ipAddressBytes, 1, ipAddressBytes.length); // remove leading 0
        }
        // Covers IPv4 and IPv6 cases when BigInteger needs less than 4 or 16 bytes to represent
        // the integer value. E.g., 0.0.0.1 will be 1 byte and 15.1.99.212 will be 3 bytes
        else if (ipAddressBytes.length <= 3 || (ipAddressBytes.length != 4 && ipAddressBytes.length <= 15)) {
            // start with zero'd out byte array and fill in starting at position j
            byte[] emptyRange = new byte[ipAddressBytes.length <= 3 ? 4 : 16];
            int j = emptyRange.length - ipAddressBytes.length;
            for (int i = 0; i < ipAddressBytes.length; i++, j++) {
                emptyRange[j] = ipAddressBytes[i];
            }
            ipAddressBytes = emptyRange;
        }
        // else length is already 4 or 16
        return ipAddressBytes;
    }

    private static InetAddress bigIntegerToIpAddress(BigInteger ipAddress) throws UnknownHostException
    {
        byte[] ipAddressBytes = bigIntegerToIPAddressBytes(ipAddress);
        return InetAddress.getByAddress(ipAddressBytes);
    }

    private static BigInteger toBigInteger(Slice ipAddress)
    {
        // first param sets values to always be positive
        return new BigInteger(1, ipAddress.getBytes());
    }

    private static boolean isIpv4(Slice ipPrefix)
    {
        // IPADDRESS types are 16 bytes for IPv4 and IPv6. IPv4 is stored as IPv4-mapped IPv6 addresses specified in RFC 4291.
        // The IPv4 address is encoded into the low-order 32 bits of the IPv6 address, and the high-order 96 bits
        // hold the fixed prefix 0:0:0:0:0:FFFF.
        // To check if this is an IPv4 address, we check if the first 10 bytes are 0 and that bytes 11 and 12 are 0xFF.
        byte[] ipPartBytes = ipPrefix.getBytes(0, 2 * Long.BYTES);

        for (int i = 0; i <= 9; i++) {
            if (ipPartBytes[i] != (byte) 0) {
                return false;
            }
        }

        return ipPartBytes[10] == (byte) 0xff && ipPartBytes[11] == (byte) 0xff;
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
