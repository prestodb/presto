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
package com.facebook.presto.util;

import io.airlift.log.Logger;

import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class CIDR6
        extends CIDR
{
    private static final Logger log = Logger.get(CIDR6.class);
    private final BigInteger addressEndBigInt;
    private BigInteger addressBigInt;

    protected CIDR6(Inet6Address newAddress, int newMask)
    {
        cidrMask = newMask;
        addressBigInt = ipv6AddressToBigInteger(newAddress);
        BigInteger mask = ipv6CidrMaskToMask(newMask);
        try {
            addressBigInt = addressBigInt.and(mask);
            baseAddress = bigIntToIPv6Address(addressBigInt);
        }
        catch (UnknownHostException e) {
            // this should never happen.
        }
        addressEndBigInt = addressBigInt.add(ipv6CidrMaskToBaseAddress(cidrMask)).subtract(BigInteger.ONE);
    }

    private static BigInteger ipv6CidrMaskToBaseAddress(int cidrMask)
    {
        return BigInteger.ONE.shiftLeft(128 - cidrMask);
    }

    private static BigInteger ipv6CidrMaskToMask(int cidrMask)
    {
        return BigInteger.ONE.shiftLeft(128 - cidrMask).subtract(BigInteger.ONE).not();
    }

    private static BigInteger ipv6AddressToBigInteger(InetAddress address)
    {
        byte[] ipv6;
        if (address instanceof Inet4Address) {
            ipv6 = getIpV6FromIpV4((Inet4Address) address);
        }
        else {
            ipv6 = address.getAddress();
        }
        if (ipv6[0] == -1) {
            return new BigInteger(1, ipv6);
        }
        return new BigInteger(ipv6);
    }

    private static InetAddress bigIntToIPv6Address(BigInteger address)
            throws UnknownHostException
    {
        byte[] a = new byte[16];
        byte[] b = address.toByteArray();
        if (b.length > 16 && !(b.length == 17 && b[0] == 0)) {
            throw new UnknownHostException("invalid IPv6 address (too big)");
        }
        if (b.length == 16) {
            return InetAddress.getByAddress(b);
        }
        // handle the case where the IPv6 address starts with "FF".
        if (b.length == 17) {
            System.arraycopy(b, 1, a, 0, 16);
        }
        else {
            // copy the address into a 16 byte array, zero-filled.
            int p = 16 - b.length;
            System.arraycopy(b, 0, a, p, b.length);
        }
        return InetAddress.getByAddress(a);
    }

    @Override
    public InetAddress getEndAddress()
    {
        try {
            return bigIntToIPv6Address(addressEndBigInt);
        }
        catch (UnknownHostException e) {
            log.error("invalid ip address calculated as an end address");
            return null;
        }
    }

    @Override
    public int compareTo(CIDR arg)
    {
        if (arg instanceof CIDR4) {
            BigInteger net = ipv6AddressToBigInteger(arg.baseAddress);
            int res = net.compareTo(addressBigInt);
            if (res == 0) {
                if (arg.cidrMask == cidrMask) {
                    return 0;
                }
                if (arg.cidrMask < cidrMask) {
                    return -1;
                }
                return 1;
            }
            return res;
        }
        CIDR6 o = (CIDR6) arg;
        if (o.addressBigInt.equals(addressBigInt) && o.cidrMask == cidrMask) {
            return 0;
        }
        int res = o.addressBigInt.compareTo(addressBigInt);
        if (res == 0) {
            if (o.cidrMask < cidrMask) {
                // greater Mask means less IpAddresses so -1
                return -1;
            }
            return 1;
        }
        return res;
    }

    @Override
    public boolean contains(InetAddress inetAddress)
    {
        if (inetAddress == null) {
            throw new NullPointerException("inetAddress");
        }

        if (cidrMask == 0) {
            return true;
        }

        BigInteger search = ipv6AddressToBigInteger(inetAddress);
        return search.compareTo(addressBigInt) >= 0 && search.compareTo(addressEndBigInt) <= 0;
    }
}
