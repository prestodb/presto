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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class CIDR4
        extends CIDR
{
    private final int addressEndInt;
    private int addressInt;

    protected CIDR4(Inet4Address newAddress, int mask)
    {
        cidrMask = mask;
        addressInt = ipv4AddressToInt(newAddress);
        int newMask = ipv4PrefixLengthToMask(mask);
        addressInt &= newMask;
        try {
            baseAddress = intToIPv4Address(addressInt);
        }
        catch (UnknownHostException e) {
            // this should never happen
        }
        addressEndInt = addressInt + ipv4PrefixLengthToLength(cidrMask) - 1;
    }

    private static int ipv4PrefixLengthToLength(int prefixLength)
    {
        return 1 << 32 - prefixLength;
    }

    private static int ipv4PrefixLengthToMask(int prefixLength)
    {
        return ~((1 << 32 - prefixLength) - 1);
    }

    private static InetAddress intToIPv4Address(int address)
            throws UnknownHostException
    {
        byte[] a = new byte[4];
        a[0] = (byte) (address >> 24 & 0xFF);
        a[1] = (byte) (address >> 16 & 0xFF);
        a[2] = (byte) (address >> 8 & 0xFF);
        a[3] = (byte) (address & 0xFF);
        return InetAddress.getByAddress(a);
    }

    private static int ipv4AddressToInt(InetAddress inetAddress)
    {
        byte[] address;
        if (inetAddress instanceof Inet6Address) {
            address = getIpV4FromIpV6((Inet6Address) inetAddress);
        }
        else {
            address = inetAddress.getAddress();
        }
        return ipv4AddressToInt(address);
    }

    private static int ipv4AddressToInt(byte[] address)
    {
        int net = 0;
        for (byte addres : address) {
            net <<= 8;
            net |= addres & 0xFF;
        }
        return net;
    }

    @Override
    public InetAddress getEndAddress()
    {
        try {
            return intToIPv4Address(addressEndInt);
        }
        catch (UnknownHostException e) {
            // this should never happen
            return null;
        }
    }

    @Override
    public int compareTo(CIDR arg)
    {
        if (arg instanceof CIDR6) {
            byte[] address = getIpV4FromIpV6((Inet6Address) arg.baseAddress);
            int net = ipv4AddressToInt(address);
            if (net == addressInt && arg.cidrMask == cidrMask) {
                return 0;
            }
            if (net < addressInt) {
                return 1;
            }
            if (net > addressInt) {
                return -1;
            }
            if (arg.cidrMask < cidrMask) {
                return -1;
            }
            return 1;
        }
        CIDR4 o = (CIDR4) arg;
        if (o.addressInt == addressInt && o.cidrMask == cidrMask) {
            return 0;
        }
        if (o.addressInt < addressInt) {
            return 1;
        }
        if (o.addressInt > addressInt) {
            return -1;
        }
        if (o.cidrMask < cidrMask) {
            // greater Mask means less IpAddresses so -1
            return -1;
        }
        return 1;
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

        int search = ipv4AddressToInt(inetAddress);
        return search >= addressInt && search <= addressEndInt;
    }
}
